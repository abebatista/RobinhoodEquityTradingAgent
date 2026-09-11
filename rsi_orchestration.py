#!/usr/bin/env python3
"""Plan bound RSI reads and load the checked-in code-mode continuation.

This module never contacts a broker. The plan validates the pre-RSI evaluator
handoff, its committed inputs, and the pinned constants before constructing any
request. Calendar arithmetic and request-window construction stay in Python.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import re
import stat
import sys

import broker_snapshot
from daily_loss_orchestration import load_source as _load_driver_source
from market_calendar import CALENDAR_YEARS, REGULAR_OPEN_MINUTE, core_session_schedule
from market_clock import EASTERN_STD_OFFSET, zone_time
from validate_constants import ConstantsValidationError, validate_constants_file


SOURCE = Path(__file__).resolve().parent / "tools" / "rsi_orchestration.js"
MAX_PRE_RSI_BYTES = 1_000_000
MAX_WINDOW_DAYS = 730
MAX_REQUEST_BARS = 10_000
INTRADAY_SECONDS = {
    "15second": 15, "30second": 30, "minute": 60, "5minute": 300,
    "10minute": 600, "30minute": 1800, "hour": 3600, "4hour": 14400,
}
CALENDAR_INTERVAL_DAYS = {
    "week": 7, "month": 31, "3month": 93, "6month": 186, "year": 366,
    "5year": 1830, "10year": 3660, "20year": 7320, "50year": 18300,
}
PARAM_CONSTANTS = {
    "volume_lookback_days": "VOLUME_LOOKBACK_DAYS",
    "high_lookback_days": "HIGH_LOOKBACK_DAYS",
    "min_median_dollar_volume": "MIN_MEDIAN_DOLLAR_VOLUME",
    "dip_entry_pct": "DIP_ENTRY_PCT",
    "max_spread_buy_pct": "MAX_SPREAD_BUY_PCT",
    "rsi_period": "RSI_PERIOD",
}
RSI_PARAM_CONSTANTS = {
    "rsi_oversold": "RSI_OVERSOLD", "rsi_lookback_bars": "RSI_LOOKBACK_BARS",
    "rsi_confirm_bars": "RSI_CONFIRM_BARS", "rsi_max_entry": "RSI_MAX_ENTRY",
}
PRE_PARAM_KEYS = {
    "bars", "quotes", "expected_symbols", "json_out", "rsi_file",
    *PARAM_CONSTANTS, *RSI_PARAM_CONSTANTS,
}


class PlanError(ValueError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def load_source(path: Path = SOURCE) -> dict:
    receipt = _load_driver_source(path)
    return {**receipt, "action": "load-rsi-orchestration"}


def parse_utc(value: str) -> datetime:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value):
        raise PlanError("rsi_clock_invalid")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise PlanError("rsi_clock_invalid") from exc


def _session(day: date) -> tuple[str, int | None]:
    if day.year not in CALENDAR_YEARS:
        raise PlanError("rsi_request_range_unsupported")
    if day.weekday() >= 5:
        return "weekend", None
    return core_session_schedule(day)


def _session_open_utc(day: date) -> datetime:
    # Noon UTC lies safely after the US DST transition hour. Reuse the same
    # deterministic offset implementation as market_clock instead of OS tzdata.
    _, _, offset = zone_time(datetime(day.year, day.month, day.day, 12, tzinfo=timezone.utc),
                             EASTERN_STD_OFFSET, "EST", "EDT")
    return datetime(day.year, day.month, day.day, 9, 30,
                    tzinfo=timezone(timedelta(hours=offset))).astimezone(timezone.utc)


def request_window(as_of_utc: str, interval: str, period: int, lookback: int) -> tuple[str, str]:
    cutoff = parse_utc(as_of_utc)
    if (type(period) is not int or type(lookback) is not int or period < 2 or lookback < 2
            or period + lookback + 2 > MAX_REQUEST_BARS):
        raise PlanError("rsi_request_range_unsupported")
    local, _, _ = zone_time(cutoff, EASTERN_STD_OFFSET, "EST", "EDT")
    day = local.date()
    _session(day)
    required = period + lookback + 2
    if interval in INTRADAY_SECONDS or interval == "day":
        # Count complete PRIOR sessions, so today's partial session cannot
        # falsely satisfy RSI warmup. Two extra bars cover the seed and edge.
        remaining = required
        for _ in range(MAX_WINDOW_DAYS):
            day -= timedelta(days=1)
            _, close = _session(day)
            if close is None:
                continue
            available = 1 if interval == "day" else max(1, ((close - REGULAR_OPEN_MINUTE) * 60) // INTRADAY_SECONDS[interval])
            remaining -= available
            if remaining <= 0:
                start = _session_open_utc(day)
                return start.strftime("%Y-%m-%dT%H:%M:%SZ"), as_of_utc
        raise PlanError("rsi_request_range_unsupported")
    days_per_bar = CALENDAR_INTERVAL_DAYS.get(interval)
    if days_per_bar is None or required * days_per_bar > MAX_WINDOW_DAYS:
        raise PlanError("rsi_request_range_unsupported")
    # Conservative calendar span for longer intervals; verify every traversed
    # date lies inside the reviewed calendar before allowing a request.
    for _ in range(required * days_per_bar):
        day -= timedelta(days=1)
        _session(day)
    for _ in range(8):
        _, close = _session(day)
        if close is not None:
            return _session_open_utc(day).strftime("%Y-%m-%dT%H:%M:%SZ"), as_of_utc
        day -= timedelta(days=1)
    raise PlanError("rsi_request_range_unsupported")


def _reject_constant(_value):
    raise PlanError("pre_rsi_validation_failed")


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise PlanError("pre_rsi_validation_failed")
        value[key] = item
    return value


def _read_pre_rsi(scratch: Path) -> dict:
    target = scratch / "pre-rsi-gates.json"
    try:
        if target.is_symlink():
            raise PlanError("pre_rsi_input_failed")
        with target.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size > MAX_PRE_RSI_BYTES:
                raise PlanError("pre_rsi_input_failed")
            raw = handle.read(MAX_PRE_RSI_BYTES + 1)
            after = os.fstat(handle.fileno())
        current = target.stat(follow_symlinks=False)
        identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)
        if (not raw or len(raw) > MAX_PRE_RSI_BYTES or identity(before) != identity(after)
                or identity(after) != identity(current) or not stat.S_ISREG(current.st_mode)):
            raise PlanError("pre_rsi_input_failed")
        text = raw.decode("utf-8", errors="strict")
        if text.startswith("\ufeff"):
            raise PlanError("pre_rsi_validation_failed")
        return json.loads(text, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (OSError, UnicodeError) as exc:
        raise PlanError("pre_rsi_input_failed") from exc
    except json.JSONDecodeError as exc:
        raise PlanError("pre_rsi_validation_failed") from exc


def _symbols(value) -> list[str]:
    if (not isinstance(value, list) or not value or len(value) > 1000
            or any(not isinstance(symbol, str) or re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,14}", symbol) is None for symbol in value)
            or len(set(value)) != len(value)):
        raise PlanError("pre_rsi_validation_failed")
    return value


def _number(value) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def validate_pre_rsi(document: dict, values, scratch: Path) -> dict:
    if (not isinstance(document, dict) or set(document) != {"schema_version", "rsi_gate_enabled", "params", "results"}
            or type(document.get("schema_version")) is not int or document["schema_version"] != 1
            or document.get("rsi_gate_enabled") is not False):
        raise PlanError("pre_rsi_validation_failed")
    params = document.get("params")
    if not isinstance(params, dict) or set(params) != PRE_PARAM_KEYS:
        raise PlanError("pre_rsi_validation_failed")
    expected_symbols = _symbols(params.get("expected_symbols"))
    if len(expected_symbols) > values["TOP_N"]:
        raise PlanError("pre_rsi_validation_failed")
    if params.get("json_out") != str(scratch / "pre-rsi-gates.json") or params.get("rsi_file") is not None:
        raise PlanError("pre_rsi_validation_failed")
    for parameter, constant in PARAM_CONSTANTS.items():
        value = params.get(parameter)
        if not _number(value) or Decimal(str(value)) != Decimal(str(values[constant])):
            raise PlanError("pre_rsi_validation_failed")
    if any(params.get(parameter) is not None for parameter in RSI_PARAM_CONSTANTS):
        raise PlanError("pre_rsi_validation_failed")
    for kind, prefix in (("bars", "historicals"), ("quotes", "candidate-quotes")):
        purposes = params.get(kind)
        if (not isinstance(purposes, list) or not purposes or len(purposes) > 1000
                or any(not isinstance(purpose, str) or re.fullmatch(prefix + r"-(?:0|[1-9]\d{0,2})(?:-retry)?", purpose) is None for purpose in purposes)
                or len(set(purposes)) != len(purposes)):
            raise PlanError("pre_rsi_validation_failed")
    rows = document.get("results")
    if not isinstance(rows, list) or len(rows) != len(expected_symbols):
        raise PlanError("pre_rsi_validation_failed")
    seen = set()
    for row in rows:
        if not isinstance(row, dict) or row.get("symbol") not in expected_symbols or row["symbol"] in seen:
            raise PlanError("pre_rsi_validation_failed")
        seen.add(row["symbol"])
        if (type(row.get("buy_candidate")) is not bool or type(row.get("insufficient_history")) is not bool
                or "skip_reason" not in row
                or (row.get("skip_reason") is not None and (not isinstance(row["skip_reason"], str) or not row["skip_reason"]))):
            raise PlanError("pre_rsi_validation_failed")
        for field in ("current_price", "median_dollar_volume", "recent_high", "pct_below_high", "spread_pct"):
            if field not in row or (row[field] is not None and not _number(row[field])):
                raise PlanError("pre_rsi_validation_failed")
        if row["buy_candidate"] and (row["insufficient_history"] or row.get("skip_reason") is not None
                                     or row.get("rsi_gate") != "disabled"):
            raise PlanError("pre_rsi_validation_failed")
    return document


def plan(scratch_path: str, expected_constants_sha256: str, as_of_utc: str, *, constants_path=None) -> dict:
    try:
        constants = validate_constants_file(constants_path)
    except ConstantsValidationError as exc:
        raise PlanError("configuration_invalid") from exc
    if constants.source_sha256 != expected_constants_sha256:
        raise PlanError("configuration_invalid")
    try:
        scratch, _ = broker_snapshot.validate_scratch_directory(scratch_path)
        if str(scratch) != str(Path(scratch_path)):
            raise PlanError("source_transport_invalid")
        # Prove the complete bound root and closed journal before any pre-RSI
        # file/schema error can be classified as an entry-only failure.
        broker_snapshot.validate_bound_external_json_purposes(scratch, [])
    except (OSError, ValueError) as exc:
        raise PlanError("source_transport_invalid") from exc
    document = validate_pre_rsi(_read_pre_rsi(scratch), constants.values, scratch)
    params = document["params"]
    try:
        # This public consumer proves both transport identities and that no
        # earlier journal purpose remains pending before any RSI reservation.
        broker_snapshot.validate_bound_external_json_purposes(scratch, params["bars"] + params["quotes"])
    except (OSError, ValueError) as exc:
        raise PlanError("source_transport_invalid") from exc
    values = constants.values
    start_time, end_time = request_window(as_of_utc, values["RSI_INTERVAL"], values["RSI_PERIOD"], values["RSI_LOOKBACK_BARS"])
    candidates = {row["symbol"] for row in document["results"] if row["buy_candidate"]}
    # Preserve original working-list priority, not the evaluator's display sort.
    symbols = [symbol for symbol in params["expected_symbols"] if symbol in candidates]
    requests = [{"symbol": symbol, "purpose": f"rsi-{index}", "arguments": {
        "symbol": symbol, "type": "rsi", "interval": values["RSI_INTERVAL"], "period": values["RSI_PERIOD"],
        "output": f"last:{values['RSI_LOOKBACK_BARS'] + 1}", "start_time": start_time,
        "end_time": end_time, "bounds": "regular", "adjustment_type": "split",
    }} for index, symbol in enumerate(symbols)]
    final_parameters = {parameter: (float(values[constant]) if isinstance(values[constant], Decimal) else values[constant])
                        for parameter, constant in {**PARAM_CONSTANTS, **RSI_PARAM_CONSTANTS}.items()}
    final_parameters.update(rsi_interval=values["RSI_INTERVAL"], rsi_start_time=start_time, rsi_as_of_utc=as_of_utc)
    return {
        "schema_version": 1, "action": "plan-rsi", "ok": True,
        "constants_sha256": constants.source_sha256, "as_of_utc": as_of_utc,
        "start_time": start_time, "end_time": end_time, "params": params,
        "expected_symbols": params["expected_symbols"], "requests": requests,
        "final_parameters": final_parameters,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--emit-javascript", action="store_true")
    actions.add_argument("--plan", action="store_true")
    parser.add_argument("--scratch")
    parser.add_argument("--expected-constants-sha256")
    parser.add_argument("--as-of-utc")
    args = parser.parse_args(argv)
    if args.plan and not all((args.scratch, args.expected_constants_sha256, args.as_of_utc)):
        parser.error("--plan requires --scratch, --expected-constants-sha256, and --as-of-utc")
    action = "load-rsi-orchestration" if args.emit_javascript else "plan-rsi"
    try:
        receipt = load_source() if args.emit_javascript else plan(args.scratch, args.expected_constants_sha256, args.as_of_utc)
    except (OSError, ValueError, UnicodeError) as exc:
        code = exc.code if isinstance(exc, PlanError) else ("driver_source_unavailable" if args.emit_javascript else "rsi_plan_failed")
        print(json.dumps({"schema_version": 1, "action": action, "ok": False, "error": {"code": code}}, separators=(",", ":")))
        return 2
    print(json.dumps(receipt, ensure_ascii=True, allow_nan=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
