"""RSI request planning and strict broker-response binding, without live calls."""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
import builtins
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import evaluate_candidates
import rsi_orchestration


VALUES = {
    "VOLUME_LOOKBACK_DAYS": 20, "HIGH_LOOKBACK_DAYS": 5,
    "MIN_MEDIAN_DOLLAR_VOLUME": 175000, "DIP_ENTRY_PCT": 5,
    "MAX_SPREAD_BUY_PCT": 2, "RSI_PERIOD": 14, "RSI_INTERVAL": "30minute",
    "RSI_LOOKBACK_BARS": 5, "RSI_CONFIRM_BARS": 2,
    "RSI_OVERSOLD": 35, "RSI_MAX_ENTRY": 60, "TOP_N": 15,
}
HASH = "a" * 64
AS_OF = "2026-09-11T19:50:00Z"


def pre_document(scratch):
    params = {parameter: VALUES[constant] for parameter, constant in rsi_orchestration.PARAM_CONSTANTS.items()}
    params.update({parameter: None for parameter in rsi_orchestration.RSI_PARAM_CONSTANTS})
    params.update(bars=["historicals-0"], quotes=["candidate-quotes-0"],
                  expected_symbols=["ZZZ", "AAA"], rsi_file=None,
                  json_out=str(scratch / "pre-rsi-gates.json"))
    rows = [{"symbol": symbol, "buy_candidate": True, "insufficient_history": False,
             "skip_reason": None, "current_price": 4, "median_dollar_volume": 500000,
             "recent_high": 6, "pct_below_high": 100 / 3, "spread_pct": 0.5,
             "rsi_gate": "disabled"} for symbol in ["AAA", "ZZZ"]]
    return {"schema_version": 1, "rsi_gate_enabled": False, "params": params, "results": rows}


def raw_rsi(symbol="AAA"):
    return {"data": {"symbol": symbol, "interval": "30minute", "bounds": "regular",
                     "indicators": [{"type": "rsi", "params": {"period": 14},
                                     "series": [{"begins_at": f"2026-09-11T{16 + index // 2:02d}:{30 * (index % 2):02d}:00Z", "value": value}
                                                for index, value in enumerate([40, 36, 33, 30, 31, 34])]}]}}


class RSIWindowTests(unittest.TestCase):
    def test_current_config_has_enough_complete_prior_sessions(self):
        start, end = rsi_orchestration.request_window(AS_OF, "30minute", 14, 5)
        self.assertEqual(start, "2026-09-09T13:30:00Z")
        self.assertEqual(end, AS_OF)

    def test_window_skips_labor_day_and_weekend(self):
        self.assertEqual(rsi_orchestration.request_window("2026-09-08T14:00:00Z", "30minute", 14, 5)[0],
                         "2026-09-03T13:30:00Z")

    def test_window_counts_early_close_capacity_and_winter_offset(self):
        # Nov 27 has seven 30-minute bars, Nov 26 is closed. Twenty bars
        # from Fri + Wed are insufficient for the 21-bar warmup requirement.
        self.assertEqual(rsi_orchestration.request_window("2026-11-30T16:00:00Z", "30minute", 14, 5)[0],
                         "2026-11-24T14:30:00Z")

    def test_long_interval_and_outside_calendar_fail_without_truncation(self):
        for args in [(AS_OF, "50year", 14, 5), (AS_OF, "30minute", 10000, 5),
                     ("2026-01-02T15:00:00Z", "day", 14, 5), (AS_OF, "invented", 14, 5)]:
            with self.subTest(args=args), self.assertRaisesRegex(rsi_orchestration.PlanError, "rsi_request_range_unsupported"):
                rsi_orchestration.request_window(*args)

    def test_clock_is_canonical_and_date_valid(self):
        for value in [None, "2026-09-11", "2026-02-30T12:00:00Z", "2026-09-11T19:50:00+00:00"]:
            with self.subTest(value=value), self.assertRaises(rsi_orchestration.PlanError):
                rsi_orchestration.parse_utc(value)


class RSIPlanTests(unittest.TestCase):
    def invoke(self, scratch, document=None, **kwargs):
        if document is not None:
            (scratch / "pre-rsi-gates.json").write_text(json.dumps(document), encoding="utf-8")
        constants = SimpleNamespace(source_sha256=HASH, values=VALUES)
        with patch.object(rsi_orchestration, "validate_constants_file", return_value=constants), \
                patch.object(rsi_orchestration.broker_snapshot, "validate_scratch_directory", return_value=(scratch, {})), \
                patch.object(rsi_orchestration.broker_snapshot, "validate_bound_external_json_purposes", return_value=[]) as bound:
            result = rsi_orchestration.plan(str(scratch), kwargs.get("expected_hash", HASH), AS_OF)
        self.assertEqual(bound.call_args.args, (scratch, ["historicals-0", "candidate-quotes-0"]))
        self.assertEqual(bound.call_args_list[0].args, (scratch, []))
        return result

    def test_plan_uses_exact_request_schema_and_preserves_working_list_order(self):
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory).resolve()
            original = pre_document(scratch)
            result = self.invoke(scratch, original)
        self.assertEqual(result["params"], original["params"])
        self.assertEqual([item["symbol"] for item in result["requests"]], ["ZZZ", "AAA"])
        self.assertEqual([item["purpose"] for item in result["requests"]], ["rsi-0", "rsi-1"])
        self.assertEqual(result["requests"][0]["arguments"], {
            "symbol": "ZZZ", "type": "rsi", "interval": "30minute", "period": 14,
            "output": "last:6", "start_time": "2026-09-09T13:30:00Z", "end_time": AS_OF,
            "bounds": "regular", "adjustment_type": "split",
        })
        self.assertEqual(result["final_parameters"]["rsi_confirm_bars"], 2)
        self.assertEqual(result["final_parameters"]["rsi_start_time"], result["start_time"])

    def test_empty_pre_rsi_candidate_set_is_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory).resolve()
            document = pre_document(scratch)
            for row in document["results"]:
                row.update(buy_candidate=False, skip_reason="blocked by liquidity")
            self.assertEqual(self.invoke(scratch, document)["requests"], [])

    def test_hash_drift_stops_before_scratch_or_source_reads(self):
        constants = SimpleNamespace(source_sha256=HASH, values=VALUES)
        with patch.object(rsi_orchestration, "validate_constants_file", return_value=constants), \
                patch.object(rsi_orchestration.broker_snapshot, "validate_scratch_directory") as scratch:
            with self.assertRaisesRegex(rsi_orchestration.PlanError, "configuration_invalid"):
                rsi_orchestration.plan("unused", "b" * 64, AS_OF)
            scratch.assert_not_called()

    def test_transport_failure_precedes_invalid_prefile(self):
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory).resolve()
            (scratch / "pre-rsi-gates.json").write_text("malformed PRIVATE_FILE", encoding="utf-8")
            constants = SimpleNamespace(source_sha256=HASH, values=VALUES)
            for failure in [ValueError("source_handoff_pending"), ValueError("root identity changed")]:
                with self.subTest(failure=failure), \
                        patch.object(rsi_orchestration, "validate_constants_file", return_value=constants), \
                        patch.object(rsi_orchestration.broker_snapshot, "validate_scratch_directory", return_value=(scratch, {})), \
                        patch.object(rsi_orchestration.broker_snapshot, "validate_bound_external_json_purposes", side_effect=failure) as bound, \
                        patch.object(rsi_orchestration, "_read_pre_rsi") as read:
                    with self.assertRaisesRegex(rsi_orchestration.PlanError, "source_transport_invalid"):
                        rsi_orchestration.plan(str(scratch), HASH, AS_OF)
                    bound.assert_called_once_with(scratch, [])
                    read.assert_not_called()

    def test_malformed_or_misbound_pre_rsi_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory).resolve()
            mutations = [lambda doc: doc.update(rsi_gate_enabled=True),
                         lambda doc: doc["params"].update(rsi_period=2),
                         lambda doc: doc["params"].update(json_out="other.json"),
                         lambda doc: doc["params"].update(bars=[{}]),
                         lambda doc: doc["params"].update(quotes=["not-the-quote-input"]),
                         lambda doc: doc["results"].pop(),
                         lambda doc: doc["results"][0].update(insufficient_history=True),
                         lambda doc: doc["results"][0].update(current_price=True),
                         lambda doc: doc["results"][0].update(current_price=float("inf"))]
            for mutate in mutations:
                document = pre_document(scratch)
                mutate(document)
                with self.subTest(mutation=mutate), self.assertRaises(rsi_orchestration.PlanError):
                    self.invoke(scratch, document)

    def test_duplicate_json_bom_missing_and_symlink_inputs_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            scratch = Path(directory).resolve()
            target = scratch / "pre-rsi-gates.json"
            for raw in ['{"schema_version":1,"schema_version":1}', "\ufeff{}", "{", "NaN"]:
                target.write_text(raw, encoding="utf-8")
                with self.subTest(raw=raw), self.assertRaises(rsi_orchestration.PlanError):
                    self.invoke(scratch)
            target.unlink()
            with self.assertRaises(rsi_orchestration.PlanError):
                self.invoke(scratch)

    def test_cli_failure_never_exposes_exception_paths(self):
        output = io.StringIO()
        with patch.object(rsi_orchestration, "plan", side_effect=OSError("SECRET_FILE")), redirect_stdout(output):
            code = rsi_orchestration.main(["--plan", "--scratch", "PRIVATE_SCRATCH", "--as-of-utc", AS_OF,
                                            "--expected-constants-sha256", HASH])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output.getvalue()), {"schema_version": 1, "action": "plan-rsi", "ok": False,
                                                        "error": {"code": "rsi_plan_failed"}})
        self.assertNotIn("SECRET", output.getvalue())
        self.assertNotIn("PRIVATE", output.getvalue())


class RSIResponseBindingTests(unittest.TestCase):
    def load(self, response):
        return evaluate_candidates.load_rsi_map([("rsi-empty" if response == {} else "rsi-0", response)], 14,
                                                interval="30minute", start_time="2026-09-09T13:30:00Z", as_of_utc=AS_OF,
                                                request_symbols=None if response == {} else ["AAA"])

    def test_complete_mcp_response_and_empty_sentinel_are_accepted(self):
        result = self.load({"structuredContent": raw_rsi(), "content": [{"type": "text", "text": "complete result"}], "isError": False})
        self.assertEqual(result["AAA"], [40, 36, 33, 30, 31, 34])
        self.assertEqual(self.load({}), {})

    def test_wrong_period_interval_bounds_or_indicator_is_rejected(self):
        for mutate in [lambda doc: doc["data"].update(symbol="OTHER"),
                       lambda doc: doc["data"].update(interval="day"),
                       lambda doc: doc["data"].update(bounds="extended"),
                       lambda doc: doc["data"]["indicators"][0]["params"].update(period=2),
                       lambda doc: doc["data"]["indicators"][0].update(type="sma"),
                       lambda doc: doc["data"]["indicators"].append(deepcopy(doc["data"]["indicators"][0]))]:
            response = raw_rsi()
            mutate(response)
            with self.subTest(mutation=mutate), self.assertRaises(ValueError):
                self.load(response)

    def test_outside_window_future_duplicate_reversed_and_invalid_timestamps_fail(self):
        for timestamp in ["2000-01-01T00:00:00Z", "2026-09-11T20:00:00Z", "2026-09-11T16:00:00Z",
                          "2026-09-11T15:30:00Z", "2026-09-11Z", None]:
            response = raw_rsi()
            response["data"]["indicators"][0]["series"][1]["begins_at"] = timestamp
            with self.subTest(timestamp=timestamp), self.assertRaises(ValueError):
                self.load(response)

    def test_bound_mode_rejects_authored_maps_but_legacy_mode_remains(self):
        legacy = {"AAA": [40, 36, 33, 30, 31, 34]}
        with self.assertRaises(ValueError):
            self.load(legacy)
        self.assertEqual(evaluate_candidates.load_rsi_map([("legacy", legacy)], 14), legacy)


class EvaluatorJSONOutputTests(unittest.TestCase):
    @staticmethod
    def minimum_arguments():
        return ["--scratch", "PRIVATE_SCRATCH", "--bars-purpose", "historicals-0", "--quotes-purpose", "candidate-quotes-0",
                "--expected-symbols", "AAA", "--volume-lookback-days", "1", "--high-lookback-days", "1",
                "--min-median-dollar-volume", "0", "--dip-entry-pct", "5", "--emit-json"]

    def test_source_validation_failure_is_typed_without_traceback_or_raw_diagnostic(self):
        for fail_call in (1, 2):
            count = 0
            def resolve(_scratch, purposes):
                nonlocal count
                count += 1
                if count == fail_call:
                    raise ValueError("PRIVATE_FILE has changed")
                return [(purposes[0], {"data": {"results": []}}, b"unused")]
            output, errors = io.StringIO(), io.StringIO()
            with self.subTest(fail_call=fail_call), \
                    patch.object(evaluate_candidates, "validate_bound_external_json_purposes", side_effect=resolve), \
                    redirect_stdout(output), redirect_stderr(errors):
                code = evaluate_candidates.main(self.minimum_arguments())
            self.assertEqual(code, 2)
            self.assertEqual(json.loads(output.getvalue()), {"schema_version": 1, "action": "evaluate-candidates", "ok": False,
                                                            "error": {"code": "candidate_source_transport_failed"}})
            self.assertEqual(errors.getvalue(), "")
            self.assertNotIn("PRIVATE", output.getvalue())

    def test_committed_malformed_payload_is_typed_as_evaluation_failure(self):
        def resolve(_scratch, purposes):
            # Both committed sources validate before the payload parser rejects
            # their content. This is distinct from a missing/changed raw file.
            return [(purposes[0], {"data": {"results": "invalid"}}, b"unused")]
        output = io.StringIO()
        with patch.object(evaluate_candidates, "validate_bound_external_json_purposes", side_effect=resolve) as bound, redirect_stdout(output):
            code = evaluate_candidates.main(self.minimum_arguments())
        self.assertEqual(code, 2)
        self.assertEqual(bound.call_count, 2)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "candidate_evaluation_failed")

    def test_machine_usage_errors_are_typed_and_legacy_exceptions_remain(self):
        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = evaluate_candidates.main(["--emit-json", "--unrecognized", "PRIVATE_PATH"])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "candidate_usage_invalid")
        self.assertEqual(errors.getvalue(), "")
        self.assertNotIn("PRIVATE", output.getvalue())
        with patch.object(evaluate_candidates, "validate_bound_external_json_purposes", side_effect=ValueError("legacy diagnostic")):
            with self.assertRaisesRegex(ValueError, "legacy diagnostic"):
                evaluate_candidates.main(self.minimum_arguments()[:-1])

    def test_emit_json_emits_only_typed_failure_when_file_readback_differs(self):
        documents = {
            "historicals-0": {"data": {"results": [{"symbol": "AAA", "bars": [{
                "begins_at": "2026-09-01T00:00:00Z", "close_price": "5", "high_price": "6", "volume": "100000",
            }]}]}},
            "candidate-quotes-0": {"AAA": 4},
        }
        def resolve(_scratch, purposes):
            return [(purpose, documents[purpose], b"unused") for purpose in purposes]
        with tempfile.TemporaryDirectory() as directory:
            output_path = str(Path(directory) / "gates.json")
            def corrupt_readback(path, mode="r", *args, **kwargs):
                if str(path) == output_path and mode == "r":
                    return io.StringIO("{}")
                return builtins.open(path, mode, *args, **kwargs)
            output = io.StringIO()
            with patch.object(evaluate_candidates, "validate_bound_external_json_purposes", side_effect=resolve), \
                    patch.object(evaluate_candidates, "open", side_effect=corrupt_readback, create=True), redirect_stdout(output):
                code = evaluate_candidates.main(["--scratch", directory, "--bars-purpose", "historicals-0", "--quotes-purpose",
                                                  "candidate-quotes-0", "--expected-symbols", "AAA", "--volume-lookback-days", "1",
                                                  "--high-lookback-days", "1", "--min-median-dollar-volume", "0", "--dip-entry-pct", "5",
                                                  "--json-out", output_path, "--emit-json"])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output.getvalue())["error"]["code"], "candidate_output_failed")

    def test_stdout_is_identical_to_saved_document_with_no_table_or_warning(self):
        bars = [{"begins_at": f"2026-08-{index + 1:02d}T00:00:00Z", "open_price": "5", "close_price": "5",
                 "high_price": "6", "low_price": "4", "volume": "100000"} for index in range(20)]
        documents = {"historicals-0": {"data": {"results": [{"symbol": "AAA", "bars": bars}]}},
                     "candidate-quotes-0": {"data": {"results": [{"quote": {"symbol": "AAA", "last_trade_price": "4",
                                                                                          "bid_price": "3.99", "ask_price": "4.01"}}]}},
                     "rsi-0": raw_rsi()}
        def resolve(_scratch, purposes):
            return [(purpose, documents[purpose], b"unused") for purpose in purposes]
        with tempfile.TemporaryDirectory() as directory:
            output_path = Path(directory) / "gates.json"
            argv = ["--scratch", directory, "--bars-purpose", "historicals-0", "--quotes-purpose", "candidate-quotes-0",
                    "--expected-symbols", "AAA", "MISSING", "--volume-lookback-days", "20", "--high-lookback-days", "5",
                    "--min-median-dollar-volume", "175000", "--dip-entry-pct", "5", "--max-spread-buy-pct", "2",
                    "--rsi-purpose", "rsi-0", "--rsi-period", "14", "--rsi-oversold", "35", "--rsi-lookback-bars", "5",
                    "--rsi-confirm-bars", "2", "--rsi-max-entry", "60", "--rsi-interval", "30minute",
                    "--rsi-request-symbols", "AAA",
                    "--rsi-start-time", "2026-09-09T13:30:00Z", "--rsi-as-of-utc", AS_OF,
                    "--json-out", str(output_path), "--emit-json"]
            output, errors = io.StringIO(), io.StringIO()
            with patch.object(evaluate_candidates, "validate_bound_external_json_purposes", side_effect=resolve), \
                    redirect_stdout(output), redirect_stderr(errors):
                code = evaluate_candidates.main(argv)
            saved = json.loads(output_path.read_text(encoding="utf-8"))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), saved)
        self.assertEqual(errors.getvalue(), "")
        self.assertNotIn("emit_json", saved["params"])
        rows = {row["symbol"]: row for row in saved["results"]}
        self.assertTrue(rows["AAA"]["buy_candidate"])
        self.assertFalse(rows["MISSING"]["buy_candidate"])


if __name__ == "__main__":
    unittest.main()
