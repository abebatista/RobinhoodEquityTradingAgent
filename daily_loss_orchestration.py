#!/usr/bin/env python3
"""Load the checked-in Codex DAILY-LOSS driver without exposing runtime state.

The driver executes inside the caller's code-mode tool operation so broker
responses, the private lease, and store/load bindings never cross a model
boundary. This loader only reads source code; it does not start a trading run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys


SOURCE = Path(__file__).resolve().parent / "tools" / "daily_loss_orchestration.js"
MAX_SOURCE_BYTES = 262_144


def load_source(path: Path = SOURCE) -> dict:
    """Return one bounded UTF-8 source receipt from the fixed local module."""
    if path.is_symlink() or not path.is_file():
        raise ValueError("driver source is not a regular file")
    with path.open("rb") as handle:
        raw = handle.read(MAX_SOURCE_BYTES + 1)
    if not raw or len(raw) > MAX_SOURCE_BYTES:
        raise ValueError("driver source is empty or oversized")
    source = raw.decode("utf-8", errors="strict")
    if source.startswith("\ufeff") or "\x00" in source or not source.strip():
        raise ValueError("driver source is not undecorated UTF-8 text")
    return {
        "schema_version": 1,
        "action": "load-daily-loss-orchestration",
        "ok": True,
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "source": source,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emit-javascript", action="store_true", required=True)
    parser.parse_args(argv)
    try:
        receipt = load_source()
    except (OSError, ValueError, UnicodeError):
        # No filesystem path or raw exception leaks into the runtime transcript.
        print(json.dumps({
            "schema_version": 1,
            "action": "load-daily-loss-orchestration",
            "ok": False,
            "error": {"code": "driver_source_unavailable"},
        }, separators=(",", ":")))
        return 2
    print(json.dumps(receipt, ensure_ascii=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
