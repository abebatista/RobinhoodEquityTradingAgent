"""Exercise the checked-in code-mode continuation without any real broker calls."""

import argparse
from contextlib import closing, redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import broker_snapshot
import daily_loss
import daily_loss_orchestration
import market_clock
import run_lifecycle
import validate_constants


NODE = shutil.which("node") or shutil.which("node.exe")
DRIVER = os.path.join(ROOT, "tools", "daily_loss_orchestration.js")
HARNESS = os.path.join(ROOT, "tests", "fixtures", "daily_loss_orchestration_harness.js")
BOOTSTRAP = "rhmra.bootstrap-state.v1"
TRANSPORT = "rhmra.transport-state.v1"
LEASE = "rhmra.lease-state.v1"
EXECUTION = "rhmra.daily-loss-execution.v1"
RSI_EXECUTION = "rhmra.rsi-execution.v1"


class DailyLossDriverLoaderTests(unittest.TestCase):
    def test_loader_returns_exact_source_and_hash(self):
        result = daily_loss_orchestration.load_source()
        raw = Path(DRIVER).read_bytes()
        self.assertEqual(set(result), {"schema_version", "action", "ok", "source_sha256", "source"})
        self.assertTrue(result["ok"])
        self.assertEqual(result["source"].encode("utf-8"), raw)
        self.assertEqual(result["source_sha256"], hashlib.sha256(raw).hexdigest())

    def test_loader_rejects_empty_bom_invalid_utf8_nul_and_oversized_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "driver.js"
            for raw in (b"", b" \r\n", b"\xef\xbb\xbfasync function x() {}", b"\xff", b"hello\x00world",
                        b"x" * (daily_loss_orchestration.MAX_SOURCE_BYTES + 1)):
                with self.subTest(raw=raw[:30], size=len(raw)):
                    source.write_bytes(raw)
                    with self.assertRaises((ValueError, UnicodeError)):
                        daily_loss_orchestration.load_source(source)
            with self.assertRaises(ValueError):
                daily_loss_orchestration.load_source(Path(temporary))

    def test_loader_failure_is_compact_and_has_no_path_or_exception_text(self):
        output = io.StringIO()
        with patch.object(daily_loss_orchestration, "load_source", side_effect=OSError("SECRET_PATH")), redirect_stdout(output):
            code = daily_loss_orchestration.main(["--emit-javascript"])
        self.assertEqual(code, 2)
        self.assertNotIn("SECRET_PATH", output.getvalue())
        self.assertEqual(json.loads(output.getvalue()), {
            "schema_version": 1, "action": "load-daily-loss-orchestration", "ok": False,
            "error": {"code": "driver_source_unavailable"},
        })

    def test_exact_second_cell_compiles_then_enters_then_runs_driver(self):
        routine = Path(ROOT, "robinhood-momentum-routine-autonomous.md").read_text(encoding="utf-8")
        section = routine.split("**CODEX ENTER SECOND — EXACT OWNER-FENCED CELL:**", 1)[1]
        cell = section.split("```javascript", 1)[1].split("```", 1)[0]
        loader = cell.index("daily_loss_orchestration.py --emit-javascript")
        enter = cell.index("run_lifecycle.py enter-second")
        binding = cell.index("second_entry_receipt: receipt")
        invocation = cell.index("await runDailyLoss(")
        self.assertLess(loader, enter)
        self.assertLess(enter, binding)
        self.assertLess(binding, invocation)
        self.assertNotIn("text(", cell[binding:invocation])


@unittest.skipUnless(NODE, "Node.js is required to exercise the code-mode JavaScript driver")
class DailyLossOrchestrationTests(unittest.TestCase):
    def run_driver(self, config=None, **extra):
        completed = subprocess.run(
            [NODE, HARNESS], input=json.dumps({"module": DRIVER, "config": config or {}, **extra}),
            text=True, capture_output=True, cwd=ROOT, timeout=90,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        try:
            result = json.loads(completed.stdout)
        except ValueError:
            self.fail(completed.stdout + completed.stderr)
        self.assertIsNone(result["thrown"], result["thrown"])
        return result

    @staticmethod
    def commands(result, script=None, action=None):
        return [event for event in result["log"] if event["type"] == "command"
                and (script is None or event["script"] == script)
                and (action is None or event["action"] == action)]

    @staticmethod
    def brokers(result, name=None):
        return [event for event in result["log"] if event["type"] == "broker"
                and (name is None or event["name"] == name)]

    def assert_no_b(self, result):
        self.assertFalse(self.commands(result, "broker_snapshot.py", "authorize-generation-b"))
        self.assertFalse(any("daily-loss-b-" in event["purpose"] for event in self.brokers(result)))

    def test_flat_zero_symbols_completes_discovery_final_snapshot_and_calculation(self):
        result = self.run_driver()
        self.assertTrue(result["result"]["ok"], result)
        self.assertEqual([(event["name"], event["purpose"]) for event in self.brokers(result)], [
            ("get_equity_positions", "daily-loss-a-discovery-positions-0"),
            ("get_equity_orders", "daily-loss-a-discovery-orders-0"),
            ("get_portfolio", "daily-loss-a-final-portfolio-0"),
            ("get_equity_positions", "daily-loss-a-final-positions-0"),
            ("get_equity_orders", "daily-loss-a-final-orders-0"),
        ])
        helpers = self.commands(result, "daily_loss.py")
        self.assertEqual(len(helpers), 2)
        self.assertIn("symbols-out", helpers[0]["options"])
        self.assertIn("json-out", helpers[1]["options"])
        self.assertNotIn("quotes", helpers[1]["options"])
        self.assertEqual(len(self.commands(result, "market_clock.py")), 2)
        self.assertFalse(self.commands(result, "run_lifecycle.py", "complete-second"))
        self.assertEqual(len(result["writes"]), 5)
        for _path, raw in result["writes"]:
            self.assertEqual(json.loads(raw)["guide"]["must_preserve"][1], "full envelope")
        wrapper = result["state"][TRANSPORT]["daily_loss_receipt"]
        self.assertEqual(wrapper, helpers[1]["receipt"])
        self.assertEqual(wrapper["result"]["helper_owned_evidence"], {"retain": "unchanged"})
        self.assertNotIn("test-private-token", json.dumps(result["result"]))

    def test_each_page_is_reserved_written_committed_and_validated_before_staging(self):
        result = self.run_driver()
        events = [event for event in result["log"] if event["type"] != "store"]
        for index, event in enumerate(events):
            if event["type"] != "broker":
                continue
            self.assertEqual(events[index - 1]["action"], "reserve-source")
            self.assertEqual(events[index + 1]["type"], "patch")
            self.assertEqual(events[index + 2]["action"], "commit-source")
            self.assertNotIn("reservation-id", events[index + 2]["options"])
            offset = 3
            if event["name"] in ("get_equity_positions", "get_equity_orders"):
                self.assertEqual(events[index + offset]["action"], "page")
                offset += 1
            self.assertEqual(events[index + offset]["action"], "stage")
        for event in self.brokers(result, "get_equity_orders"):
            self.assertEqual(set(event["args"]), {"account_number"})

    def test_flat_positions_still_fetch_nonempty_required_quote_symbols(self):
        result = self.run_driver({"symbols": ["AAA", "BBB"]})
        self.assertTrue(result["result"]["ok"], result)
        quotes = self.brokers(result, "get_equity_quotes")
        self.assertEqual([event["args"] for event in quotes], [{"symbols": ["AAA", "BBB"]}])
        calculation = self.commands(result, "daily_loss.py")[-1]
        self.assertEqual(len(calculation["options"]["quotes"]), 1)

    def test_multipage_cursors_aggregate_all_pages_and_quote_batches_at_most_twenty(self):
        symbols = ["T" + str(number).zfill(2) for number in range(45)]
        result = self.run_driver({"symbols": symbols, "pages": {
            "discovery:positions": 2, "discovery:orders": 3,
            "final:positions": 2, "final:orders": 3,
        }})
        self.assertTrue(result["result"]["ok"], result)
        batches = [event["args"]["symbols"] for event in self.brokers(result, "get_equity_quotes")]
        self.assertEqual(list(map(len, batches)), [20, 20, 5])
        self.assertEqual(sum(batches, []), symbols)
        page_commands = self.commands(result, "connector_contract.py", "page")
        for event in page_commands:
            chain = event["options"]["request-cursor"]
            self.assertEqual(chain[0], "FIRST")
            self.assertEqual(len(chain), len(set(chain)))
        stages = self.commands(result, "broker_snapshot.py", "stage")
        aggregates = [event for event in stages if "source" in event["options"]]
        self.assertGreaterEqual(len(aggregates), 4)
        for event in aggregates:
            if event["options"]["kind"][0] in ("positions", "orders"):
                self.assertEqual(len(event["options"]["source"]), len(event["options"]["request-cursor"]))
                self.assertNotIn("allow-more", event["options"])
        calculation = self.commands(result, "daily_loss.py")[-1]
        self.assertEqual(len(calculation["options"]["positions"]), 2)
        self.assertEqual(len(calculation["options"]["orders"]), 3)
        self.assertEqual(len(calculation["options"]["quotes"]), 3)
        # argparse's nargs='+' stores only the last repeated option. Exercise the
        # real parser so the mock's convenient option list cannot hide lost pages.
        for command in self.commands(result, "daily_loss.py"):
            parsed = daily_loss._parser().parse_args(command["args"])
            self.assertEqual(parsed.positions, command["options"]["positions"])
            self.assertEqual(parsed.orders, command["options"]["orders"])
            self.assertEqual(parsed.quotes or [], command["options"].get("quotes", []))
            self.assertEqual(command["args"].count("--positions"), 1)
            self.assertEqual(command["args"].count("--orders"), 1)
            self.assertLessEqual(command["args"].count("--quotes"), 1)

    def test_missing_or_ambiguous_broker_tool_stops_before_any_broker_call(self):
        for config in ({"missingTool": "get_equity_orders"}, {"missingCallable": "get_portfolio"}, {"duplicateTool": True}):
            with self.subTest(config=config):
                result = self.run_driver(config)
                self.assertFalse(result["result"]["ok"])
                self.assertFalse(self.brokers(result))
                self.assertFalse(self.commands(result, "broker_snapshot.py", "reserve-source"))

    def test_explicit_connector_failure_retries_identical_arguments_once_under_same_reservation(self):
        result = self.run_driver({"readFailures": {"A:discovery:positions": 1}})
        self.assertTrue(result["result"]["ok"], result)
        reads = self.brokers(result, "get_equity_positions")
        self.assertEqual(reads[0]["args"], reads[1]["args"])
        aborts = self.commands(result, "broker_snapshot.py", "abort-source")
        self.assertEqual(len(aborts), 0)
        self.assertEqual(reads[0]["purpose"], reads[1]["purpose"])
        self.assert_no_b(result)

    def test_two_connector_errors_stop_after_exactly_two_reads_without_b(self):
        result = self.run_driver({"readFailures": {"A:discovery:positions": 2}, "readThrows": True})
        self.assertFalse(result["result"]["ok"])
        self.assertEqual(len(self.brokers(result)), 2)
        aborts = self.commands(result, "broker_snapshot.py", "abort-source")
        self.assertEqual(len(aborts), 1)
        self.assertEqual(aborts[0]["options"]["reservation-id"], ["reservation-1"])
        self.assertEqual(aborts[0]["options"]["reason"], ["connector-failed"])
        self.assert_no_b(result)

    def test_committed_receipt_uncertainty_cannot_retry_read_or_consume_b(self):
        result = self.run_driver({"failures": [{"when": "commit", "at": 1, "kind": "positions", "malformed": True}]})
        self.assertFalse(result["result"]["ok"])
        self.assertEqual(len(self.brokers(result)), 1)
        self.assertEqual(result["result"]["classification"], "coordination-halt")
        self.assertFalse(self.commands(result, "broker_snapshot.py", "abort-source"))
        self.assert_no_b(result)

    def test_file_save_failure_or_uncertainty_never_repeats_broker_call(self):
        for mode in ("throw", "error", "write-throw"):
            with self.subTest(mode=mode):
                result = self.run_driver({"patchFailure": mode})
                self.assertFalse(result["result"]["ok"])
                self.assertEqual(len(self.brokers(result)), 1)
                self.assertEqual(result["result"]["classification"], "snapshot-failure")
                self.assert_no_b(result)

    def test_typed_a_semantic_failure_runs_one_whole_new_b_generation(self):
        for point, code in (("stage", "stage_semantic_invalid"), ("page", "invalid_contract"), ("discovery", "daily_loss_semantic_invalid")):
            with self.subTest(point=point):
                result = self.run_driver({"failures": [{"when": point, "generation": "A", "at": 1, "code": code}]})
                self.assertTrue(result["result"]["ok"], result)
                self.assertEqual(result["result"]["generation"], "B")
                self.assertEqual(len(self.commands(result, "broker_snapshot.py", "authorize-generation-b")), 1)
                self.assertEqual(self.commands(result, "broker_snapshot.py", "finish-generation-b")[-1]["options"]["outcome"], ["completed"])
                b = [event for event in self.brokers(result) if "daily-loss-b-" in event["purpose"]]
                self.assertEqual(len(b), 5)
                self.assertEqual(b[0]["purpose"], "daily-loss-b-discovery-positions-0")

    def test_b_semantic_failure_records_exhaustion_and_snapshot_terminal(self):
        result = self.run_driver({"failures": [
            {"when": "discovery", "generation": "A", "code": "daily_loss_semantic_invalid"},
            {"when": "discovery", "generation": "B", "code": "daily_loss_semantic_invalid"},
        ]})
        self.assertFalse(result["result"]["ok"])
        self.assertEqual(result["result"]["reason_code"], "snapshot-second-attempt-failed")
        finished = self.commands(result, "broker_snapshot.py", "finish-generation-b")
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["options"]["outcome"], ["failed"])
        self.assertEqual(self.commands(result, "run_lifecycle.py", "complete-second")[-1]["options"]["outcome"], ["snapshot-terminal"])

    def test_transport_unknown_and_malformed_semantic_failures_do_not_authorize_b(self):
        cases = [
            ({"when": "stage", "code": "stage_write_failed"}, "snapshot-failure"),
            ({"when": "stage", "code": "new_unknown_failure"}, "coordination-halt"),
            ({"when": "stage", "code": "stage_semantic_invalid", "extra": True}, "coordination-halt"),
            ({"when": "stage", "code": "stage_semantic_invalid", "wrongGeneration": "B"}, "coordination-halt"),
            ({"when": "page", "code": "request_binding_invalid"}, "coordination-halt"),
            ({"when": "page", "code": "pagination_stopped"}, "snapshot-failure"),
            ({"when": "discovery", "code": "daily_loss_input_failed"}, "snapshot-failure"),
        ]
        for failure, classification in cases:
            with self.subTest(failure=failure):
                result = self.run_driver({"failures": [{**failure, "generation": "A", "at": 1}]})
                self.assertFalse(result["result"]["ok"])
                self.assertEqual(result["result"]["classification"], classification)
                self.assert_no_b(result)

    def test_checkpoint_uncertainty_retries_only_identical_calculation_without_broker_replay(self):
        for fault in ({"code": "daily_loss_checkpoint_failed"}, {"malformed": True}):
            with self.subTest(fault=fault):
                result = self.run_driver({"failures": [{"when": "calculation", "at": 1, **fault}]})
                self.assertTrue(result["result"]["ok"], result)
                calculations = [event for event in self.commands(result, "daily_loss.py") if "json-out" in event["options"]]
                self.assertEqual(len(calculations), 2)
                self.assertEqual(calculations[0]["command"], calculations[1]["command"])
                self.assertEqual(len(self.brokers(result)), 5)
                self.assertFalse(self.commands(result, "run_lifecycle.py", "complete-second"))
                self.assert_no_b(result)

    def test_unresolved_checkpoint_forbids_public_terminal_and_keeps_result_unavailable(self):
        result = self.run_driver({"failures": [{"when": "calculation", "code": "daily_loss_checkpoint_failed"}]})
        self.assertFalse(result["result"]["ok"])
        self.assertTrue(result["result"]["checkpoint_uncertain"])
        self.assertEqual(len([event for event in self.commands(result, "daily_loss.py") if "json-out" in event["options"]]), 2)
        self.assertFalse(self.commands(result, "run_lifecycle.py", "complete-second"))
        self.assertNotIn("daily_loss_receipt", result["state"][TRANSPORT])
        self.assert_no_b(result)

    def test_complete_wrapper_is_kept_unchanged_and_execution_fence_prevents_reentry(self):
        result = self.run_driver({"rerun": True, "stopCountTripped": True})
        self.assertTrue(result["result"]["ok"], result)
        self.assertEqual(result["result"]["entry_guard_outcome"], "tripped")
        self.assertIsNone(result["secondThrown"])
        self.assertFalse(result["secondResult"]["ok"])
        after = result["log"][result["firstLogLength"]:]
        self.assertFalse([event for event in after if event["type"] in ("command", "broker", "patch")])
        fence = result["state"][EXECUTION]
        self.assertEqual(fence["status"], "finished")

    def test_chunked_helper_stdout_is_drained_before_using_receipt(self):
        result = self.run_driver({"chunked": True})
        self.assertTrue(result["result"]["ok"], result)
        self.assertEqual(len([event for event in result["log"] if event["type"] == "drain"]), len(self.commands(result)))

    def test_exact_second_cell_runs_whole_driver_without_intervening_model_output(self):
        for chunked in (False, True):
            with self.subTest(chunked=chunked):
                result = self.run_driver({"secondCell": True, "chunked": chunked},
                    routine=os.path.join(ROOT, "robinhood-momentum-routine-autonomous.md"))
                self.assertTrue(result["result"]["ok"], result)
                commands = self.commands(result)
                self.assertEqual(commands[0]["script"], "daily_loss_orchestration.py")
                self.assertEqual(commands[1]["action"], "enter-second")
                self.assertEqual(commands[2]["action"], "reserve-source")
                self.assertEqual(len(self.brokers(result)), 5)
                texts = [index for index, event in enumerate(result["log"]) if event["type"] == "text"]
                self.assertEqual(texts, [len(result["log"]) - 1])

    def test_exact_second_cell_loader_failure_precedes_enter_second_and_broker(self):
        for failure in ("loaderFailure", "loaderInvalidJs"):
            with self.subTest(failure=failure):
                result = self.run_driver({"secondCell": True, failure: True},
                    routine=os.path.join(ROOT, "robinhood-momentum-routine-autonomous.md"))
                self.assertFalse(result["result"]["ok"])
                self.assertEqual(result["result"]["action"], "daily-loss-driver-load-failed")
                self.assertFalse(self.commands(result, "run_lifecycle.py", "enter-second"))
                self.assertFalse(self.brokers(result))

    def test_final_telemetry_clears_all_five_slots_on_success_and_every_failure(self):
        slots = (BOOTSTRAP, TRANSPORT, LEASE, EXECUTION, RSI_EXECUTION)
        cases = [(fault, phase) for fault in (None, "throw", "nonzero", "malformed", "wrong-invocation")
                 for phase in ("lease-released", "lease-lost")]
        for fault, phase in cases:
            with self.subTest(fault=fault, phase=phase):
                result = self.run_driver({"telemetryAfter": True, "telemetryFault": fault,
                    "terminalLeasePhase": phase,
                    "publishedStatus": fault is None, "chunked": fault == "malformed"},
                    routine=os.path.join(ROOT, "robinhood-momentum-routine-autonomous.md"))
                self.assertTrue(result["result"]["ok"])
                self.assertIsNone(result["telemetryThrown"])
                self.assertEqual(result["telemetryResult"]["telemetry_ok"], fault is None)
                self.assertEqual(result["telemetryResult"]["expected_report_file"], "rhmra-log-2026_09_11-11_33.md")
                self.assertEqual({key: result["afterTelemetryState"][key] for key in slots}, dict.fromkeys(slots))
                self.assertEqual(len(self.commands(result, "run_performance.py", "record-internal")), 1)
                cleanup = [event["key"] for event in result["log"] if event["type"] == "store" and event["value"] is None]
                self.assertCountEqual(cleanup, slots)

    def test_nonterminal_telemetry_state_does_not_clear_execution_fence_or_private_lease(self):
        result = self.run_driver({"telemetryAfter": True, "telemetryInvalidTerminal": True},
            routine=os.path.join(ROOT, "robinhood-momentum-routine-autonomous.md"))
        self.assertIn("final machine state is not safely terminal", result["telemetryThrown"])
        self.assertFalse(self.commands(result, "run_performance.py", "record-internal"))
        self.assertEqual(result["afterTelemetryState"][EXECUTION]["status"], "finished")
        self.assertEqual(result["afterTelemetryState"][RSI_EXECUTION]["status"], "finished")
        self.assertEqual(result["afterTelemetryState"][LEASE]["run_lock_token"], "test-private-token")
        self.assertFalse([event for event in result["log"] if event["type"] == "store" and event["value"] is None])

    def test_next_bound_invocation_runs_only_after_successful_terminal_cleanup(self):
        for cleanup in (False, True):
            with self.subTest(cleanup=cleanup):
                result = self.run_driver({"telemetryAfter": cleanup, "nextInvocation": True, "telemetryFault": "throw"},
                    routine=os.path.join(ROOT, "robinhood-momentum-routine-autonomous.md"))
                self.assertTrue(result["result"]["ok"])
                self.assertIsNone(result["nextThrown"])
                self.assertEqual(result["nextResult"]["ok"], cleanup)
                next_events = result["log"][result["nextLogStart"]:]
                if cleanup:
                    self.assertEqual(len([event for event in next_events if event["type"] == "broker"]), 5)
                    self.assertEqual(result["state"][TRANSPORT]["daily_loss_receipt"]["invocation_id"], "next-test-invocation")
                else:
                    self.assertEqual(result["nextResult"]["failure"], "execution-already-started")
                    self.assertFalse([event for event in next_events if event["type"] in ("command", "broker", "patch")])

    def test_multipage_flow_against_real_isolated_python_helpers(self):
        self._real_helper_flow({"pages": {"discovery:positions": 2, "discovery:orders": 2,
            "final:positions": 2, "final:orders": 2}})

    def _real_helper_flow(self, config=None):
        constants = validate_constants.validate_constants_file()
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(market_clock.main(["--json", "--expected-constants-sha256", constants.source_sha256]), 0)
        clock = json.loads(output.getvalue())
        with tempfile.TemporaryDirectory() as temporary:
            native_temp = str(Path(tempfile.gettempdir()).resolve(strict=True))
            scratch = Path(tempfile.mkdtemp(prefix="rhmra-session-", dir=native_temp)).resolve(strict=True)
            source_root = Path(tempfile.mkdtemp(prefix="rhmra-source-", dir=native_temp)).resolve(strict=True)
            self.addCleanup(shutil.rmtree, scratch, True)
            self.addCleanup(shutil.rmtree, source_root, True)
            source_stat = os.lstat(source_root)
            preflight = broker_snapshot._preflight_directory(
                scratch, prepared_source_root=source_root,
                prepared_source_root_identity=(source_stat.st_dev, source_stat.st_ino),
            )
            canary = source_root / "get-accounts.json"
            canary.write_bytes(broker_snapshot._canonical_bytes({"data": {"accounts": [{
                "nickname": "Agentic", "account_number": "test-account", "agentic_allowed": True,
            }]}}))
            broker_snapshot._bind_transport(argparse.Namespace(
                scratch=str(scratch), source_root=str(source_root), canary=str(canary), account_name="Agentic",
            ))
            files = {name: str(Path(temporary) / filename) for name, filename in (
                ("lifecycle", "lifecycle.sqlite3"), ("projection", "lifecycle.json"),
                ("context", "context.json"), ("lock", "lock.sqlite3"),
            )}
            invocation = str(uuid.uuid4())
            run_lifecycle.start_invocation(invocation_id=invocation, state_file=files["lifecycle"],
                projection_file=files["projection"], lock_file=files["lock"])
            run_lifecycle.record_event(invocation_id=invocation, phase="preflight", run_start_pt=clock["pt_iso"],
                state_file=files["lifecycle"], projection_file=files["projection"])
            context = run_lifecycle.acquire_and_bind_active_context(invocation_id=invocation,
                state_file=files["lifecycle"], projection_file=files["projection"],
                context_file=files["context"], lock_file=files["lock"])
            token = context["run_lock_token"]
            run_lifecycle.record_event(invocation_id=invocation, phase="position-management",
                state_file=files["lifecycle"], projection_file=files["projection"])
            second = run_lifecycle.enter_second(invocation_id=invocation, run_token=token, scratch=str(scratch),
                expected_constants_sha256=constants.source_sha256, state_file=files["lifecycle"],
                projection_file=files["projection"], context_file=files["context"], lock_file=files["lock"])
            state = {
                BOOTSTRAP: {"schema_version": 1, "phase": "context-bound", "context_receipt": {"invocation_id": invocation},
                    "start_clock": clock, "constants_receipt": {"source_sha256": constants.source_sha256,
                        "values": {"DAILY_LOSS_HALT_PCT": constants.raw_values["DAILY_LOSS_HALT_PCT"], "STOP_COUNT_HALT": constants.values["STOP_COUNT_HALT"]}}},
                TRANSPORT: {"schema_version": 1, "phase": "transport-bound", "python_exe": sys.executable,
                    "project_root": ROOT, "lease_binding": {"invocation_id": invocation},
                    "context_receipt": {"invocation_id": invocation}, "receipt": preflight,
                    "account_number": "test-account", "agentic_allowed": True, "second_entry_receipt": second},
                LEASE: {"schema_version": 1, "phase": "lease-owned", "invocation_id": invocation, "run_lock_token": token},
            }
            result = self.run_driver(config, state=state, integration={**files, "scratch": str(scratch),
                "source_root": str(source_root), "root": ROOT, "python": sys.executable})
            self.assertTrue(result["result"]["ok"], result)
            receipt = result["state"][TRANSPORT]["daily_loss_receipt"]
            self.assertEqual(receipt["entry_guard_outcome"], "clear")
            self.assertEqual(receipt, json.loads((scratch / "daily-loss-a.json").read_text(encoding="utf-8")))
            with closing(sqlite3.connect(files["lifecycle"])) as connection:
                checkpoints = {row[0] for row in connection.execute(
                    "SELECT checkpoint FROM lifecycle_checkpoints WHERE invocation_id = ?", (invocation,)
                )}
            self.assertIn("daily-loss-clear", checkpoints)
            self.assertNotIn("daily-loss-coordination-terminal", checkpoints)


if __name__ == "__main__":
    unittest.main()
