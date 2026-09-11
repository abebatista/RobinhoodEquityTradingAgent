"""Exercise the production RSI continuation with real isolated CLI helpers."""

from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

NODE = shutil.which("node")
if NODE is None and Path(r"C:\Program Files\nodejs\node.exe").is_file():
    NODE = r"C:\Program Files\nodejs\node.exe"


@unittest.skipUnless(NODE, "Node.js is required for the checked-in RSI continuation")
class RsiOrchestrationIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.native_temp = Path(tempfile.gettempdir()).resolve(strict=True)
        self.directories = []
        self.checkout = Path(tempfile.mkdtemp(
            prefix="rhmra-rsi-integration-", dir=self.native_temp
        )).resolve(strict=True)
        self._track_directory(self.checkout, "rhmra-rsi-integration-")
        self.addCleanup(self._cleanup_directories)
        self.private_token = None
        self.source_paths = {}
        for script in Path(ROOT).glob("*.py"):
            shutil.copyfile(script, self.checkout / script.name)
        (self.checkout / "tools").mkdir()
        shutil.copyfile(Path(ROOT) / "tools" / "rsi_orchestration.js",
                        self.checkout / "tools" / "rsi_orchestration.js")
        constants = (Path(ROOT) / "constants.md").read_text(encoding="utf-8")
        constants, count = re.subn(
            r"^(\|\s*`DRY_RUN`\s*\|\s*`)[^`]*(`\s*\|)",
            lambda match: match.group(1) + "true" + match.group(2),
            constants, flags=re.MULTILINE,
        )
        self.assertEqual(count, 1)
        (self.checkout / "constants.md").write_text(constants, encoding="utf-8")

    def _track_directory(self, directory, prefix):
        directory = Path(directory)
        self.assertFalse(directory.is_symlink())
        self.assertEqual(directory.resolve(strict=True).parent, self.native_temp)
        self.assertTrue(directory.name.startswith(prefix))
        stat = directory.stat()
        self.directories.append((directory, stat.st_dev, stat.st_ino))

    def _cleanup_directories(self):
        for directory, device, inode in reversed(self.directories):
            self.assertFalse(directory.is_symlink())
            self.assertEqual(directory.resolve(strict=True).parent, self.native_temp)
            stat = directory.stat()
            self.assertEqual((stat.st_dev, stat.st_ino), (device, inode))
            shutil.rmtree(directory)

    def _cli(self, script, *args, expected=0):
        process = subprocess.run(
            [sys.executable, str(self.checkout / script), *map(str, args)],
            cwd=self.checkout, capture_output=True, text=True, timeout=60,
        )
        diagnostic = process.stdout + process.stderr
        if self.private_token:
            diagnostic = diagnostic.replace(self.private_token, "<private lease>")
        self.assertEqual(process.returncode, expected, f"{script}: {diagnostic}")
        return json.loads(process.stdout)

    def _commit(self, purpose, payload):
        reserved = self._cli("broker_snapshot.py", "reserve-source", "--scratch",
                             self.scratch, "--purpose", purpose)
        self.source_paths[purpose] = reserved["source"]
        with open(reserved["source"], "x", encoding="utf-8") as handle:
            json.dump(payload, handle)
        return self._cli("broker_snapshot.py", "commit-source", "--scratch",
                         self.scratch, "--purpose", purpose)

    def _prepare(self, symbols=("TEST",), *, pre_candidates=True):
        started = self._cli("run_lifecycle.py", "start")
        invocation_id = started["invocation_id"]
        constants = self._cli("validate_constants.py", "--json")
        values = constants["values"]
        self.assertIs(values["DRY_RUN"], True)
        clock = self._cli("market_clock.py", "--json", "--expected-constants-sha256",
                          constants["source_sha256"])
        self._cli("run_lifecycle.py", "event", "--invocation-id", invocation_id,
                  "--phase", "preflight", "--run-start-pt", clock["pt_iso"])
        acquired = self._cli("run_lifecycle.py", "acquire-bind-context",
                             "--invocation-id", invocation_id)
        self.private_token = acquired["run_lock_token"]
        context = acquired["context_receipt"]
        preflight = self._cli("broker_snapshot.py", "preflight", "--create-scratch")
        self.scratch = preflight["scratch"]
        self._track_directory(self.scratch, "rhmra-session-")
        self._track_directory(preflight["source_root"], "rhmra-source-")
        canary = Path(preflight["source_root"]) / "get-accounts-canary.json"
        canary.write_text(json.dumps({"data": {"accounts": [{
            "nickname": values["AGENTIC_ACCOUNT_NAME"],
            "account_number": "synthetic-test-account", "agentic_allowed": True,
        }]}}), encoding="utf-8")
        bound = self._cli("broker_snapshot.py", "bind-transport", "--scratch", self.scratch,
                          "--source-root", preflight["source_root"], "--canary", canary,
                          "--account-name", values["AGENTIC_ACCOUNT_NAME"])
        bars = []
        for offset in range(max(values["VOLUME_LOOKBACK_DAYS"], values["HIGH_LOOKBACK_DAYS"])):
            day = date(2026, 7, 1) + timedelta(days=offset)
            bars.append({"begins_at": day.isoformat() + "T00:00:00Z", "open_price": "4.5",
                         "close_price": "4.5", "high_price": "5", "low_price": "4.5",
                         "volume": 900000, "session": "reg"})
        self._commit("historicals-0", {"data": {"results": [
            {"symbol": symbol, "bars": bars} for symbol in symbols]}})
        price = "4" if pre_candidates else "5"
        self._commit("candidate-quotes-0", {"data": {"results": [{"quote": {
            "symbol": symbol, "last_trade_price": price, "bid_price": price,
            "ask_price": price}} for symbol in symbols]}})
        arguments = ["--scratch", self.scratch, "--bars-purpose", "historicals-0",
                     "--quotes-purpose", "candidate-quotes-0", "--expected-symbols", *symbols]
        for flag, name in (
            ("volume-lookback-days", "VOLUME_LOOKBACK_DAYS"),
            ("high-lookback-days", "HIGH_LOOKBACK_DAYS"),
            ("min-median-dollar-volume", "MIN_MEDIAN_DOLLAR_VOLUME"),
            ("dip-entry-pct", "DIP_ENTRY_PCT"),
            ("max-spread-buy-pct", "MAX_SPREAD_BUY_PCT"),
            ("rsi-period", "RSI_PERIOD"),
        ):
            arguments.extend(["--" + flag, str(values[name])])
        pre_rsi_path = Path(self.scratch) / "pre-rsi-gates.json"
        pre_rsi = self._cli("evaluate_candidates.py", *arguments,
                            "--json-out", pre_rsi_path, "--emit-json")
        self.assertEqual(pre_rsi, json.loads(pre_rsi_path.read_text(encoding="utf-8")))
        self.assertEqual([row["symbol"] for row in pre_rsi["results"]], sorted(symbols))
        self.assertTrue(all(row["buy_candidate"] is pre_candidates for row in pre_rsi["results"]))
        self.state = {
            "rhmra.bootstrap-state.v1": {"schema_version": 1, "phase": "context-bound",
                "resolver_receipt": {"schema_version": 1, "status": "valid",
                    "python": sys.executable, "version": sys.version.split()[0]},
                "lifecycle_receipt": started, "context_receipt": context,
                "constants_receipt": constants, "start_clock": clock},
            "rhmra.transport-state.v1": {"schema_version": 1, "phase": "transport-bound",
                "python_exe": sys.executable, "project_root": str(self.checkout),
                "context_receipt": context,
                "lease_binding": {"schema_version": 1, "invocation_id": invocation_id},
                "receipt": preflight, "account_name": bound["account_name"],
                "account_number": bound["account_number"], "agentic_allowed": True,
                "pre_rsi_receipt": pre_rsi, "historicals_purposes": ["historicals-0"],
                "candidate_quotes_purposes": ["candidate-quotes-0"],
                "expected_symbols": list(symbols)},
            "rhmra.lease-state.v1": {"schema_version": 1, "phase": "lease-owned",
                "invocation_id": invocation_id, "run_lock_token": self.private_token},
        }
        self.expected_gate = self.checkout / "run-reports" / context["expected_gate_file"]
        return self.state

    def _run(self, config=None, *, cell=None):
        process = subprocess.run(
            [NODE, str(Path(ROOT) / "tests" / "fixtures" / "rsi_orchestration_harness.js"),
             str(Path(ROOT) / "tools" / "rsi_orchestration.js")],
            input=json.dumps({"state": self.state, "config": config or {}, "cell": cell}),
            cwd=self.checkout, capture_output=True, text=True, timeout=180,
        )
        diagnostic = (process.stdout + process.stderr).replace(self.private_token, "<private lease>")
        self.assertEqual(process.returncode, 0, diagnostic)
        result = json.loads(process.stdout)
        self.assertTrue(result["private_lease_unchanged"])
        self.assertFalse(result["private_token_leaked"])
        return result

    @staticmethod
    def _events(result, kind, action=None):
        return [event for event in result["events"] if event["type"] == kind
                and (action is None or event.get("action") == action)]

    def _assert_final_gate(self, result):
        self.assertTrue(result["outcome"]["ok"], result)
        self.assertTrue(result["outcome"]["final_refresh_allowed"], result)
        gate = json.loads(self.expected_gate.read_text(encoding="utf-8"))
        self.assertTrue(gate["rsi_gate_enabled"])
        self.assertEqual(gate, result["public_state"]["rhmra.transport-state.v1"]["final_gate_receipt"])
        return gate

    def _assert_fresh_refresh_can_be_committed(self):
        self._commit("final-status-a-portfolio", {"data": {"total_value": "1000",
            "equity_value": "0", "cash": "1000", "buying_power": {"buying_power": "1000"}}})
        receipt = self._cli("connector_contract.py", "portfolio", "--scratch", self.scratch,
                            "--source-purpose", "final-status-a-portfolio")
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["values"]["equity_value"], "0")

    def test_two_rsi_failures_abort_exact_reservation_then_finish_missing_rsi_gate(self):
        self._prepare()
        result = self._run({"readFailures": {"TEST": 2}, "throwErrors": True})
        reads = self._events(result, "broker")
        self.assertEqual(len(reads), 2)
        self.assertEqual(reads[0]["args"], reads[1]["args"])
        self.assertEqual(set(reads[0]["args"]), {
            "symbol", "type", "interval", "period", "output", "start_time", "end_time",
            "bounds", "adjustment_type",
        })
        values = self.state["rhmra.bootstrap-state.v1"]["constants_receipt"]["values"]
        self.assertEqual(reads[0]["args"]["period"], values["RSI_PERIOD"])
        self.assertEqual(reads[0]["args"]["interval"], values["RSI_INTERVAL"])
        self.assertEqual(reads[0]["args"]["output"], f"last:{values['RSI_LOOKBACK_BARS'] + 1}")
        self.assertEqual(reads[0]["args"]["bounds"], "regular")
        self.assertEqual(reads[0]["args"]["adjustment_type"], "split")
        reserves = self._events(result, "helper", "reserve-source")
        aborts = self._events(result, "helper", "abort-source")
        self.assertEqual(len(aborts), 1)
        self.assertEqual(aborts[0]["receipt"]["reservation_id"], reserves[0]["receipt"]["reservation_id"])
        self.assertIn("--reservation-id", aborts[0]["command"])
        self.assertEqual(aborts[0]["receipt"]["reason"], "connector-failed")
        self.assertEqual([event["receipt"]["purpose"] for event in reserves], ["rsi-0", "rsi-empty"])
        self.assertEqual([event["payload"] for event in self._events(result, "write")], [{}])
        gate = self._assert_final_gate(result)
        self.assertEqual(gate["params"]["rsi_file"], ["rsi-empty"])
        self.assertFalse(gate["results"][0]["buy_candidate"])
        self.assertEqual(gate["results"][0]["rsi_gate"], "block")
        self.assertIn("no/insufficient data", gate["results"][0]["rsi_reason"])
        self.assertEqual(result["outcome"]["exhausted_count"], 1)
        self._assert_fresh_refresh_can_be_committed()

    def test_recovered_read_preserves_full_response_and_identical_request(self):
        self._prepare()
        result = self._run({"readFailures": {"TEST": 1}})
        reads = self._events(result, "broker")
        self.assertEqual(len(reads), 2)
        self.assertEqual(reads[0]["args"], reads[1]["args"])
        self.assertEqual(len(self._events(result, "helper", "reserve-source")), 1)
        self.assertFalse(self._events(result, "helper", "abort-source"))
        self.assertEqual(self._events(result, "write")[0]["payload"],
                         self._events(result, "broker-response")[0]["payload"])
        gate = self._assert_final_gate(result)
        self.assertTrue(gate["results"][0]["buy_candidate"])
        self.assertEqual(gate["results"][0]["rsi_gate"], "pass")
        self.assertEqual(result["outcome"]["diagnostics"], [{"symbol": "TEST", "status": "recovered"}])
        helper_events = self._events(result, "helper")
        clock = next(event["receipt"] for event in helper_events if event["script"] == "market_clock.py")
        plan = next(event["receipt"] for event in helper_events if event["script"] == "rsi_orchestration.py")
        self.assertEqual(plan["as_of_utc"], clock["utc"])
        self.assertEqual(reads[0]["args"], plan["requests"][0]["arguments"])
        self.assertEqual(gate["params"]["rsi_as_of_utc"], clock["utc"])
        self._assert_fresh_refresh_can_be_committed()

    def test_exhausted_candidate_does_not_block_other_candidates(self):
        self._prepare(("TEST", "ZZZZ"))
        result = self._run({"readFailures": {"TEST": 2}})
        gate = self._assert_final_gate(result)
        rows = {row["symbol"]: row for row in gate["results"]}
        self.assertFalse(rows["TEST"]["buy_candidate"])
        self.assertTrue(rows["ZZZZ"]["buy_candidate"])
        self.assertEqual(gate["params"]["rsi_file"], ["rsi-1"])
        self.assertEqual(result["outcome"]["candidate_count"], 1)
        self._assert_fresh_refresh_can_be_committed()

    def test_no_pre_rsi_candidates_still_produces_final_enabled_gate(self):
        self._prepare(pre_candidates=False)
        result = self._run()
        self.assertFalse(self._events(result, "broker"))
        gate = self._assert_final_gate(result)
        self.assertEqual(gate["params"]["rsi_file"], ["rsi-empty"])
        self.assertFalse(gate["results"][0]["buy_candidate"])
        self._assert_fresh_refresh_can_be_committed()

    def test_malformed_committed_success_is_not_fetched_or_repaired_again(self):
        self._prepare()
        result = self._run({"malformedSuccess": True})
        self.assertFalse(result["outcome"]["ok"])
        self.assertEqual(len(self._events(result, "broker")), 1)
        self.assertEqual(len(self._events(result, "helper", "commit-source")), 1)
        self.assertFalse(self._events(result, "helper", "abort-source"))
        self.assertFalse(self.expected_gate.exists())
        self.assertTrue(result["outcome"]["final_refresh_allowed"], result)
        evaluation = next(event for event in self._events(result, "helper")
                          if event["script"] == "evaluate_candidates.py")
        self.assertEqual(evaluation["exit_code"], 2)
        self.assertEqual(evaluation["receipt"]["error"]["code"], "candidate_evaluation_failed")
        self._assert_fresh_refresh_can_be_committed()

    def test_source_changed_after_plan_and_rsi_commit_forbids_final_refresh(self):
        self._prepare()
        result = self._run({"tamperBeforeFinal": self.source_paths["historicals-0"]})
        self.assertFalse(result["outcome"]["ok"])
        self.assertEqual(result["outcome"]["classification"], "snapshot-failure")
        self.assertEqual(result["outcome"]["reason_code"], "snapshot-write-failed")
        self.assertFalse(result["outcome"]["final_refresh_allowed"])
        self.assertEqual(len(self._events(result, "broker")), 1)
        self.assertEqual(len(self._events(result, "helper", "commit-source")), 1)
        self.assertEqual(len(self._events(result, "tamper")), 1)
        self.assertFalse(self._events(result, "helper", "abort-source"))
        self.assertFalse(self.expected_gate.exists())
        evaluation = next(event for event in self._events(result, "helper")
                          if event["script"] == "evaluate_candidates.py")
        self.assertEqual(evaluation["exit_code"], 2)
        self.assertEqual(evaluation["receipt"]["error"]["code"], "candidate_source_transport_failed")

    def _assert_untyped_final_reply_blocks_refresh(self, failure):
        self._prepare()
        result = self._run({"finalReplyFailure": failure})
        self.assertFalse(result["outcome"]["ok"])
        self.assertFalse(result["outcome"]["final_refresh_allowed"])
        self.assertEqual(len(self._events(result, "broker")), 1)
        self.assertFalse(self._events(result, "helper", "abort-source"))
        evaluation = [event for event in self._events(result, "helper")
                      if event["script"] == "evaluate_candidates.py"]
        self.assertEqual(len(evaluation), 1)
        self.assertEqual(evaluation[0]["reply_failure"], failure)
        self.assertNotIn("final_gate_receipt", result["public_state"]["rhmra.transport-state.v1"])

    def test_malformed_final_receipt_cannot_enable_refresh_or_replay(self):
        self._assert_untyped_final_reply_blocks_refresh("malformed-success")

    def test_unknown_nonzero_final_receipt_cannot_enable_refresh_or_replay(self):
        self._assert_untyped_final_reply_blocks_refresh("unknown-nonzero")

    def test_lost_final_command_receipt_cannot_enable_refresh_or_replay(self):
        self._assert_untyped_final_reply_blocks_refresh("throw-after-success")

    def test_swapped_response_symbols_cannot_pass_even_when_both_were_expected(self):
        self._prepare(("TEST", "ZZZZ"))
        result = self._run({"responseSymbols": {"TEST": "ZZZZ", "ZZZZ": "TEST"}})
        self.assertFalse(result["outcome"]["ok"])
        self.assertEqual(len(self._events(result, "broker")), 2)
        self.assertEqual(len(self._events(result, "helper", "commit-source")), 2)
        self.assertFalse(self._events(result, "helper", "abort-source"))
        self.assertFalse(self.expected_gate.exists())
        self.assertTrue(result["outcome"]["final_refresh_allowed"], result)
        self._assert_fresh_refresh_can_be_committed()

    def test_abort_failure_retains_pending_fence_and_prevents_continuation(self):
        self._prepare()
        result = self._run({"readFailures": {"TEST": 2}, "abortFailure": True})
        self.assertFalse(result["outcome"]["ok"])
        self.assertFalse(result["outcome"]["final_refresh_allowed"])
        self.assertEqual(len(self._events(result, "broker")), 2)
        self.assertFalse(self._events(result, "write"))
        self.assertFalse(self.expected_gate.exists())
        fenced = self._cli("broker_snapshot.py", "reserve-source", "--scratch", self.scratch,
                           "--purpose", "final-status-a-portfolio", expected=2)
        self.assertEqual(fenced["error"]["code"], "source_handoff_pending")

    def test_uncertain_written_source_does_not_abort_or_repeat_read(self):
        self._prepare()
        result = self._run({"patchFailure": "after-write"})
        self.assertFalse(result["outcome"]["ok"])
        self.assertFalse(result["outcome"]["final_refresh_allowed"])
        self.assertEqual(len(self._events(result, "broker")), 1)
        self.assertFalse(self._events(result, "helper", "abort-source"))
        self.assertFalse(self._events(result, "helper", "commit-source"))
        self.assertFalse(self.expected_gate.exists())
        lookup = self._cli("broker_snapshot.py", "lookup-source", "--scratch", self.scratch,
                           "--purpose", "rsi-0")
        self.assertEqual((lookup["status"], lookup["recovery_action"]), ("reserved", "commit-only"))

    def test_unavailable_indicator_tool_blocks_candidate_and_allows_final_refresh(self):
        self._prepare()
        result = self._run({"missingTool": True})
        self.assertFalse(self._events(result, "broker"))
        gate = self._assert_final_gate(result)
        self.assertFalse(gate["results"][0]["buy_candidate"])
        self.assertEqual([event["receipt"]["purpose"] for event in
                          self._events(result, "helper", "reserve-source")], ["rsi-empty"])
        self._assert_fresh_refresh_can_be_committed()

    def test_ambiguous_indicator_tool_stops_before_any_rsi_reservation(self):
        self._prepare()
        result = self._run({"duplicateTool": True})
        self.assertFalse(result["outcome"]["ok"])
        self.assertFalse(self._events(result, "broker"))
        self.assertFalse(self._events(result, "helper", "reserve-source"))
        self.assertFalse(self.expected_gate.exists())

    def test_invalid_pre_rsi_document_skips_reads_and_preserves_final_refresh(self):
        self._prepare()
        pre_rsi_path = Path(self.scratch) / "pre-rsi-gates.json"
        document = json.loads(pre_rsi_path.read_text(encoding="utf-8"))
        document["rsi_gate_enabled"] = True
        pre_rsi_path.write_text(json.dumps(document), encoding="utf-8")
        result = self._run()
        self.assertFalse(result["outcome"]["ok"])
        self.assertTrue(result["outcome"]["final_refresh_allowed"], result)
        self.assertFalse(self._events(result, "broker"))
        self.assertFalse(self._events(result, "helper", "reserve-source"))
        self._assert_fresh_refresh_can_be_committed()

    def test_preexisting_pending_source_stops_before_indicator_calls(self):
        self._prepare()
        self._cli("broker_snapshot.py", "reserve-source", "--scratch", self.scratch,
                  "--purpose", "uncertain-earlier-read")
        result = self._run()
        self.assertFalse(result["outcome"]["ok"])
        self.assertFalse(result["outcome"]["final_refresh_allowed"])
        self.assertFalse(self._events(result, "broker"))
        self.assertFalse(self._events(result, "helper", "reserve-source"))
        self.assertFalse(self.expected_gate.exists())

    def test_invalid_pre_file_cannot_hide_preexisting_pending_source(self):
        self._prepare()
        self._cli("broker_snapshot.py", "reserve-source", "--scratch", self.scratch,
                  "--purpose", "uncertain-earlier-read")
        (Path(self.scratch) / "pre-rsi-gates.json").write_text("{broken", encoding="utf-8")
        result = self._run()
        self.assertFalse(result["outcome"]["ok"])
        self.assertEqual(result["outcome"]["classification"], "snapshot-failure")
        self.assertEqual(result["outcome"]["reason_code"], "snapshot-write-failed")
        self.assertFalse(result["outcome"]["final_refresh_allowed"])
        self.assertFalse(self._events(result, "broker"))
        self.assertFalse(self._events(result, "helper", "reserve-source"))
        plan = next(event for event in self._events(result, "helper")
                    if event["script"] == "rsi_orchestration.py")
        self.assertEqual(plan["receipt"]["error"]["code"], "source_transport_invalid")

    def test_invalid_pre_file_cannot_hide_invalid_source_root_binding(self):
        self._prepare()
        (Path(self.scratch) / "pre-rsi-gates.json").write_text("{broken", encoding="utf-8")
        marker = Path(self.scratch) / ".rhmra-broker-response-transport.json"
        self.assertTrue(marker.is_file())
        marker.write_text("{}", encoding="utf-8")
        result = self._run()
        self.assertFalse(result["outcome"]["ok"])
        self.assertEqual(result["outcome"]["classification"], "snapshot-failure")
        self.assertEqual(result["outcome"]["reason_code"], "snapshot-write-failed")
        self.assertFalse(result["outcome"]["final_refresh_allowed"])
        self.assertFalse(self._events(result, "broker"))
        self.assertFalse(self._events(result, "helper", "reserve-source"))
        plan = next(event for event in self._events(result, "helper")
                    if event["script"] == "rsi_orchestration.py")
        self.assertEqual(plan["receipt"]["error"]["code"], "source_transport_invalid")

    def test_exact_routine_loader_cell_runs_real_plan_read_and_final_evaluator(self):
        self._prepare()
        routine = (Path(ROOT) / "robinhood-momentum-routine-autonomous.md").read_text(encoding="utf-8")
        section = routine.split("**CODEX RSI + FINAL EVALUATION — EXACT EXECUTABLE CELL:**", 1)[1]
        cell = re.search(r"```javascript\n(.*?)\n```", section, flags=re.DOTALL).group(1)
        result = self._run(cell=cell)
        gate = self._assert_final_gate(result)
        self.assertTrue(gate["results"][0]["buy_candidate"])
        helper_events = self._events(result, "helper")
        self.assertEqual((helper_events[0]["script"], helper_events[0]["action"]),
                         ("rsi_orchestration.py", "--emit-javascript"))
        self.assertTrue(helper_events[0]["receipt"]["ok"])
        self.assertEqual(len(self._events(result, "broker")), 1)
        emissions = self._events(result, "emit")
        self.assertEqual(len(emissions), 1)
        self.assertTrue(emissions[0]["final_gate_bound"])
        self.assertEqual(result["events"][-1], emissions[0])
        self.assertEqual(emissions[0]["value"], result["outcome"])
        self._assert_fresh_refresh_can_be_committed()

    def test_constants_change_fails_fresh_clock_before_any_rsi_reservation(self):
        self._prepare()
        # Deliberately alter only the synthetic copy; the original remains local
        # maintainer-controlled account configuration and is never written.
        copied_constants = self.checkout / "constants.md"
        with copied_constants.open("a", encoding="utf-8") as handle:
            handle.write("\nSynthetic changed configuration hash.\n")
        result = self._run()
        self.assertFalse(result["outcome"]["ok"])
        self.assertEqual(result["outcome"]["classification"], "configuration-halt", result)
        self.assertEqual(result["outcome"]["reason_code"], "configuration-invalid")
        self.assertFalse(result["outcome"]["final_refresh_allowed"])
        self.assertFalse(self._events(result, "broker"))
        self.assertFalse(self._events(result, "helper", "reserve-source"))

    def test_loader_emits_exact_checked_in_source_and_hash(self):
        receipt = self._cli("rsi_orchestration.py", "--emit-javascript")
        source = (Path(ROOT) / "tools" / "rsi_orchestration.js").read_bytes()
        self.assertTrue(receipt["ok"])
        self.assertEqual(receipt["source"].encode("utf-8"), source)
        self.assertEqual(receipt["source_sha256"], hashlib.sha256(source).hexdigest())


if __name__ == "__main__":
    unittest.main()
