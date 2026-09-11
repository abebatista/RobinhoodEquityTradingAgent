"""Run the checked-in DAILY-LOSS driver against real, isolated CLI helpers."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


NODE = shutil.which("node")
if NODE is None:
    bundled_node = Path(r"C:\Program Files\nodejs\node.exe")
    if bundled_node.is_file():
        NODE = str(bundled_node)


@unittest.skipUnless(NODE, "Node.js is required to execute the checked-in driver")
class DailyLossDriverIntegrationTests(unittest.TestCase):
    def setUp(self):
        # Production helpers choose their default journals relative to __file__.
        # Copy code, never runtime journals, so every lifecycle write is isolated.
        self.native_temp = Path(tempfile.gettempdir()).resolve(strict=True)
        self.created_directories = []
        self.checkout = Path(tempfile.mkdtemp(
            prefix="rhmra-driver-integration-", dir=self.native_temp
        )).resolve(strict=True)
        self._track_directory(self.checkout, "rhmra-driver-integration-")
        self.addCleanup(self._cleanup_directories)
        for script in Path(ROOT).glob("*.py"):
            shutil.copyfile(script, self.checkout / script.name)
        constants = (Path(ROOT) / "constants.md").read_text(encoding="utf-8")
        constants, count = re.subn(
            r"^(\|\s*`DRY_RUN`\s*\|\s*`)[^`]*(`\s*\|)",
            lambda match: match.group(1) + "true" + match.group(2),
            constants,
            flags=re.MULTILINE,
        )
        self.assertEqual(count, 1)
        (self.checkout / "constants.md").write_text(constants, encoding="utf-8")
        self.private_token = None

    def _track_directory(self, directory, prefix):
        directory = Path(directory)
        self.assertFalse(directory.is_symlink())
        self.assertEqual(directory.resolve(strict=True).parent, self.native_temp)
        self.assertTrue(directory.name.startswith(prefix))
        stat = directory.stat()
        self.created_directories.append((directory, stat.st_dev, stat.st_ino))

    def _cleanup_directories(self):
        # Never remove a replacement path or anything outside our exact issued
        # native-temp children, even when a test fails partway through setup.
        for directory, device, inode in reversed(self.created_directories):
            self.assertFalse(directory.is_symlink())
            self.assertEqual(directory.resolve(strict=True).parent, self.native_temp)
            stat = directory.stat()
            self.assertEqual((stat.st_dev, stat.st_ino), (device, inode))
            shutil.rmtree(directory)

    def _cli(self, script, *args):
        process = subprocess.run(
            [sys.executable, str(self.checkout / script), *map(str, args)],
            cwd=self.checkout, capture_output=True, text=True, timeout=60,
        )
        diagnostic = process.stdout + process.stderr
        if self.private_token:
            diagnostic = diagnostic.replace(self.private_token, "<private lease>")
        self.assertEqual(process.returncode, 0, f"{script}: {diagnostic}")
        return json.loads(process.stdout)

    def test_flat_account_records_real_calculation_and_clear_checkpoint(self):
        self._exercise_flat_account()

    def test_native_semantic_exit_two_authorizes_generation_b_and_records_clear(self):
        self._exercise_flat_account(semantic_failure=True)

    def _exercise_flat_account(self, *, semantic_failure=False):
        started = self._cli("run_lifecycle.py", "start")
        invocation_id = started["invocation_id"]
        constants = self._cli("validate_constants.py", "--json")
        self.assertIs(constants["values"]["DRY_RUN"], True)
        clock = self._cli(
            "market_clock.py", "--json", "--expected-constants-sha256",
            constants["source_sha256"],
        )
        self._cli(
            "run_lifecycle.py", "event", "--invocation-id", invocation_id,
            "--phase", "preflight", "--run-start-pt", clock["pt_iso"],
        )
        acquired = self._cli(
            "run_lifecycle.py", "acquire-bind-context", "--invocation-id", invocation_id
        )
        self.private_token = acquired["run_lock_token"]
        context = acquired["context_receipt"]
        preflight = self._cli("broker_snapshot.py", "preflight", "--create-scratch")
        self._track_directory(preflight["scratch"], "rhmra-session-")
        self._track_directory(preflight["source_root"], "rhmra-source-")
        account_name = constants["values"]["AGENTIC_ACCOUNT_NAME"]
        canary = Path(preflight["source_root"]) / "get-accounts-canary.json"
        accounts = {"data": {"accounts": [{
            "nickname": account_name, "account_number": "synthetic-test-account",
            "agentic_allowed": True,
        }]}}
        canary.write_text(json.dumps({
            "content": [{"type": "text", "text": json.dumps(accounts)}],
            "isError": False,
        }), encoding="utf-8")
        transport = self._cli(
            "broker_snapshot.py", "bind-transport", "--scratch", preflight["scratch"],
            "--source-root", preflight["source_root"], "--canary", canary,
            "--account-name", account_name,
        )
        self.assertFalse(canary.exists())
        self._cli(
            "run_lifecycle.py", "event", "--invocation-id", invocation_id,
            "--phase", "position-management",
        )
        second_entry = self._cli(
            "run_lifecycle.py", "enter-second", "--invocation-id", invocation_id,
            "--run-token", self.private_token, "--scratch", preflight["scratch"],
            "--expected-constants-sha256", constants["source_sha256"],
        )
        machine_state = {
            "rhmra.bootstrap-state.v1": {
                "schema_version": 1, "phase": "context-bound",
                "resolver_receipt": {"schema_version": 1, "status": "valid",
                                     "python": sys.executable,
                                     "version": sys.version.split()[0]},
                "lifecycle_receipt": started, "constants_receipt": constants,
                "context_receipt": context, "start_clock": clock,
            },
            "rhmra.transport-state.v1": {
                "schema_version": 1, "phase": "transport-bound",
                "python_exe": sys.executable, "project_root": str(self.checkout),
                "context_receipt": context,
                "lease_binding": {"schema_version": 1, "invocation_id": invocation_id},
                "receipt": preflight, "canary_path": None,
                "account_name": transport["account_name"],
                "account_number": transport["account_number"],
                "agentic_allowed": transport["agentic_allowed"],
                "second_entry_receipt": second_entry,
            },
            "rhmra.lease-state.v1": {
                "schema_version": 1, "phase": "lease-owned",
                "invocation_id": invocation_id, "run_lock_token": self.private_token,
            },
        }
        driver = Path(ROOT) / "tools" / "daily_loss_orchestration.js"
        fixture = Path(ROOT) / "tests" / "fixtures" / "daily_loss_orchestration_integration.js"
        process = subprocess.run(
            [NODE, str(fixture), str(driver), "semantic-a" if semantic_failure else "flat"],
            input=json.dumps(machine_state),
            cwd=self.checkout, capture_output=True, text=True, timeout=180,
        )
        diagnostic = (process.stdout + process.stderr).replace(
            self.private_token, "<private lease>"
        )
        self.assertEqual(process.returncode, 0, diagnostic)
        result = json.loads(process.stdout)
        self.assertTrue(result["outcome"]["ok"], result)
        generation = "B" if semantic_failure else "A"
        self.assertEqual(result["outcome"]["generation"], generation)
        completed_generation_reads = [
            "get_equity_positions", "get_equity_orders", "get_portfolio",
            "get_equity_positions", "get_equity_orders",
        ]
        expected_reads = completed_generation_reads
        if semantic_failure:
            expected_reads = completed_generation_reads[:3] + completed_generation_reads
            self.assertEqual(len(result["semantic_failures"]), 1)
            failure = result["semantic_failures"][0]
            self.assertEqual(failure["exit_code"], 2)
            self.assertEqual(failure["receipt"]["error"]["code"], "stage_semantic_invalid")
            self.assertEqual(failure["receipt"]["generation"], "A")
            self.assertEqual(failure["receipt"]["kind"], "portfolio")
            self.assertEqual(failure["receipt"]["recovery_action"], "generation-b")
            self.assertEqual(result["generation_actions"], [
                "authorize-generation-b", "finish-generation-b",
            ])
            self.assertFalse((Path(preflight["scratch"]) / "daily-loss-a.json").exists())
        else:
            self.assertEqual(result["semantic_failures"], [])
            self.assertEqual(result["generation_actions"], [])
        self.assertEqual(result["broker_calls"], expected_reads)
        self.assertEqual(result["saved_responses"], len(expected_reads))
        self.assertIn("daily_loss.py", result["helper_scripts"])
        self.assertNotIn("complete-second", result["lifecycle_actions"])
        self.assertTrue(result["private_lease_unchanged"])
        self.assertFalse(result["private_token_leaked"])
        calculation_name = f"daily-loss-{generation.lower()}.json"
        calculation_path = Path(preflight["scratch"]) / calculation_name
        calculation = json.loads(calculation_path.read_text(encoding="utf-8"))
        self.assertEqual(result["calculation_receipt"], calculation)
        self.assertFalse(calculation["daily_loss_tripped"])
        self.assertFalse(calculation["stop_count_tripped"])
        state_path = self.checkout / "run-reports" / "rhmra-run-lifecycle.sqlite3"
        with closing(sqlite3.connect(f"{state_path.as_uri()}?mode=ro", uri=True)) as connection:
            checkpoints = connection.execute(
                "SELECT checkpoint FROM lifecycle_checkpoints WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchall()
            evidence = connection.execute(
                "SELECT outcome, generation, result_file, result_sha256, constants_sha256 "
                "FROM lifecycle_second_evidence WHERE invocation_id = ?", (invocation_id,),
            ).fetchall()
        self.assertEqual({row[0] for row in checkpoints}, {
            "entry-eligible", "daily-loss-attempted", "daily-loss-clear",
        })
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0][0:2], ("clear", generation))
        self.assertEqual(Path(evidence[0][2]).name, calculation_name)
        self.assertEqual(evidence[0][3], hashlib.sha256(calculation_path.read_bytes()).hexdigest())
        self.assertEqual(evidence[0][4], constants["source_sha256"])
        self._cli("run_lifecycle.py", "validate")
        finished = self._cli(
            "run_lifecycle.py", "release-finish", "--invocation-id", invocation_id,
            "--run-token", self.private_token, "--classification", "completed",
        )
        self.assertTrue(finished["ok"])


if __name__ == "__main__":
    unittest.main()
