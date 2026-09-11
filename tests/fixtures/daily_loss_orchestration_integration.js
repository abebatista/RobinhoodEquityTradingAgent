"use strict";

// This harness executes the production driver and Python CLIs. Its only broker
// surface consists of immutable synthetic read responses; no MCP is connected.
const fs = require("node:fs");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const assert = require("node:assert/strict");

(async () => {
  const state = JSON.parse(fs.readFileSync(0, "utf8"));
  const transport = state["rhmra.transport-state.v1"];
  const root = fs.realpathSync(transport.project_root);
  const sourceRoot = fs.realpathSync(transport.receipt.source_root);
  const sourceIdentity = fs.statSync(sourceRoot);
  const lease = state["rhmra.lease-state.v1"];
  const originalLease = JSON.stringify(lease);
  const brokerCalls = [];
  const helperScripts = [];
  const lifecycleActions = [];
  const generationActions = [];
  const semanticFailures = [];
  const savedSources = new Set();
  const failFirstPortfolio = process.argv[3] === "semantic-a";
  let portfolioReads = 0;
  const allowedScripts = new Set([
    "broker_snapshot.py", "daily_loss.py", "market_clock.py", "run_lock.py",
    "run_lifecycle.py", "connector_contract.py",
  ]);
  const api = {
    write_stdin: async () => {throw new Error("synchronous test helper cannot have a session");},
    exec_command: async args => {
      assert.equal(fs.realpathSync(args.workdir), root);
      const script = args.cmd.match(/\b([a-z_]+\.py)\b/);
      assert.ok(script && allowedScripts.has(script[1]), "unexpected helper");
      helperScripts.push(script[1]);
      const action = args.cmd.slice(script.index + script[0].length).trim().split(/\s+/)[0];
      if (script[1] === "run_lifecycle.py") {
        lifecycleActions.push(action);
      }
      if (script[1] === "broker_snapshot.py" &&
          ["authorize-generation-b", "finish-generation-b"].includes(action)) {
        generationActions.push(action);
      }
      const windows = process.platform === "win32";
      if (windows) assert.equal(args.shell, "powershell.exe");
      const child = spawnSync(windows ? "powershell.exe" : "/bin/sh",
        windows ? ["-NoProfile", "-NonInteractive", "-Command", args.cmd] : ["-c", args.cmd],
        {cwd: root, encoding: "utf8", timeout: 60000, maxBuffer: 2 * 1024 * 1024});
      assert.ifError(child.error);
      if (script[1] === "broker_snapshot.py" && action === "stage" && child.status !== 0) {
        // Capture the real CLI's structured semantic failure and native status;
        // do not fabricate the typed envelope whose routing this test verifies.
        semanticFailures.push({exit_code: child.status, receipt: JSON.parse(child.stdout)});
      }
      return {exit_code: child.status, output: child.stdout + child.stderr};
    },
    apply_patch: async patch => {
      const lines = patch.split("\n");
      assert.equal(lines.shift(), "*** Begin Patch");
      const header = lines.shift();
      assert.ok(header.startsWith("*** Add File: "));
      const target = path.resolve(header.slice("*** Add File: ".length));
      assert.equal(path.dirname(target), sourceRoot);
      const currentIdentity = fs.statSync(sourceRoot);
      assert.equal(currentIdentity.dev, sourceIdentity.dev);
      assert.equal(currentIdentity.ino, sourceIdentity.ino);
      while (lines.at(-1) === "") lines.pop();
      assert.equal(lines.pop(), "*** End Patch");
      assert.ok(lines.every(line => line.startsWith("+")));
      assert.ok(!savedSources.has(target), "source response must be saved once");
      fs.writeFileSync(target, lines.map(line => line.slice(1)).join("\n") + "\n", {flag: "wx"});
      savedSources.add(target);
      return "Success. Updated the following files.";
    },
  };
  const payloads = {
    get_equity_positions: {data: {positions: [], next: null}},
    get_equity_orders: {data: {orders: [], next: null}},
    get_portfolio: {data: {total_value: "1000", cash: "1000", equity_value: "0",
      buying_power: {buying_power: "1000"}}},
  };
  const allTools = [];
  for (const shortName of [...Object.keys(payloads), "get_equity_quotes"]) {
    const name = "mcp__robinhood_mcp__" + shortName;
    allTools.push({name, description: "Synthetic read-only test tool"});
    api[name] = async args => {
      assert.notEqual(shortName, "get_equity_quotes", "flat account must not fetch quotes");
      assert.deepEqual(args, {account_number: transport.account_number});
      brokerCalls.push(shortName);
      let payload = payloads[shortName];
      if (shortName === "get_portfolio") {
        portfolioReads += 1;
        if (failFirstPortfolio && portfolioReads === 1) {
          // A successful broker envelope with missing authoritative buying
          // power is a genuine deterministic stage-semantic failure.
          payload = {data: {total_value: "1000"}};
        }
      }
      return {content: [{type: "text", text: JSON.stringify(payload)}], isError: false};
    };
  }
  const tools = new Proxy(api, {get(target, key) {
    assert.ok(Object.hasOwn(target, key), "forbidden tool: " + String(key));
    return target[key];
  }});
  const source = fs.readFileSync(process.argv[2], "utf8");
  const runDailyLoss = new Function('"use strict"; return (' + source + '\n);')();
  const outcome = await runDailyLoss({tools, allTools,
    load: key => state[key], store: (key, value) => {state[key] = value;}});
  const publicState = Object.fromEntries(Object.entries(state).filter(([key]) => key !== "rhmra.lease-state.v1"));
  const output = {
    outcome, broker_calls: brokerCalls, saved_responses: savedSources.size,
    helper_scripts: helperScripts, lifecycle_actions: lifecycleActions,
    generation_actions: generationActions, semantic_failures: semanticFailures,
    calculation_receipt: state["rhmra.transport-state.v1"].daily_loss_receipt,
    private_lease_unchanged: JSON.stringify(state["rhmra.lease-state.v1"]) === originalLease,
    private_token_leaked: JSON.stringify({outcome, publicState}).includes(lease.run_lock_token),
  };
  process.stdout.write(JSON.stringify(output));
})().catch(error => {
  // Assertions identify harness contract failures without printing token-bearing
  // shell commands, state, or broker payloads.
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
});
