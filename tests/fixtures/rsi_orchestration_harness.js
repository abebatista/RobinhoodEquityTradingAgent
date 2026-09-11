"use strict";

// No live connector is imported. Only synthetic RSI reads and real copied Python
// helpers are available to the production continuation loaded below.
const fs = require("node:fs");
const path = require("node:path");
const {spawnSync} = require("node:child_process");
const assert = require("node:assert/strict");

(async () => {
  const input = JSON.parse(fs.readFileSync(0, "utf8"));
  const state = input.state;
  const config = input.config || {};
  const transport = state["rhmra.transport-state.v1"];
  const sourceRoot = fs.realpathSync(transport.receipt.source_root);
  const root = fs.realpathSync(transport.project_root);
  const identity = fs.statSync(sourceRoot);
  const leaseKey = "rhmra.lease-state.v1";
  const originalLease = JSON.stringify(state[leaseKey]);
  const privateToken = state[leaseKey].run_lock_token;
  const events = [];
  const calls = {};
  const saved = new Set();
  let tampered = false;
  const allowedScripts = new Set([
    "rsi_orchestration.py", "evaluate_candidates.py", "broker_snapshot.py",
    "run_lock.py", "market_clock.py", "run_lifecycle.py",
  ]);
  const api = {
    write_stdin: async () => {throw new Error("synchronous test has no session");},
    exec_command: async args => {
      assert.equal(fs.realpathSync(args.workdir), root);
      const script = args.cmd.match(/\b([a-z_]+\.py)\b/);
      assert.ok(script && allowedScripts.has(script[1]), "unexpected helper");
      const action = args.cmd.slice(script.index + script[0].length).trim().split(/\s+/)[0];
      const event = {type: "helper", script: script[1], action,
        command: args.cmd.replaceAll(privateToken, "<private lease>")};
      events.push(event);
      if (config.tamperBeforeFinal && script[1] === "evaluate_candidates.py" && !tampered) {
        const target = path.resolve(config.tamperBeforeFinal);
        assert.equal(path.dirname(target), sourceRoot);
        assert.equal(fs.realpathSync(target), target);
        // Change an already committed test-owned source after the plan and RSI
        // commit, while keeping valid JSON. The real evaluator must catch its
        // broken sealed identity before treating it as a candidate data issue.
        fs.appendFileSync(target, "\n ");
        events.push({type: "tamper"});
        tampered = true;
      }
      if (config.abortFailure && action === "abort-source") {
        return {exit_code: 2, output: JSON.stringify({schema_version: 1,
          action: "abort-source", ok: false, error: {code: "source_journal_busy", message: "synthetic abort failure"}})};
      }
      const windows = process.platform === "win32";
      if (windows) assert.equal(args.shell, "powershell.exe");
      const child = spawnSync(windows ? "powershell.exe" : "/bin/sh",
        windows ? ["-NoProfile", "-NonInteractive", "-Command", args.cmd] : ["-c", args.cmd],
        {cwd: root, encoding: "utf8", timeout: 60000, maxBuffer: 2 * 1024 * 1024});
      assert.ifError(child.error);
      const output = child.stdout + child.stderr;
      event.exit_code = child.status;
      try {
        const receipt = JSON.parse(output);
        if (["broker_snapshot.py", "rsi_orchestration.py", "market_clock.py", "evaluate_candidates.py"].includes(script[1])) {
          if (action === "--emit-javascript") {
            const {source: ignored, ...metadata} = receipt;
            event.receipt = metadata;
          } else event.receipt = receipt;
        }
      } catch {}
      if (child.status !== 0) event.diagnostic = output.replaceAll(privateToken, "<private lease>");
      if (script[1] === "evaluate_candidates.py" && config.finalReplyFailure) {
        event.reply_failure = config.finalReplyFailure;
        if (config.finalReplyFailure === "throw-after-success") throw new Error("synthetic lost evaluator receipt");
        if (config.finalReplyFailure === "malformed-success") return {exit_code: 0, output: "not JSON"};
        if (config.finalReplyFailure === "unknown-nonzero") return {exit_code: 2, output: "{}"};
        throw new Error("unknown test reply failure");
      }
      return {exit_code: child.status, output};
    },
    apply_patch: async patch => {
      const lines = patch.split("\n");
      assert.equal(lines.shift(), "*** Begin Patch");
      const header = lines.shift();
      assert.ok(header.startsWith("*** Add File: "));
      const target = path.resolve(header.slice("*** Add File: ".length));
      assert.equal(path.dirname(target), sourceRoot);
      const currentIdentity = fs.statSync(sourceRoot);
      assert.equal(currentIdentity.dev, identity.dev);
      assert.equal(currentIdentity.ino, identity.ino);
      while (lines.at(-1) === "") lines.pop();
      assert.equal(lines.pop(), "*** End Patch");
      assert.ok(lines.every(line => line.startsWith("+")));
      assert.ok(!saved.has(target), "response source must be written once");
      const content = lines.map(line => line.slice(1)).join("\n") + "\n";
      const parsed = JSON.parse(content);
      events.push({type: "write", payload: parsed});
      if (config.patchFailure === "before-write") throw new Error("synthetic write failure");
      fs.writeFileSync(target, content, {flag: "wx"});
      saved.add(target);
      if (config.patchFailure === "after-write") throw new Error("synthetic lost write receipt");
      return "Success. Updated the following files.";
    },
  };
  const name = "mcp__robinhood_mcp__get_equity_technical_indicators";
  api[name] = async args => {
    assert.equal(typeof args.symbol, "string");
    const attempt = (calls[args.symbol] || 0) + 1;
    calls[args.symbol] = attempt;
    events.push({type: "broker", name, args: JSON.parse(JSON.stringify(args)), attempt});
    if (attempt <= (config.readFailures || {})[args.symbol]) {
      if (config.throwErrors) throw new Error("synthetic connector outage");
      return {isError: true, content: [{type: "text", text: "synthetic connector outage"}]};
    }
    const values = config.malformedSuccess ? ["not-a-number"] : [40, 36, 33, 28, 30, 34];
    const constants = state["rhmra.bootstrap-state.v1"].constants_receipt.values;
    const cutoff = Date.parse(args.end_time || state["rhmra.bootstrap-state.v1"].start_clock.utc);
    const seriesStart = cutoff - values.length * 30 * 60 * 1000;
    const responseSymbol = (config.responseSymbols || {})[args.symbol] || args.symbol;
    const result = {data: {symbol: responseSymbol, interval: constants.RSI_INTERVAL, bounds: "regular",
      indicators: [{type: "rsi", params: {period: constants.RSI_PERIOD},
        series: values.map((value, index) => ({begins_at: new Date(seriesStart + index * 30 * 60 * 1000).toISOString(), value}))}]}};
    const envelope = {content: [{type: "text", text: "synthetic RSI response"}],
      structuredContent: result, isError: false};
    events.push({type: "broker-response", payload: envelope});
    return envelope;
  };
  const allTools = config.missingTool ? [] : [{name, description: "Synthetic RSI only"}];
  if (config.duplicateTool) allTools.push({name, description: "Duplicate synthetic RSI"});
  const tools = new Proxy(api, {get(target, key) {
    assert.ok(Object.hasOwn(target, key), "forbidden tool: " + String(key));
    return target[key];
  }});
  const load = key => state[key];
  const store = (key, value) => {state[key] = value;};
  let outcome;
  if (typeof input.cell === "string") {
    const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
    const executeCell = new AsyncFunction("tools", "ALL_TOOLS", "load", "store", "text", "exit", input.cell);
    const exitSignal = Symbol("cell exit");
    const emit = value => {
      outcome = JSON.parse(value);
      events.push({type: "emit", value: outcome,
        final_gate_bound: !!state["rhmra.transport-state.v1"].final_gate_receipt});
    };
    try {
      await executeCell(tools, allTools, load, store, emit, () => {throw exitSignal;});
    } catch (error) {if (error !== exitSignal) throw error;}
  } else {
    const source = fs.readFileSync(process.argv[2], "utf8");
    const runRsi = new Function('"use strict"; return (' + source + '\n);')();
    outcome = await runRsi({tools, allTools, load, store});
  }
  const publicState = Object.fromEntries(Object.entries(state).filter(([key]) => key !== leaseKey));
  const output = {outcome, events, public_state: publicState,
    private_lease_unchanged: JSON.stringify(state[leaseKey]) === originalLease,
    private_token_leaked: JSON.stringify({outcome, events, publicState}).includes(privateToken)};
  process.stdout.write(JSON.stringify(output));
})().catch(error => {
  process.stderr.write(error.stack || String(error));
  process.exitCode = 1;
});
