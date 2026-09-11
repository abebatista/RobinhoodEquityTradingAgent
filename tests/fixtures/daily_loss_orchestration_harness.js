"use strict";

// Test-only capability adapter. No broker is contacted and mock mode writes no files.
const fs = require("fs");
const path = require("path");
const childProcess = require("child_process");
const crypto = require("crypto");
const input = JSON.parse(fs.readFileSync(0, "utf8"));
const config = input.config || {};
const source = fs.readFileSync(input.module, "utf8");
const runDailyLoss = new Function("return (" + source + "\n);")();
const HASH = "a".repeat(64);
const PREFIX = "mcp__robinhood_mcp__";
const BOOTSTRAP = "rhmra.bootstrap-state.v1";
const TRANSPORT = "rhmra.transport-state.v1";
const LEASE = "rhmra.lease-state.v1";
let scratch = input.integration ? input.integration.scratch : "C:/test/scratch";
let sourceRoot = input.integration ? input.integration.source_root : "C:/test/source";
const invocationId = "test-invocation";
const clock = {
  schema_version: 1, utc: "2026-09-11T18:33:00Z", date_et: "2026-09-11",
  date_pt: "2026-09-11", constants_sha256: HASH, session: "regular",
  entry_session_open: true, opening_blackout: false, calendar_status: "open",
  et: "2026-09-11 14:33:00 EDT", pt: "2026-09-11 11:33:00 PDT",
  pt_iso: "2026-09-11T11:33:00-07:00", historicals_start_time: "2026-08-07T18:33:00.000Z",
  regular_close_et: "16:00", minutes_since_open: 303
};
const state = {
  [BOOTSTRAP]: {
    schema_version: 1, phase: "context-bound",
    context_receipt: {invocation_id: invocationId}, start_clock: clock,
    constants_receipt: {source_sha256: HASH, values: {DAILY_LOSS_HALT_PCT: "3", STOP_COUNT_HALT: 3}}
  },
  [TRANSPORT]: {
    schema_version: 1, phase: "transport-bound", python_exe: "C:/test/python.exe",
    project_root: "C:/test/project", lease_binding: {invocation_id: invocationId},
    context_receipt: {invocation_id: invocationId}, account_number: "test-account",
    agentic_allowed: true,
    receipt: {scratch, scratch_id: "scratch-id", source_root: sourceRoot, source_root_id: "source-id"},
    second_entry_receipt: {
      schema_version: 1, action: "enter-second", ok: true, invocation_id: invocationId,
      phase: "daily-loss", recorded: true, event_sequence: 7, entry_eligible: true,
      daily_loss_attempted: true, scratch_id: "scratch-id", source_root_id: "source-id",
      constants_sha256: HASH, daily_loss_halt_pct: "3", stop_count_halt: 3, lease_renewed: true
    }
  },
  [LEASE]: {schema_version: 1, phase: "lease-owned", invocation_id: invocationId, run_lock_token: "test-private-token"}
};
if (input.state) Object.assign(state, input.state);
if (config.statePatch) {
  for (const [key, value] of Object.entries(config.statePatch)) state[key] = value;
}
const originalSecond = state[TRANSPORT] && state[TRANSPORT].second_entry_receipt;
const initialState = JSON.parse(JSON.stringify(state));
if (config.secondCell) delete state[TRANSPORT].second_entry_receipt;
const log = [];
const reservations = new Map();
const writes = new Map();
const stageOutputs = new Map();
const returned = new Map();
const counters = new Map();
let pending = null;
let sequence = 0;
let session = null;

function count(key) {
  const next = (counters.get(key) || 0) + 1;
  counters.set(key, next);
  return next;
}
function parseCommand(command) {
  // Model the explicit PowerShell exit trailer; it is shell syntax, not a
  // Python argument. Real-shell integration separately verifies its effect.
  command = command.replace(/\r?\nexit \$LASTEXITCODE$/, "");
  const tokens = (command.match(/'(?:''|[^'])*'|"(?:[^"\\]|\\.)*"|[^\s]+/g) || []).map(token => {
    if (token.startsWith("'")) return token.slice(1, -1).replaceAll("''", "'");
    if (token.startsWith('"')) return JSON.parse(token);
    return token;
  });
  if (tokens[0] === "&") tokens.shift();
  const python = tokens.shift();
  const script = tokens.shift();
  const args = tokens;
  const options = {};
  let current = null;
  for (const value of args) {
    if (value.startsWith("--")) { current = value.slice(2); options[current] ||= []; }
    else if (current) options[current].push(value);
  }
  return {python, script, action: args[0] && !args[0].startsWith("--") ? args[0] : null, args, options};
}
function one(options, name) { return options[name] && options[name][0]; }
function purposeParts(purpose) {
  const match = /^daily-loss-([ab])-(discovery|final|marks)-(positions|orders|portfolio|quotes)-(\d+)$/.exec(purpose);
  if (!match) throw new Error("noncanonical source purpose: " + purpose);
  return {generation: match[1].toUpperCase(), phase: match[2], kind: match[3], index: Number(match[4])};
}
function success(action, extras = {}) { return {schema_version: 1, action, ok: true, ...extras}; }
function processReceipt(receipt, exitCode = 0) { return {exit_code: exitCode, output: JSON.stringify(receipt)}; }
function failureAt(point, details, defaults) {
  const nth = count("failure:" + point + ":" + (details.generation || "") + ":" + (details.kind || ""));
  const match = (config.failures || []).find(item => item.when === point &&
    (!item.generation || item.generation === details.generation) &&
    (!item.kind || item.kind === details.kind) && (!item.at || item.at === nth));
  if (!match) return null;
  if (match.throw) throw new Error("injected " + point + " uncertainty");
  if (match.malformed) return {exit_code: match.exitCode ?? 0, output: "malformed local receipt"};
  const receipt = {schema_version: 1, action: defaults.action, ok: false,
    ...defaults.fields, error: {code: match.code, message: "injected fixture failure"}};
  if (match.code === "stage_semantic_invalid" || match.code === "daily_loss_semantic_invalid") {
    receipt.recovery_action = details.generation === "A" ? "generation-b" : "snapshot-second-attempt-failed";
  }
  if (match.extra) receipt.untrusted_extra = true;
  if (match.wrongGeneration) receipt.generation = match.wrongGeneration;
  return processReceipt(receipt, match.exitCode ?? 2);
}

async function execute(commandArguments) {
  const command = parseCommand(commandArguments.cmd);
  const {script, action, options} = command;
  const event = {type: "command", command: commandArguments.cmd, script, action, args: command.args, options};
  log.push(event);
  if (input.integration) {
    const allowed = ["broker_snapshot.py", "connector_contract.py", "market_clock.py", "daily_loss.py", "run_lock.py", "run_lifecycle.py"];
    if (!allowed.includes(script)) throw new Error("unexpected real helper " + script);
    if (commandArguments.workdir !== input.integration.root) throw new Error("wrong integration working directory");
    const extra = [];
    const files = input.integration;
    if (script === "daily_loss.py" && options["json-out"]) {
      extra.push("--lifecycle-state-file", files.lifecycle, "--lifecycle-projection-file", files.projection,
        "--lifecycle-context-file", files.context, "--lifecycle-lock-file", files.lock);
    } else if (script === "run_lifecycle.py") {
      extra.push("--state-file", files.lifecycle, "--projection-file", files.projection,
        "--context-file", files.context, "--lock-file", files.lock);
    } else if (script === "run_lock.py") extra.push("--lock-file", files.lock);
    const executed = childProcess.spawnSync(files.python, [path.join(files.root, script), ...command.args, ...extra], {
      cwd: files.root, encoding: "utf8", timeout: 30000, windowsHide: true
    });
    if (executed.error) throw executed.error;
    let receipt;
    try { receipt = JSON.parse(executed.stdout); } catch {}
    event.exit_code = executed.status;
    event.receipt = receipt || executed.stdout + executed.stderr;
    if (action === "reserve-source" && receipt && receipt.ok) {
      pending = {...receipt, ...purposeParts(receipt.purpose)};
      reservations.set(receipt.purpose, pending);
    }
    if (action === "commit-source" && receipt && receipt.ok) pending = null;
    return {exit_code: executed.status, output: executed.stdout + executed.stderr};
  }
  const generation = one(options, "generation") || one(options, "snapshot-generation");
  const kind = one(options, "kind");
  if (script === "daily_loss_orchestration.py") {
    if (config.loaderFailure) return processReceipt({schema_version: 1,
      action: "load-daily-loss-orchestration", ok: false, error: {code: "driver_source_unavailable"}}, 2);
    const loadedSource = config.loaderInvalidJs ? "async function (" : source;
    return processReceipt(success("load-daily-loss-orchestration", {
      source: loadedSource, source_sha256: crypto.createHash("sha256").update(loadedSource).digest("hex")}));
  }
  if (script === "run_performance.py" && action === "record-internal") {
    if (config.telemetryFault === "throw") throw new Error("mock telemetry call failure");
    if (config.telemetryFault === "nonzero") return {exit_code: 2, output: "mock telemetry failure"};
    if (config.telemetryFault === "malformed") return {exit_code: 0, output: "not-json"};
    return processReceipt(success(action, {
      invocation_id: config.telemetryFault === "wrong-invocation" ? "wrong" : state[BOOTSTRAP].context_receipt.invocation_id,
      estimated_run_start_pt: state[BOOTSTRAP].context_receipt.run_start_pt,
      estimated_run_end_pt: "2026-09-11T11:43:00-07:00", estimated_run_total_ms: 600000,
      estimated_run_total_display: "10m00", estimate_clock_source: "final-summary-boundary"
    }));
  }
  if (script === "run_lifecycle.py" && action === "enter-second") return processReceipt(originalSecond);
  if (script === "broker_snapshot.py") {
    if (action === "reserve-source") {
      if (pending) throw new Error("reservation while preceding source is pending");
      const purpose = one(options, "purpose");
      if (reservations.has(purpose)) throw new Error("duplicate purpose");
      const parts = purposeParts(purpose);
      if (parts.generation === "B" && !counters.has("authorized-b")) throw new Error("B before authorization");
      pending = {purpose, ...parts, source: sourceRoot + "/source-" + (++sequence) + ".json", reservation_id: "reservation-" + sequence};
      reservations.set(purpose, pending);
      return processReceipt(success(action, {scratch, ...pending, status: "reserved", idempotent: false}));
    }
    if (action === "abort-source") {
      const entry = reservations.get(one(options, "purpose"));
      if (!entry || one(options, "reservation-id") !== entry.reservation_id || writes.has(entry.source)) throw new Error("unsafe abort");
      pending = null;
      return processReceipt(success(action, {scratch, purpose: entry.purpose, reservation_id: entry.reservation_id,
        status: "aborted", reason: one(options, "reason"), source: null}));
    }
    if (action === "commit-source") {
      if (options["reservation-id"]) throw new Error("unattended commit must self-correlate");
      const entry = reservations.get(one(options, "purpose"));
      if (!entry || pending !== entry || !writes.has(entry.source)) throw new Error("commit before exact write");
      if (writes.get(entry.source) !== JSON.stringify(returned.get(entry.purpose))) throw new Error("changed or partial broker serialization");
      pending = null;
      const fault = failureAt("commit", entry, {action});
      if (fault) return fault;
      return processReceipt(success(action, {scratch, purpose: entry.purpose, reservation_id: entry.reservation_id,
        status: "committed", source_sha256: HASH, source_size: writes.get(entry.source).length}));
    }
    if (action === "stage") {
      if (pending) throw new Error("stage while source is uncertain");
      const fault = failureAt("stage", {kind, generation}, {action, fields: {kind, generation}});
      if (fault) return fault;
      const purposes = options["source-purpose"] || [];
      const priorPaths = options.source || [];
      const sources = purposes.length ? purposes.map(purpose => reservations.get(purpose)) : priorPaths.map(file => stageOutputs.get(file));
      if (!sources.length || sources.some(item => !item)) throw new Error("unbound stage input");
      if ((kind === "portfolio" || kind === "quotes") && (options["request-cursor"] || options["allow-more"])) throw new Error("cursor flags on nonpaginated kind");
      const files = sources.map((item, index) => {
        const output = scratch + "/stage-" + (++sequence) + ".json";
        stageOutputs.set(output, item);
        return {index: index + 1, output, transport: "file-change", source_sha256: HASH,
          payload_sha256: HASH, provenance: output + ".meta.json", source_purpose: item.purpose,
          row_count: item.row_count || 0, next_cursor: item.next_cursor ?? null,
          request_cursor: item.request_cursor || "FIRST"};
      });
      return processReceipt(success(action, {output_mode: "helper-allocated", kind, generation,
        set_id: "stage-set-" + sequence, complete: !options["allow-more"], file_count: files.length,
        files, output_paths: files.map(item => item.output)}));
    }
    if (action === "authorize-generation-b") {
      count("authorized-b");
      return processReceipt(success(action, {scratch, state: "generation-b-authorized", reason: "daily-loss-semantic-invalid", idempotent: false}));
    }
    if (action === "finish-generation-b") return processReceipt(success(action, {scratch, state: "generation-b-exhausted", outcome: one(options, "outcome"), idempotent: false}));
  }
  if (script === "connector_contract.py" && action === "page") {
    const entry = reservations.get(one(options, "source-purpose"));
    if (pending || !entry || !writes.has(entry.source)) throw new Error("page before committed source");
    const fault = failureAt("page", entry, {action});
    if (fault) return fault;
    const chain = options["request-cursor"] || [];
    const current = chain[chain.length - 1];
    if (current !== entry.request_cursor || chain[0] !== "FIRST") throw new Error("wrong page cursor chain");
    return processReceipt(success(action, {kind, source_purpose: entry.purpose, request_cursor: current,
      request_cursors: chain, row_count: entry.row_count || 0, next_cursor: entry.next_cursor,
      complete: entry.next_cursor === null}));
  }
  if (script === "market_clock.py") {
    const number = count("clock");
    return processReceipt({...state[BOOTSTRAP].start_clock, utc: "2026-09-11T18:34:" + String(number).padStart(2, "0") + "Z"});
  }
  if (script === "daily_loss.py") {
    const mode = options["symbols-out"] ? "discovery" : "calculation";
    const fault = failureAt(mode, {generation}, {action: "daily-loss", fields: {mode, generation}});
    if (fault) return fault;
    for (const name of ["positions", "orders", "portfolio", "quotes"]) {
      for (const file of options[name] || []) if (!stageOutputs.has(file)) throw new Error("unsealed calculation input");
    }
    if (mode === "discovery") {
      if (options.portfolio || options.quotes || options["halt-pct"] || options["run-token"]) throw new Error("calculation-only discovery option");
      const symbols = config.symbols || [];
      return processReceipt(success("discover-symbols", {trading_date_et: one(options, "trading-date"),
        as_of_utc: one(options, "as-of-utc"), symbol_count: symbols.length, symbols}));
    }
    if (options["halt-pct"] || one(options, "run-token") !== state[LEASE].run_lock_token) throw new Error("wrong calculation authority");
    const expectedGenerationPhase = file => stageOutputs.get(file).generation === generation;
    for (const name of ["positions", "orders", "portfolio"]) {
      if (!(options[name] || []).every(file => expectedGenerationPhase(file) && stageOutputs.get(file).phase === "final")) throw new Error("calculation reused discovery or wrong generation");
    }
    const result = {schema_version: 1, trading_date_et: one(options, "trading-date"), as_of_utc: one(options, "as-of-utc"),
      stop_count_date_pt: one(options, "stop-date-pt"), status: config.dailyLossTripped ? "tripped" : "clear",
      halt_new_buys: Boolean(config.dailyLossTripped), stop_fills_today: config.stopCountTripped ? 3 : 0,
      stopped_out_symbols: config.stopCountTripped ? ["TEST"] : [], daily_pnl: "0", loss_pct_of_total: "0",
      helper_owned_evidence: {retain: "unchanged"}};
    const receipt = success("daily-loss", {mode, generation, invocation_id: state[LEASE].invocation_id,
      constants_sha256: state[BOOTSTRAP].constants_receipt.source_sha256,
      daily_loss_halt_pct: state[BOOTSTRAP].constants_receipt.values.DAILY_LOSS_HALT_PCT,
      stop_count_halt: state[BOOTSTRAP].constants_receipt.values.STOP_COUNT_HALT,
      daily_loss_tripped: Boolean(config.dailyLossTripped), stop_count_tripped: Boolean(config.stopCountTripped),
      entry_guard_outcome: config.dailyLossTripped || config.stopCountTripped ? "tripped" : "clear", result});
    event.receipt = receipt;
    return processReceipt(receipt);
  }
  if (script === "run_lock.py" && action === "renew") return processReceipt(success(action, {
    token: state[LEASE].run_lock_token, renewed_at: "2098-12-31T23:40:00Z",
    expires_at: "2099-01-01T00:00:00Z", lease_seconds: 1200}));
  if (script === "run_lifecycle.py" && action === "complete-second") return processReceipt(success(action, {
    invocation_id: state[LEASE].invocation_id, phase: "daily-loss", outcome: one(options, "outcome"),
    recorded: true, sequence: 100, lease_renewed: true}));
  throw new Error("Unexpected command in test harness: " + commandArguments.cmd);
}

const tools = {
  exec_command: async args => {
    const value = await execute(args);
    if (config.chunked && !session) {
      const split = Math.floor(value.output.length / 2);
      session = {exit_code: value.exit_code, output: value.output.slice(split)};
      return {session_id: 123, output: value.output.slice(0, split)};
    }
    return value;
  },
  write_stdin: async args => {
    log.push({type: "drain", session_id: args.session_id});
    if (args.session_id !== 123 || !session) throw new Error("unexpected drain");
    const value = session;
    session = null;
    return value;
  },
  apply_patch: async patch => {
    log.push({type: "patch", purpose: pending && pending.purpose});
    if (!pending) throw new Error("patch without reserved source");
    const match = /^\*\*\* Begin Patch\r?\n\*\*\* Add File: ([^\r\n]+)\r?\n([\s\S]*?)\r?\n\*\*\* End Patch\s*$/.exec(patch);
    if (!match || match[1] !== pending.source.replaceAll("\\", "/")) throw new Error("patch did not use exact reserved source");
    const raw = match[2].split(/\r?\n/).map(line => {
      if (!line.startsWith("+")) throw new Error("invalid patch line");
      return line.slice(1);
    }).join("\n");
    if (config.patchFailure === "throw") throw new Error("injected file writer failure");
    if (config.patchFailure === "error") return {isError: true, content: [{type: "text", text: "denied"}]};
    writes.set(pending.source, raw);
    if (input.integration) {
      if (!path.resolve(match[1]).startsWith(path.resolve(sourceRoot) + path.sep)) throw new Error("integration patch outside fixture root");
      fs.writeFileSync(match[1], raw, {flag: "wx"});
    }
    if (config.patchFailure === "write-throw") throw new Error("injected uncertain file writer result");
    return {content: [{type: "text", text: "Success. Updated file"}]};
  }
};
const names = ["get_equity_positions", "get_equity_orders", "get_equity_quotes", "get_portfolio"];
for (const name of names) tools[PREFIX + name] = async args => {
  if (!pending) throw new Error("broker call before source reservation");
  const entry = pending;
  log.push({type: "broker", name, purpose: entry.purpose, args});
  const key = entry.generation + ":" + entry.phase + ":" + entry.kind;
  const nth = count("broker:" + key);
  if ((config.readFailures || {})[key] >= nth) {
    if (config.readThrows) throw new Error("explicit mock connector error");
    return {isError: true, content: [{type: "text", text: "mock connector failed"}]};
  }
  const current = args.cursor || "FIRST";
  const cursorIndex = current === "FIRST" ? 0 : Number(current.match(/(\d+)$/)[1]);
  const pageCount = (config.pages || {})[entry.phase + ":" + entry.kind] || 1;
  entry.request_cursor = current;
  entry.next_cursor = cursorIndex + 1 < pageCount ? entry.phase + "-" + entry.kind + "-cursor-" + (cursorIndex + 1) : null;
  entry.row_count = 0;
  let data;
  if (name === "get_equity_positions") data = {positions: [], next: entry.next_cursor ? "https://example.invalid/positions?cursor=" + entry.next_cursor : null};
  else if (name === "get_equity_orders") data = {orders: [], next: entry.next_cursor ? "https://example.invalid/orders?cursor=" + entry.next_cursor : null};
  else if (name === "get_portfolio") data = {total_value: "1000", cash: "1000", equity_value: "0", buying_power: {buying_power: "1000"}};
  else data = {results: (args.symbols || []).map(symbol => ({quote: {symbol}, close: {symbol}}))};
  const result = {isError: false, content: [{type: "text", text: JSON.stringify({data})}],
    structuredContent: {data}, guide: {must_preserve: [1, "full envelope", {nested: true}]}};
  returned.set(entry.purpose, result);
  return result;
};
const allTools = names.filter(name => name !== config.missingTool).map(name => ({name: PREFIX + name, description: "Mock broker read"}));
if (config.duplicateTool) allTools.push({...allTools[0]});
if (config.missingCallable) delete tools[PREFIX + config.missingCallable];

(async () => {
  let result;
  let thrown = null;
  const store = (key, value) => {state[key] = value; log.push({type: "store", key, value});};
  try {
    if (config.secondCell) {
      const routine = fs.readFileSync(input.routine, "utf8");
      const cell = routine.split("**CODEX ENTER SECOND — EXACT OWNER-FENCED CELL:**")[1].split("```javascript")[1].split("```")[0];
      const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
      const executeCell = new AsyncFunction("tools", "ALL_TOOLS", "load", "store", "text", "exit", cell);
      const exited = {};
      try {
        await executeCell(tools, allTools, key => state[key], store, value => {
          result = JSON.parse(value); log.push({type: "text", value: result});
        }, () => {throw exited;});
      } catch (error) { if (error !== exited) throw error; }
    } else result = await runDailyLoss({tools, allTools, load: key => state[key], store});
  }
  catch (error) { thrown = String(error.stack || error); }
  let secondResult;
  let secondThrown = null;
  const firstLogLength = log.length;
  if (config.rerun) {
    try { secondResult = await runDailyLoss({tools, allTools, load: key => state[key], store: (key, value) => {state[key] = value; log.push({type: "store", key, value});}}); }
    catch (error) { secondThrown = String(error.stack || error); }
  }
  let telemetryResult;
  let telemetryThrown = null;
  let afterTelemetryState;
  if (config.telemetryAfter) {
    state["rhmra.rsi-execution.v1"] = {schema_version: 1,
      invocation_id: state[LEASE].invocation_id, status: "finished"};
    const context = {...state[BOOTSTRAP].context_receipt, expected_report_file: "rhmra-log-2026_09_11-11_33.md",
      expected_status_file: "rhmra-status-2026_09_11-11_33.json", run_start_pt: clock.pt_iso};
    state[BOOTSTRAP] = {...state[BOOTSTRAP], context_receipt: context, resolver_receipt: {python: state[TRANSPORT].python_exe}};
    state[TRANSPORT] = {...state[TRANSPORT], phase: config.publishedStatus ? "status-published" : "status-unavailable",
      context_receipt: context, report_binding: {expected_report_file: context.expected_report_file, persisted: true, read_back: true}};
    if (config.publishedStatus) state[TRANSPORT].status_binding = {status_file: context.expected_status_file,
      byte_count: 123, sha256: HASH, persisted: true, read_back: true};
    if (!config.telemetryInvalidTerminal) state[LEASE] = {schema_version: 1,
      phase: config.terminalLeasePhase || "lease-released", invocation_id: context.invocation_id};
    const routine = fs.readFileSync(input.routine, "utf8");
    const cell = routine.split("**CODEX FINAL TELEMETRY + REPORT POINTER — EXACT:**")[1].split("```javascript")[1].split("```")[0]
      .replace('const session = "<unchanged START CLOCK session>";', 'const session = "regular";');
    const AsyncFunction = Object.getPrototypeOf(async function() {}).constructor;
    const executeCell = new AsyncFunction("tools", "load", "store", "text", cell);
    try {
      await executeCell(tools, key => state[key], store, value => {
        telemetryResult = JSON.parse(value); log.push({type: "text", value: telemetryResult});
      });
    } catch (error) { telemetryThrown = String(error.stack || error); }
    afterTelemetryState = JSON.parse(JSON.stringify(state));
  }
  let nextResult;
  let nextThrown = null;
  const nextLogStart = log.length;
  if (config.nextInvocation) {
    const freshState = JSON.parse(JSON.stringify(initialState));
    const nextId = "next-test-invocation";
    scratch = "C:/test/next-scratch";
    sourceRoot = "C:/test/next-source";
    freshState[BOOTSTRAP].context_receipt.invocation_id = nextId;
    freshState[TRANSPORT].context_receipt.invocation_id = nextId;
    freshState[TRANSPORT].lease_binding.invocation_id = nextId;
    freshState[TRANSPORT].receipt = {scratch, source_root: sourceRoot, scratch_id: "next-scratch-id", source_root_id: "next-source-id"};
    freshState[TRANSPORT].second_entry_receipt = {...freshState[TRANSPORT].second_entry_receipt,
      invocation_id: nextId, scratch_id: "next-scratch-id", source_root_id: "next-source-id"};
    freshState[LEASE].invocation_id = nextId;
    Object.assign(state, freshState);
    reservations.clear(); writes.clear(); stageOutputs.clear(); returned.clear(); counters.clear();
    pending = null; sequence = 0;
    try { nextResult = await runDailyLoss({tools, allTools, load: key => state[key], store}); }
    catch (error) { nextThrown = String(error.stack || error); }
  }
  process.stdout.write(JSON.stringify({result, thrown, secondResult, secondThrown, firstLogLength,
    telemetryResult, telemetryThrown, afterTelemetryState, nextResult, nextThrown, nextLogStart,
    log, state, writes: [...writes.entries()], pending: pending && pending.purpose}));
})().catch(error => {process.stderr.write(String(error.stack || error)); process.exitCode = 1;});
