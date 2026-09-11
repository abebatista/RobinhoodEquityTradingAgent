async function runRsi({tools, allTools, load, store}) {
// Execute the read-only RSI continuation without returning between the request
// plan, broker responses, and final evaluator. Python owns every trading value.
const BOOTSTRAP_KEY = "rhmra.bootstrap-state.v1";
const STATE_KEY = "rhmra.transport-state.v1";
const LEASE_KEY = "rhmra.lease-state.v1";
const EXECUTION_KEY = "rhmra.rsi-execution.v1";
const plain = value => value !== null && typeof value === "object" &&
  !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
const textValue = value => typeof value === "string" && value.length > 0;
const sha256 = value => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
const exactKeys = (value, names) => plain(value) &&
  Object.keys(value).sort().join("\u0000") === [...names].sort().join("\u0000");
const sameJson = (left, right) => {
  if (left === right) return true;
  if (Array.isArray(left) && Array.isArray(right))
    return left.length === right.length && left.every((value, index) => sameJson(value, right[index]));
  if (!plain(left) || !plain(right)) return false;
  const keys = Object.keys(left);
  return keys.length === Object.keys(right).length && keys.every(key =>
    Object.prototype.hasOwnProperty.call(right, key) && sameJson(left[key], right[key]));
};
const textList = values => Array.isArray(values) && values.length > 0 &&
  values.every(value => textValue(value) && value === value.trim()) && new Set(values).size === values.length;
const purposeList = values => textList(values) && values.every(value => /^[a-z0-9][a-z0-9-]{0,47}$/.test(value));
let stage = "state-precondition";
let invocationId = null;
let state;
let bootstrap;
let scratch;
let projectRoot;
let pythonExe;
let constantsHash;
let startClock;
let launcher;
let quote;
let isWindows;
let fenceStarted = false;
let sourcesClosed = true;
const committedPurposes = [];
const committedSymbols = [];
const diagnostics = [];
class Failure extends Error {
  constructor(classification, reason, refreshAllowed) {
    super("RSI orchestration stopped");
    this.classification = classification;
    this.reason = reason;
    this.refreshAllowed = refreshAllowed;
    this.stage = stage;
  }
}
const halt = (classification = "coordination-halt", reason = "coordination-state", refreshAllowed = false) => {
  throw new Failure(classification, reason, refreshAllowed);
};
const requireActive = () => {
  const currentBootstrap = load(BOOTSTRAP_KEY);
  const current = load(STATE_KEY);
  const lease = load(LEASE_KEY);
  if (!plain(currentBootstrap) || currentBootstrap.schema_version !== 1 ||
      currentBootstrap.phase !== "context-bound" || !plain(currentBootstrap.context_receipt) ||
      currentBootstrap.context_receipt.invocation_id !== invocationId ||
      !plain(currentBootstrap.constants_receipt) || currentBootstrap.constants_receipt.source_sha256 !== constantsHash ||
      !plain(current) || current.schema_version !== 1 || current.phase !== "transport-bound" ||
      !plain(current.context_receipt) || current.context_receipt.invocation_id !== invocationId ||
      current.context_receipt.expected_gate_file !== state.context_receipt.expected_gate_file ||
      !plain(current.lease_binding) || current.lease_binding.invocation_id !== invocationId ||
      current.python_exe !== pythonExe || current.project_root !== projectRoot ||
      !plain(current.receipt) || current.receipt.scratch !== scratch ||
      current.receipt.scratch_id !== state.receipt.scratch_id ||
      current.receipt.source_root !== state.receipt.source_root ||
      current.receipt.source_root_id !== state.receipt.source_root_id ||
      !plain(lease) || lease.schema_version !== 1 || lease.phase !== "lease-owned" ||
      lease.invocation_id !== invocationId || !textValue(lease.run_lock_token))
    halt("coordination-halt", "coordination-state", false);
  return lease;
};
const saveState = fields => {
  requireActive();
  store(STATE_KEY, {...load(STATE_KEY), ...fields});
};
const runCommand = async command => {
  requireActive();
  // PowerShell otherwise maps a native exit 2 to process exit 1. Keep typed
  // helper failures unchanged without mutating any frozen evaluator command.
  const args = {cmd: isWindows ? command + "\nexit $LASTEXITCODE" : command,
    workdir: projectRoot, yield_time_ms: 30000, max_output_tokens: 24000};
  if (isWindows) args.shell = "powershell.exe";
  let process = await tools.exec_command(args);
  let output = String(process.output ?? "");
  while (process.session_id !== undefined) {
    const next = await tools.write_stdin({session_id: process.session_id, chars: "", yield_time_ms: 30000, max_output_tokens: 24000});
    output += String(next.output ?? "");
    process = next;
  }
  let receipt = null;
  try { receipt = JSON.parse(output); } catch (ignored) {}
  return Object.freeze({process: Object.freeze({...process, output}), receipt});
};
const sourceReceipt = (result, action, purpose) => {
  const receipt = result.receipt;
  if (result.process.exit_code !== 0 || !plain(receipt) || receipt.schema_version !== 1 ||
      receipt.action !== action || receipt.ok !== true || receipt.scratch !== scratch || receipt.purpose !== purpose)
    halt("snapshot-failure", "snapshot-write-failed", false);
  return receipt;
};
const reserve = async purpose => {
  stage = "source-reserve";
  // A lost reserve response can have left a pending journal entry.
  sourcesClosed = false;
  const result = await runCommand(launcher + " broker_snapshot.py reserve-source --scratch " + quote(scratch) + " --purpose " + quote(purpose));
  const receipt = sourceReceipt(result, "reserve-source", purpose);
  if (receipt.status !== "reserved" || receipt.idempotent !== false ||
      !textValue(receipt.reservation_id) || !textValue(receipt.source))
    halt("coordination-halt", "coordination-state", false);
  return receipt;
};
const abort = async reservation => {
  stage = "connector-abort";
  const result = await runCommand(launcher + " broker_snapshot.py abort-source --scratch " + quote(scratch) +
    " --purpose " + quote(reservation.purpose) + " --reservation-id " + quote(reservation.reservation_id) + " --reason connector-failed");
  const receipt = sourceReceipt(result, "abort-source", reservation.purpose);
  if (receipt.status !== "aborted" || receipt.reservation_id !== reservation.reservation_id ||
      receipt.reason !== "connector-failed" || receipt.source !== null)
    halt("coordination-halt", "coordination-state", false);
  sourcesClosed = true;
};
const persist = async (reservation, fullToolResult) => {
  stage = "source-write";
  try {
    const payload = JSON.stringify(fullToolResult);
    if (typeof payload !== "string" || payload[0] !== "{" || payload[payload.length - 1] !== "}" ||
        !plain(JSON.parse(payload))) throw new Error("invalid complete response");
    const patchTarget = reservation.source.replaceAll("\\", "/");
    const patch = "*** Begin Patch\n*** Add File: " + patchTarget + "\n+" +
      payload.replaceAll("\n", "\n+") + "\n*** End Patch";
    requireActive();
    const result = await tools.apply_patch(patch);
    if (result && result.isError === true) throw new Error("file change failed");
  } catch (ignored) { halt("snapshot-failure", "snapshot-write-failed", false); }
  stage = "source-commit";
  const result = await runCommand(launcher + " broker_snapshot.py commit-source --scratch " + quote(scratch) +
    " --purpose " + quote(reservation.purpose));
  const receipt = sourceReceipt(result, "commit-source", reservation.purpose);
  if (receipt.status !== "committed" || !textValue(receipt.reservation_id) ||
      !sha256(receipt.source_sha256) || !Number.isSafeInteger(receipt.source_size) || receipt.source_size < 0)
    halt("coordination-halt", "coordination-state", false);
  sourcesClosed = true;
  committedPurposes.push(reservation.purpose);
};
const bindFinal = (result, expectedParams, expectedSymbols, requests, unavailableSymbols) => {
  const receipt = result.receipt;
  if (result.process.exit_code !== 0 || !exactKeys(receipt, ["schema_version", "rsi_gate_enabled", "params", "results"]) ||
      receipt.schema_version !== 1 || receipt.rsi_gate_enabled !== true || !sameJson(receipt.params, expectedParams) ||
      !Array.isArray(receipt.results) || receipt.results.length !== expectedSymbols.length) return false;
  const seen = new Set();
  const requested = new Set(requests.map(request => request.symbol));
  for (const row of receipt.results) {
    if (!plain(row) || !textValue(row.symbol) || !expectedSymbols.includes(row.symbol) || seen.has(row.symbol) ||
        typeof row.buy_candidate !== "boolean" || typeof row.insufficient_history !== "boolean" ||
        (row.skip_reason !== null && typeof row.skip_reason !== "string") ||
        !["current_price", "median_dollar_volume", "recent_high", "pct_below_high", "spread_pct"].every(key =>
          row[key] === null || (typeof row[key] === "number" && Number.isFinite(row[key]))) ||
        (row.buy_candidate && (row.rsi_gate !== "pass" || row.insufficient_history !== false ||
          !requested.has(row.symbol) || unavailableSymbols.has(row.symbol))) ||
        (Object.prototype.hasOwnProperty.call(row, "rsi_gate") && !["pass", "block"].includes(row.rsi_gate)) ||
        (Object.prototype.hasOwnProperty.call(row, "rsi_series") && (!Array.isArray(row.rsi_series) ||
          row.rsi_series.some(value => typeof value !== "number" || !Number.isFinite(value))))) return false;
    seen.add(row.symbol);
  }
  return true;
};
try {
  state = load(STATE_KEY);
  bootstrap = load(BOOTSTRAP_KEY);
  invocationId = state && state.lease_binding && state.lease_binding.invocation_id;
  if (load(EXECUTION_KEY) !== undefined && load(EXECUTION_KEY) !== null) {
    stage = "execution-already-started";
    halt();
  }
  if (!plain(state) || state.schema_version !== 1 || state.phase !== "transport-bound" ||
      !plain(bootstrap) || bootstrap.schema_version !== 1 || bootstrap.phase !== "context-bound" ||
      !textValue(invocationId) || !textValue(state.python_exe) || !textValue(state.project_root) ||
      !plain(state.receipt) || !textValue(state.receipt.scratch) || !textValue(state.receipt.scratch_id) ||
      !textValue(state.receipt.source_root) || !textValue(state.receipt.source_root_id) ||
      !plain(bootstrap.constants_receipt) || !sha256(bootstrap.constants_receipt.source_sha256) ||
      !plain(bootstrap.start_clock) || bootstrap.start_clock.schema_version !== 1 ||
      typeof bootstrap.start_clock.date_et !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(bootstrap.start_clock.date_et) ||
      typeof bootstrap.start_clock.utc !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(bootstrap.start_clock.utc) ||
      !Number.isFinite(Date.parse(bootstrap.start_clock.utc)) ||
      bootstrap.start_clock.constants_sha256 !== bootstrap.constants_receipt.source_sha256 ||
      !plain(state.context_receipt) || !plain(bootstrap.context_receipt) ||
      state.context_receipt.expected_gate_file !== bootstrap.context_receipt.expected_gate_file ||
      typeof state.context_receipt.expected_gate_file !== "string" ||
      !/^rhmra-gates-\d{4}_\d{2}_\d{2}-\d{2}_\d{2}\.json$/.test(state.context_receipt.expected_gate_file) ||
      Object.prototype.hasOwnProperty.call(state, "final_gate_receipt")) halt();
  pythonExe = state.python_exe;
  projectRoot = state.project_root;
  scratch = state.receipt.scratch;
  constantsHash = bootstrap.constants_receipt.source_sha256;
  startClock = bootstrap.start_clock;
  isWindows = /^[A-Za-z]:[\\/]/.test(pythonExe);
  const psq = value => "'" + String(value).replaceAll("'", "''") + "'";
  const shq = value => "'" + String(value).replaceAll("'", "'\"'\"'") + "'";
  quote = isWindows ? psq : shq;
  launcher = (isWindows ? "& " : "") + quote(pythonExe);
  requireActive();
  if (typeof tools.exec_command !== "function" || typeof tools.write_stdin !== "function" || typeof tools.apply_patch !== "function") halt();
  store(EXECUTION_KEY, {schema_version: 1, invocation_id: invocationId, status: "running"});
  fenceStarted = true;
  const fence = load(EXECUTION_KEY);
  if (!plain(fence) || fence.schema_version !== 1 || fence.invocation_id !== invocationId || fence.status !== "running") halt();
  stage = "rsi-clock";
  const clockResult = await runCommand(launcher + " market_clock.py --json --expected-constants-sha256 " + quote(constantsHash));
  const clock = clockResult.receipt;
  if (clockResult.process.exit_code === 2 || (plain(clock) && clock.constants_sha256 !== constantsHash))
    halt("configuration-halt", "configuration-invalid", false);
  if (clockResult.process.exit_code !== 0 || !plain(clock) || clock.schema_version !== 1 ||
      clock.constants_sha256 !== constantsHash || clock.date_et !== startClock.date_et ||
      typeof clock.utc !== "string" || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(clock.utc) ||
      !Number.isFinite(Date.parse(clock.utc)) || Date.parse(clock.utc) < Date.parse(startClock.utc))
    halt("coordination-halt", "clock-unavailable", false);
  stage = "rsi-request-plan";
  const planResult = await runCommand(launcher + " rsi_orchestration.py --plan --scratch " + quote(scratch) +
    " --as-of-utc " + quote(clock.utc) + " --expected-constants-sha256 " + quote(constantsHash));
  const plan = planResult.receipt;
  if (planResult.process.exit_code !== 0) {
    const typed = planResult.process.exit_code === 2 && exactKeys(plan, ["schema_version", "action", "ok", "error"]) &&
      plan.schema_version === 1 && plan.action === "plan-rsi" && plan.ok === false && exactKeys(plan.error, ["code"]);
    if (typed && plan.error.code === "configuration_invalid") halt("configuration-halt", "configuration-invalid", false);
    if (typed && plan.error.code === "source_transport_invalid") halt("snapshot-failure", "snapshot-write-failed", false);
    const candidateFailure = typed && ["pre_rsi_input_failed", "pre_rsi_validation_failed", "rsi_clock_invalid",
      "rsi_request_range_unsupported", "rsi_plan_failed"].includes(plan.error.code);
    halt("coordination-halt", "coordination-state", candidateFailure);
  }
  if (!plain(plan) || plan.schema_version !== 1 || plan.action !== "plan-rsi" || plan.ok !== true)
    halt("coordination-halt", "coordination-state", false);
  if (plan.constants_sha256 !== constantsHash || plan.as_of_utc !== clock.utc || !plain(plan.params) ||
      !plain(plan.final_parameters) || !purposeList(plan.params.bars) || !purposeList(plan.params.quotes) ||
      !textList(plan.expected_symbols) || !sameJson(plan.params.expected_symbols, plan.expected_symbols) ||
      !Array.isArray(plan.requests) || !textValue(plan.start_time) || !textValue(plan.end_time) ||
      (Object.prototype.hasOwnProperty.call(state, "historicals_purposes") && !sameJson(state.historicals_purposes, plan.params.bars)) ||
      (Object.prototype.hasOwnProperty.call(state, "candidate_quotes_purposes") && !sameJson(state.candidate_quotes_purposes, plan.params.quotes)) ||
      (Object.prototype.hasOwnProperty.call(state, "expected_symbols") && !sameJson(state.expected_symbols, plan.expected_symbols)) ||
      (Object.prototype.hasOwnProperty.call(state, "pre_rsi_receipt") && (!plain(state.pre_rsi_receipt) ||
        state.pre_rsi_receipt.schema_version !== 1 || state.pre_rsi_receipt.rsi_gate_enabled !== false ||
        !sameJson(state.pre_rsi_receipt.params, plan.params)))) halt("coordination-halt", "coordination-state", true);
  const parameterFlags = Object.freeze([
    ["volume_lookback_days", "volume-lookback-days"], ["high_lookback_days", "high-lookback-days"],
    ["min_median_dollar_volume", "min-median-dollar-volume"], ["dip_entry_pct", "dip-entry-pct"],
    ["max_spread_buy_pct", "max-spread-buy-pct"], ["rsi_period", "rsi-period"],
    ["rsi_oversold", "rsi-oversold"], ["rsi_lookback_bars", "rsi-lookback-bars"],
    ["rsi_confirm_bars", "rsi-confirm-bars"], ["rsi_max_entry", "rsi-max-entry"],
    ["rsi_interval", "rsi-interval"], ["rsi_start_time", "rsi-start-time"], ["rsi_as_of_utc", "rsi-as-of-utc"]
  ]);
  if (!exactKeys(plan.final_parameters, parameterFlags.map(pair => pair[0])) ||
      parameterFlags.some(([key]) => !(typeof plan.final_parameters[key] === "number" && Number.isFinite(plan.final_parameters[key])) &&
        !textValue(plan.final_parameters[key])) || plan.final_parameters.rsi_as_of_utc !== clock.utc ||
      plan.final_parameters.rsi_start_time !== plan.start_time) halt("coordination-halt", "coordination-state", true);
  const requestedSymbols = new Set();
  for (let index = 0; index < plan.requests.length; index += 1) {
    const request = plan.requests[index];
    if (!exactKeys(request, ["symbol", "purpose", "arguments"]) || !plan.expected_symbols.includes(request.symbol) ||
        requestedSymbols.has(request.symbol) || request.purpose !== "rsi-" + index ||
        !exactKeys(request.arguments, ["symbol", "type", "interval", "period", "output", "start_time", "end_time", "bounds", "adjustment_type"]) ||
        request.arguments.symbol !== request.symbol || request.arguments.type !== "rsi" ||
        request.arguments.interval !== plan.final_parameters.rsi_interval || request.arguments.period !== plan.final_parameters.rsi_period ||
        request.arguments.start_time !== plan.start_time || request.arguments.end_time !== plan.end_time ||
        request.arguments.bounds !== "regular" || request.arguments.adjustment_type !== "split" || !textValue(request.arguments.output))
      halt("coordination-halt", "coordination-state", true);
    requestedSymbols.add(request.symbol);
  }
  saveState({rsi_plan_receipt: plan});
  stage = "rsi-tool-resolution";
  const toolName = "mcp__robinhood_mcp__get_equity_technical_indicators";
  const matches = Array.isArray(allTools) ? allTools.filter(tool => tool && tool.name === toolName) : [];
  if (matches.length > 1) halt("coordination-halt", "coordination-state", true);
  const readTool = matches.length === 1 && typeof tools[toolName] === "function" ? tools[toolName].bind(tools) : null;
  const unavailableSymbols = new Set();
  for (const request of plan.requests) {
    if (!readTool) {
      unavailableSymbols.add(request.symbol);
      diagnostics.push(Object.freeze({symbol: request.symbol, status: "indicator-unavailable"}));
      continue;
    }
    const reservation = await reserve(request.purpose);
    const argumentsObject = Object.freeze({...request.arguments});
    stage = "rsi-read";
    requireActive();
    let fullToolResult;
    let failed = false;
    try { fullToolResult = await readTool(argumentsObject); failed = !!(fullToolResult && fullToolResult.isError === true); }
    catch (ignored) { failed = true; }
    const firstFailed = failed;
    if (failed) {
      requireActive();
      try { fullToolResult = await readTool(argumentsObject); failed = !!(fullToolResult && fullToolResult.isError === true); }
      catch (ignored) { failed = true; }
    }
    if (failed) {
      await abort(reservation);
      unavailableSymbols.add(request.symbol);
      diagnostics.push(Object.freeze({symbol: request.symbol, status: "connector-exhausted"}));
      continue;
    }
    await persist(reservation, fullToolResult);
    committedSymbols.push(request.symbol);
    diagnostics.push(Object.freeze({symbol: request.symbol, status: firstFailed ? "recovered" : "committed"}));
  }
  const successfulCount = committedPurposes.length;
  if (successfulCount === 0) {
    const reservation = await reserve("rsi-empty");
    await persist(reservation, {});
  }
  stage = "final-evaluation";
  const separator = projectRoot.includes("\\") ? "\\" : "/";
  const gatePath = projectRoot + separator + "run-reports" + separator + state.context_receipt.expected_gate_file;
  const purposes = Object.freeze([...committedPurposes]);
  const requestSymbols = Object.freeze([...committedSymbols]);
  const grouped = (flag, values) => " --" + flag + " " + values.map(quote).join(" ");
  const commandParts = Object.freeze([
    launcher + " evaluate_candidates.py --scratch " + quote(scratch),
    grouped("bars-purpose", plan.params.bars), grouped("quotes-purpose", plan.params.quotes),
    grouped("expected-symbols", plan.expected_symbols), grouped("rsi-purpose", purposes),
    ...(requestSymbols.length ? [grouped("rsi-request-symbols", requestSymbols)] : []),
    ...parameterFlags.map(([key, flag]) => " --" + flag + " " + quote(plan.final_parameters[key])),
    " --json-out " + quote(gatePath) + " --emit-json"
  ]);
  const expectedParams = {...plan.params, ...plan.final_parameters, rsi_file: [...purposes], json_out: gatePath,
    ...(requestSymbols.length ? {rsi_request_symbols: [...requestSymbols]} : {})};
  const finalResult = await runCommand(commandParts.join(""));
  // Only the helper's typed semantic failure proves committed source transport
  // was validated. A lost, malformed, or unknown result cannot authorize refresh.
  if (finalResult.process.exit_code === 2 &&
      exactKeys(finalResult.receipt, ["schema_version", "action", "ok", "error"]) &&
      finalResult.receipt.schema_version === 1 && finalResult.receipt.action === "evaluate-candidates" &&
      finalResult.receipt.ok === false && exactKeys(finalResult.receipt.error, ["code"])) {
    const code = finalResult.receipt.error.code;
    if (code === "candidate_source_transport_failed")
      halt("snapshot-failure", "snapshot-write-failed", false);
    if (code === "candidate_evaluation_failed")
      halt("coordination-halt", "coordination-state", true);
    halt("coordination-halt", "coordination-state", false);
  }
  if (!bindFinal(finalResult, expectedParams, plan.expected_symbols, plan.requests, unavailableSymbols))
    halt("coordination-halt", "coordination-state", false);
  const compact = Object.freeze({schema_version: 1, action: "rsi-orchestration", ok: true,
    requested_count: plan.requests.length, committed_count: successfulCount,
    exhausted_count: diagnostics.filter(item => item.status === "connector-exhausted").length,
    candidate_count: finalResult.receipt.results.filter(row => row.buy_candidate === true).length,
    final_refresh_allowed: true, diagnostics: Object.freeze([...diagnostics])});
  saveState({final_gate_receipt: finalResult.receipt, rsi_purposes: purposes, rsi_orchestration: compact});
  store(EXECUTION_KEY, {schema_version: 1, invocation_id: invocationId, status: "finished"});
  return compact;
} catch (error) {
  const failure = error instanceof Failure ? error : new Failure("coordination-halt", "coordination-state", false);
  let active = false;
  if (fenceStarted) {
    try { requireActive(); active = true; } catch (ignored) {}
  }
  const compact = Object.freeze({schema_version: 1, action: "rsi-orchestration", ok: false,
    failure: failure.stage, classification: failure.classification, reason_code: failure.reason,
    final_refresh_allowed: Boolean(failure.refreshAllowed && sourcesClosed && active),
    diagnostics: Object.freeze([...diagnostics])});
  if (fenceStarted) {
    const current = load(STATE_KEY);
    if (plain(current) && plain(current.lease_binding) && current.lease_binding.invocation_id === invocationId)
      store(STATE_KEY, {...current, rsi_purposes: Object.freeze([...committedPurposes]), rsi_orchestration: compact});
    store(EXECUTION_KEY, {schema_version: 1, invocation_id: invocationId, status: "finished"});
  }
  return compact;
}
}
