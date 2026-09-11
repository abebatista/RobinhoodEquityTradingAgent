async function runDailyLoss({tools, allTools, load, store}) {
// Checked-in sequencing only. Python helpers own broker semantics, money, clocks,
// provenance, generation authorization, and the owner-fenced SECOND checkpoint.
const BOOTSTRAP_KEY = "rhmra.bootstrap-state.v1";
const STATE_KEY = "rhmra.transport-state.v1";
const LEASE_KEY = "rhmra.lease-state.v1";
const EXECUTION_KEY = "rhmra.daily-loss-execution.v1";
const plain = value => value !== null && typeof value === "object" &&
  !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
const textValue = value => typeof value === "string" && value.length > 0;
const exactKeys = (value, keys) => plain(value) &&
  Object.keys(value).sort().join("\u0000") === [...keys].sort().join("\u0000");
const sha256 = value => typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
let stage = "state-precondition";
let invocationId = null;
let activeGeneration = "A";
let calculationBound = false;
let checkpointUncertain = false;
let fenceStarted = false;
let generationFinished = false;
let generationFinishAttempted = false;
const connectorRetries = [];
class Failure extends Error {
  constructor(classification = "coordination-halt", reason = "coordination-state", semantic = false) {
    super("daily-loss orchestration stopped");
    this.classification = classification;
    this.reason = reason;
    this.semantic = semantic;
    this.stage = stage;
  }
}
const halt = (classification, reason, semantic = false) => {
  throw new Failure(classification, reason, semantic);
};
const compactFailure = failure => Object.freeze({
  schema_version: 1, action: "daily-loss-orchestration", ok: false,
  failure: failure.stage, classification: failure.classification,
  reason_code: failure.reason, checkpoint_uncertain: checkpointUncertain
});
let state;
let bootstrap;
let pythonExe;
let projectRoot;
let scratch;
let startClock;
let constantsReceipt;
let isWindows;
let quote;
let launcher;

// This is the only local command wrapper, including failure disposition. Never
// expose its stdout: receipts can contain a lease token or native file paths.
const runSnapshotJsonCommand = async command => {
  const args = {
    cmd: isWindows ? command + "\nexit $LASTEXITCODE" : command,
    workdir: projectRoot,
    yield_time_ms: 30000,
    max_output_tokens: 12000
  };
  if (isWindows) args.shell = "powershell.exe";
  let process = await tools.exec_command(args);
  let stdout = String(process.output ?? "");
  while (process.session_id !== undefined) {
    const next = await tools.write_stdin({
      session_id: process.session_id,
      chars: "",
      yield_time_ms: 30000,
      max_output_tokens: 12000
    });
    stdout += String(next.output ?? "");
    process = next;
  }
  const finalProcess = Object.freeze({...process, output: stdout});
  let receipt = null;
  try { receipt = JSON.parse(stdout); } catch {}
  return Object.freeze({process: finalProcess, receipt});
};

const requireActive = () => {
  const currentBootstrap = load(BOOTSTRAP_KEY);
  const current = load(STATE_KEY);
  const lease = load(LEASE_KEY);
  if (!plain(currentBootstrap) || currentBootstrap.schema_version !== 1 ||
      !plain(currentBootstrap.context_receipt) ||
      currentBootstrap.context_receipt.invocation_id !== invocationId ||
      !plain(current) || current.schema_version !== 1 || current.phase !== "transport-bound" ||
      !plain(current.context_receipt) || current.context_receipt.invocation_id !== invocationId ||
      !plain(current.lease_binding) || current.lease_binding.invocation_id !== invocationId ||
      current.python_exe !== pythonExe || current.project_root !== projectRoot ||
      !plain(current.receipt) || current.receipt.scratch !== scratch ||
      current.receipt.scratch_id !== state.receipt.scratch_id ||
      current.receipt.source_root !== state.receipt.source_root ||
      current.receipt.source_root_id !== state.receipt.source_root_id ||
      current.account_number !== state.account_number || current.agentic_allowed !== true ||
      !plain(lease) || lease.schema_version !== 1 || lease.phase !== "lease-owned" ||
      lease.invocation_id !== invocationId || !textValue(lease.run_lock_token)) halt();
  return lease;
};
const saveTransport = fields => {
  requireActive();
  const current = load(STATE_KEY);
  store(STATE_KEY, {...current, ...fields});
};
const clockShape = receipt => plain(receipt) && receipt.schema_version === 1 &&
  ["utc", "et", "pt", "pt_iso", "date_et", "date_pt", "historicals_start_time",
    "constants_sha256", "session", "calendar_status"].every(key => textValue(receipt[key])) &&
  (receipt.regular_close_et === null || textValue(receipt.regular_close_et)) &&
  typeof receipt.entry_session_open === "boolean" &&
  typeof receipt.opening_blackout === "boolean" &&
  (receipt.minutes_since_open === null || Number.isSafeInteger(receipt.minutes_since_open)) &&
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(receipt.utc) &&
  Number.isFinite(Date.parse(receipt.utc)) && /^\d{4}-\d{2}-\d{2}$/.test(receipt.date_et) &&
  /^\d{4}-\d{2}-\d{2}$/.test(receipt.date_pt);

const buildSnapshotSourcePurpose = (generation, phase, kind, index) => {
  const generationPurposeSlug = generation === "A" ? "a" : generation === "B" ? "b" : null;
  const allowedPhaseKinds = [
    "discovery:positions", "discovery:orders", "marks:quotes",
    "final:portfolio", "final:positions", "final:orders"
  ];
  if (generationPurposeSlug === null ||
      !allowedPhaseKinds.includes(phase + ":" + kind) ||
      !Number.isSafeInteger(index) || index < 0 || index > 999) {
    return null;
  }
  const purpose = "daily-loss-" + generationPurposeSlug + "-" +
    phase + "-" + kind + "-" + index;
  return /^[a-z0-9][a-z0-9-]{0,47}$/.test(purpose) ? purpose : null;
};

const bindStageOutputPaths = (
  commandResult, expectedKind, expectedGeneration, expectedComplete, expectedFileCount,
  expectedPageBinding
) => {
  const failure = () => Object.freeze({
    schema_version: 1,
    action: "bind-stage-receipt",
    ok: false,
    failure: "stage-receipt-invalid"
  });
  if (!commandResult || typeof commandResult !== "object" ||
      !commandResult.process || commandResult.process.exit_code !== 0) {
    return failure();
  }
  const stageReceipt = commandResult.receipt;
  const sha256 = /^[0-9a-f]{64}$/;
  const allowedKinds = new Set(["portfolio", "positions", "orders", "quotes"]);
  const pageBound = expectedPageBinding !== null;
  if (!stageReceipt || typeof stageReceipt !== "object" || Array.isArray(stageReceipt) ||
      stageReceipt.schema_version !== 1 || stageReceipt.action !== "stage" ||
      stageReceipt.ok !== true || stageReceipt.output_mode !== "helper-allocated" ||
      !allowedKinds.has(expectedKind) ||
      stageReceipt.kind !== expectedKind ||
      (expectedGeneration !== "A" && expectedGeneration !== "B") ||
      stageReceipt.generation !== expectedGeneration ||
      typeof expectedComplete !== "boolean" ||
      stageReceipt.complete !== expectedComplete ||
      typeof stageReceipt.set_id !== "string" || stageReceipt.set_id.length === 0 ||
      !Number.isSafeInteger(stageReceipt.file_count) || stageReceipt.file_count < 1 ||
      !Array.isArray(stageReceipt.files) || !Array.isArray(stageReceipt.output_paths) ||
      !Number.isSafeInteger(expectedFileCount) || expectedFileCount < 1 ||
      stageReceipt.file_count !== stageReceipt.files.length ||
      stageReceipt.file_count !== stageReceipt.output_paths.length ||
      stageReceipt.file_count !== expectedFileCount ||
      (pageBound && (
        (expectedKind !== "positions" && expectedKind !== "orders") ||
        stageReceipt.file_count !== 1 || !expectedPageBinding ||
        typeof expectedPageBinding.source_purpose !== "string" ||
        expectedPageBinding.source_purpose.length < 1 ||
        !Number.isSafeInteger(expectedPageBinding.row_count) ||
        expectedPageBinding.row_count < 0 ||
        (expectedPageBinding.next_cursor !== null &&
          (typeof expectedPageBinding.next_cursor !== "string" ||
           expectedPageBinding.next_cursor.length < 1)) ||
        typeof expectedPageBinding.request_cursor !== "string" ||
        expectedPageBinding.request_cursor.length < 1 ||
        expectedComplete !== (expectedPageBinding.next_cursor === null)
      ))) {
    return failure();
  }
  const outputPaths = [];
  const seenOutputPaths = new Set();
  for (let i = 0; i < stageReceipt.file_count; i += 1) {
    const descriptor = stageReceipt.files[i];
    const outputPath = stageReceipt.output_paths[i];
    if (!descriptor || typeof descriptor !== "object" || Array.isArray(descriptor) ||
        descriptor.index !== i + 1 || typeof descriptor.output !== "string" ||
        typeof descriptor.transport !== "string" || descriptor.transport.length < 1 ||
        !sha256.test(descriptor.source_sha256) ||
        !sha256.test(descriptor.payload_sha256) ||
        typeof descriptor.provenance !== "string" || descriptor.provenance.length < 1 ||
        typeof outputPath !== "string" || outputPath.length < 1 ||
        descriptor.output !== outputPath || seenOutputPaths.has(outputPath) ||
        (pageBound && (
          descriptor.source_purpose !== expectedPageBinding.source_purpose ||
          descriptor.row_count !== expectedPageBinding.row_count ||
          descriptor.next_cursor !== expectedPageBinding.next_cursor ||
          descriptor.request_cursor !== expectedPageBinding.request_cursor
        ))) {
      return failure();
    }
    seenOutputPaths.add(outputPath);
    outputPaths.push(outputPath);
  }
  return Object.freeze({
    schema_version: 1,
    action: "bind-stage-receipt",
    ok: true,
    output_paths: Object.freeze(outputPaths)
  });
};

const bindStageCommandFailure = (commandResult, expectedKind, expectedGeneration) => {
  if (commandResult && commandResult.process &&
      commandResult.process.exit_code === 0) return null;
  const receipt = commandResult && commandResult.receipt;
  const exactFailureProcess = commandResult && commandResult.process &&
    commandResult.process.exit_code === 2;
  const isPlainJsonObject = value => value !== null && typeof value === "object" &&
    !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
  const hasExactJsonKeys = (value, expectedKeys) =>
    Object.keys(value).sort().join("\u0000") === expectedKeys.join("\u0000");
  const trustedStageFailureCodes = Object.freeze([
    "stage_semantic_invalid", "stage_response_invalid", "stage_input_failed",
    "stage_binding_invalid", "stage_internal_failed", "stage_write_failed",
    "source_file_missing", "source_file_changed",
    "source_file_invalid", "stage_retry_state_failed"
  ]);
  const coreFailureEnvelope = exactFailureProcess &&
    isPlainJsonObject(receipt) &&
    receipt.schema_version === 1 && receipt.action === "stage" &&
    receipt.ok === false && receipt.kind === expectedKind &&
    receipt.generation === expectedGeneration && isPlainJsonObject(receipt.error) &&
    hasExactJsonKeys(receipt.error, ["code", "message"]) &&
    typeof receipt.error.code === "string" &&
    typeof receipt.error.message === "string" && receipt.error.message.trim().length > 0 &&
    trustedStageFailureCodes.includes(receipt.error.code);
  const expectedFailureKeys = coreFailureEnvelope &&
    receipt.error.code === "stage_semantic_invalid" ?
    ["action", "error", "generation", "kind", "ok", "recovery_action", "schema_version"] :
    ["action", "error", "generation", "kind", "ok", "schema_version"];
  const expectedSemanticRecovery = expectedGeneration === "A" ?
    "generation-b" : "snapshot-second-attempt-failed";
  const exactFailureEnvelope = coreFailureEnvelope &&
    hasExactJsonKeys(receipt, expectedFailureKeys) &&
    (receipt.error.code !== "stage_semantic_invalid" ||
      receipt.recovery_action === expectedSemanticRecovery);
  const code = exactFailureEnvelope ? receipt.error.code : null;
  if (code === "stage_semantic_invalid") {
    return Object.freeze({
      schema_version: 1, action: "stage-failure", ok: false, failure: code,
      recovery_action: receipt.recovery_action
    });
  }
  if (code === "stage_input_failed" || code === "stage_write_failed" ||
      code === "source_file_missing" || code === "source_file_changed" ||
      code === "source_file_invalid") {
    return Object.freeze({
      schema_version: 1, action: "stage-failure", ok: false, failure: code,
      recovery_action: "snapshot-write-failed"
    });
  }
  if (code === "stage_binding_invalid") {
    return Object.freeze({
      schema_version: 1, action: "stage-failure", ok: false, failure: code,
      recovery_action: "coordination-state"
    });
  }
  if (code === "stage_response_invalid") {
    return Object.freeze({
      schema_version: 1, action: "stage-failure", ok: false, failure: code,
      recovery_action: "coordination-state"
    });
  }
  if (code === "stage_retry_state_failed") {
    return Object.freeze({
      schema_version: 1, action: "stage-failure", ok: false, failure: code,
      recovery_action: "coordination-state"
    });
  }
  if (code === "stage_internal_failed") {
    return Object.freeze({
      schema_version: 1, action: "stage-failure", ok: false, failure: code,
      recovery_action: "coordination-state"
    });
  }
  return Object.freeze({
    schema_version: 1, action: "stage-failure", ok: false,
    failure: "stage-command-unclassified", recovery_action: "coordination-state"
  });
};

const bindDailyLossCommandFailure = (
  commandResult, expectedMode, expectedGeneration
) => {
  if (commandResult && commandResult.process &&
      commandResult.process.exit_code === 0) return null;
  const receipt = commandResult && commandResult.receipt;
  const plain = value => value !== null && typeof value === "object" &&
    !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype;
  const exactKeys = (value, keys) => plain(value) &&
    Object.keys(value).sort().join("\u0000") === keys.join("\u0000");
  const nonSemanticCodes = Object.freeze([
    "daily_loss_binding_invalid", "daily_loss_input_failed",
    "daily_loss_output_failed", "daily_loss_internal_failed"
  ]);
  const semantic = receipt && receipt.error &&
    receipt.error.code === "daily_loss_semantic_invalid";
  const checkpointRetry = expectedMode === "calculation" && receipt &&
    receipt.error && receipt.error.code === "daily_loss_checkpoint_failed";
  const expectedKeys = semantic ?
    ["action", "error", "generation", "mode", "ok", "recovery_action", "schema_version"] :
    ["action", "error", "generation", "mode", "ok", "schema_version"];
  const expectedRecovery = expectedGeneration === "A" ?
    "generation-b" : "snapshot-second-attempt-failed";
  const exact = commandResult && commandResult.process &&
    commandResult.process.exit_code === 2 &&
    (expectedMode === "discovery" || expectedMode === "calculation") &&
    (expectedGeneration === "A" || expectedGeneration === "B") &&
    exactKeys(receipt, expectedKeys) && receipt.schema_version === 1 &&
    receipt.action === "daily-loss" && receipt.ok === false &&
    receipt.mode === expectedMode && receipt.generation === expectedGeneration &&
    exactKeys(receipt.error, ["code", "message"]) &&
    typeof receipt.error.message === "string" &&
    receipt.error.message.trim().length > 0 &&
    ((semantic && receipt.recovery_action === expectedRecovery) ||
     (!semantic && nonSemanticCodes.includes(receipt.error.code)) ||
     checkpointRetry);
  if (!exact) {
    return Object.freeze({
      schema_version: 1, action: "daily-loss-failure", ok: false,
      failure: "daily-loss-command-unclassified",
      recovery_action: "coordination-state"
    });
  }
  if (semantic) {
    return Object.freeze({
      schema_version: 1, action: "daily-loss-failure", ok: false,
      failure: receipt.error.code,
      recovery_action: receipt.recovery_action
    });
  }
  if (checkpointRetry) {
    return Object.freeze({
      schema_version: 1, action: "daily-loss-failure", ok: false,
      failure: receipt.error.code,
      recovery_action: "retry-identical-calculation"
    });
  }
  const writeFailure = receipt.error.code === "daily_loss_input_failed" ||
    receipt.error.code === "daily_loss_output_failed";
  return Object.freeze({
    schema_version: 1, action: "daily-loss-failure", ok: false,
    failure: receipt.error.code,
    recovery_action: writeFailure ? "snapshot-write-failed" : "coordination-state"
  });
};

const bindDiscoveredSymbols = (
  commandResult, expectedTradingDateEt, expectedAsOfUtc
) => {
  const failure = () => Object.freeze({
    schema_version: 1,
    action: "bind-discovered-symbols",
    ok: false,
    failure: "symbol-discovery-receipt-invalid"
  });
  if (!commandResult || typeof commandResult !== "object" ||
      !commandResult.process || commandResult.process.exit_code !== 0) {
    return failure();
  }
  const receipt = commandResult.receipt;
  if (!receipt || typeof receipt !== "object" || Array.isArray(receipt) ||
      receipt.schema_version !== 1 || receipt.action !== "discover-symbols" ||
      receipt.ok !== true || receipt.trading_date_et !== expectedTradingDateEt ||
      receipt.as_of_utc !== expectedAsOfUtc ||
      !Number.isSafeInteger(receipt.symbol_count) || receipt.symbol_count < 0 ||
      !Array.isArray(receipt.symbols) || receipt.symbols.length !== receipt.symbol_count) {
    return failure();
  }
  const symbols = [];
  const seen = new Set();
  for (const symbol of receipt.symbols) {
    if (typeof symbol !== "string" || symbol.length < 1 || symbol !== symbol.trim() ||
        symbol !== symbol.toUpperCase() || seen.has(symbol)) {
      return failure();
    }
    seen.add(symbol);
    symbols.push(symbol);
  }
  return Object.freeze({
    schema_version: 1,
    action: "bind-discovered-symbols",
    ok: true,
    symbols: Object.freeze(symbols)
  });
};

const bindSourceReceipt = (result, action, purpose) => {
  const receipt = result.receipt;
  if (result.process.exit_code !== 0 || !plain(receipt) || receipt.schema_version !== 1 ||
      receipt.action !== action || receipt.ok !== true || receipt.scratch !== scratch ||
      receipt.purpose !== purpose) halt();
  return receipt;
};

const reserveSnapshotSourceBeforeRead = async (generation, phase, kind, index) => {
  stage = "source-reservation";
  const purpose = buildSnapshotSourcePurpose(generation, phase, kind, index);
  if (purpose === null) halt();
  const command = launcher + " broker_snapshot.py reserve-source --scratch " + quote(scratch) +
    " --purpose " + quote(purpose);
  const commandResult = await runSnapshotJsonCommand(command);
  const receipt = bindSourceReceipt(commandResult, "reserve-source", purpose);
  if (receipt.status !== "reserved" || receipt.idempotent !== false ||
      !textValue(receipt.reservation_id) || !textValue(receipt.source)) halt();
  return receipt;
};

// A read is reserved BEFORE invocation. Only an explicit connector error permits
// the immediate identical second attempt; source/save/helper faults cannot replay it.
const readCommittedSource = async (generation, phase, kind, index, readTool, brokerArguments) => {
  requireActive();
  const sourceReservation = await reserveSnapshotSourceBeforeRead(generation, phase, kind, index);
  stage = "broker-read";
  let fullToolResult;
  let firstFailed = false;
  requireActive();
  try {
    fullToolResult = await readTool(brokerArguments);
    firstFailed = !!(fullToolResult && fullToolResult.isError === true);
  } catch (ignored) { firstFailed = true; }
  if (firstFailed) {
    let retryFailed = false;
    requireActive();
    try {
      fullToolResult = await readTool(brokerArguments);
      retryFailed = !!(fullToolResult && fullToolResult.isError === true);
    } catch (ignored) { retryFailed = true; }
    connectorRetries.push(Object.freeze({generation, phase, kind, recovered: !retryFailed}));
    if (retryFailed) {
      stage = "connector-abort";
      const abortCommand = launcher + " broker_snapshot.py abort-source --scratch " + quote(scratch) +
        " --purpose " + quote(sourceReservation.purpose) +
        " --reservation-id " + quote(sourceReservation.reservation_id) + " --reason connector-failed";
      const result = await runSnapshotJsonCommand(abortCommand);
      const receipt = bindSourceReceipt(result, "abort-source", sourceReservation.purpose);
      if (receipt.status !== "aborted" || receipt.reservation_id !== sourceReservation.reservation_id ||
          receipt.reason !== "connector-failed" || receipt.source !== null) halt();
      stage = "broker-read";
      halt("snapshot-failure", "snapshot-validation-failed");
    }
  }
  stage = "source-write";
  try {
    const payload = JSON.stringify(fullToolResult);
    if (typeof payload !== "string" || payload[0] !== "{" || payload[payload.length - 1] !== "}")
      throw new Error("invalid full response");
    const parsed = JSON.parse(payload);
    if (!plain(parsed)) throw new Error("invalid full response");
    const patchTarget = sourceReservation.source.replaceAll("\\", "/");
    const patch = "*** Begin Patch\n*** Add File: " + patchTarget + "\n+" +
      payload.replaceAll("\n", "\n+") + "\n*** End Patch";
    const patchResult = await tools.apply_patch(patch);
    if (patchResult && patchResult.isError === true) throw new Error("file change rejected");
  } catch (ignored) { halt("snapshot-failure", "snapshot-write-failed"); }
  stage = "source-commit";
  let commandResult;
  try {
    commandResult = await runSnapshotJsonCommand(launcher +
      " broker_snapshot.py commit-source --scratch " + quote(scratch) +
      " --purpose " + quote(sourceReservation.purpose));
  } catch (ignored) { halt("snapshot-failure", "snapshot-write-failed"); }
  // A failed commit is a transport failure. A successful helper whose runner
  // binding is malformed is local coordination, with no reread or second write.
  if (commandResult.process.exit_code !== 0) halt("snapshot-failure", "snapshot-write-failed");
  const receipt = bindSourceReceipt(commandResult, "commit-source", sourceReservation.purpose);
  if (receipt.status !== "committed" || !textValue(receipt.reservation_id) ||
      !sha256(receipt.source_sha256) || !Number.isSafeInteger(receipt.source_size) ||
      receipt.source_size < 0) halt();
  return sourceReservation;
};

const routeFailure = failure => {
  if (failure.recovery_action === "generation-b" ||
      failure.recovery_action === "snapshot-second-attempt-failed")
    halt("snapshot-failure", "snapshot-second-attempt-failed", true);
  if (failure.recovery_action === "snapshot-write-failed")
    halt("snapshot-failure", "snapshot-write-failed");
  halt();
};

// Receipt binding is shared. Command construction stays with each explicit kind
// below, so nonpaginated kinds can never acquire cursor flags.
const bindStage = (result, kind, generation, complete, count, pageBinding) => {
  const failure = bindStageCommandFailure(result, kind, generation);
  if (failure !== null) routeFailure(failure);
  const binding = bindStageOutputPaths(result, kind, generation, complete, count, pageBinding);
  if (!binding.ok) halt();
  return binding.output_paths;
};

const pageContract = async (sourceReservation, kind, requestCursors) => {
  stage = "page-contract";
  const result = await runSnapshotJsonCommand(launcher +
    " connector_contract.py page --scratch " + quote(scratch) +
    " --source-purpose " + quote(sourceReservation.purpose) + " --kind " + kind +
    requestCursors.map(cursor => " --request-cursor " + quote(cursor)).join(""));
  const receipt = result.receipt;
  if (result.process.exit_code !== 0) {
    const typed = result.process.exit_code === 2 &&
      exactKeys(receipt, ["schema_version", "action", "ok", "error"]) &&
      receipt.schema_version === 1 && receipt.action === "page" && receipt.ok === false &&
      exactKeys(receipt.error, ["code", "message"]) && textValue(receipt.error.message) &&
      receipt.error.message.trim().length > 0;
    if (typed && receipt.error.code === "invalid_contract")
      halt("snapshot-failure", "snapshot-second-attempt-failed", true);
    if (typed && ["source_file_missing", "source_file_changed", "source_file_invalid"].includes(receipt.error.code))
      halt("snapshot-failure", "snapshot-write-failed");
    if (typed && receipt.error.code === "pagination_stopped")
      halt("snapshot-failure", "snapshot-validation-failed");
    halt();
  }
  if (!plain(receipt) || receipt.schema_version !== 1 || receipt.action !== "page" ||
      receipt.ok !== true || receipt.kind !== kind ||
      receipt.source_purpose !== sourceReservation.purpose ||
      receipt.request_cursor !== requestCursors[requestCursors.length - 1] ||
      !Array.isArray(receipt.request_cursors) ||
      receipt.request_cursors.length !== requestCursors.length ||
      receipt.request_cursors.some((cursor, index) => cursor !== requestCursors[index]) ||
      !Number.isSafeInteger(receipt.row_count) || receipt.row_count < 0 ||
      (receipt.next_cursor !== null && !textValue(receipt.next_cursor)) ||
      typeof receipt.complete !== "boolean" || receipt.complete !== (receipt.next_cursor === null)) halt();
  return receipt;
};

const collectPages = async (generation, phase, kind, readTool) => {
  const requestCursors = [];
  const pagePaths = [];
  let cursor = "FIRST";
  for (let pageIndex = 0; pageIndex < 1000; pageIndex += 1) {
    requestCursors.push(cursor);
    const brokerArguments = Object.freeze(pageIndex === 0 ? {account_number: state.account_number} :
      {account_number: state.account_number, cursor});
    const sourceReservation = await readCommittedSource(generation, phase, kind, pageIndex, readTool, brokerArguments);
    const contract = await pageContract(sourceReservation, kind, Object.freeze([...requestCursors]));
    const pageStageBinding = Object.freeze({source_purpose: contract.source_purpose,
      row_count: contract.row_count, next_cursor: contract.next_cursor, request_cursor: cursor});
    const expectedFileCount = 1;
    stage = "page-stage";
    // The two paginated templates deliberately cannot serve portfolio or quotes.
    const stageCommand = kind === "positions" ? launcher +
      " broker_snapshot.py stage --kind positions --generation " + generation +
      " --source-purpose " + quote(sourceReservation.purpose) + " --auto-output-scratch " + quote(scratch) +
      " --request-cursor " + quote(cursor) + (contract.complete ? "" : " --allow-more") : launcher +
      " broker_snapshot.py stage --kind orders --generation " + generation +
      " --source-purpose " + quote(sourceReservation.purpose) + " --auto-output-scratch " + quote(scratch) +
      " --request-cursor " + quote(cursor) + (contract.complete ? "" : " --allow-more");
    const result = await runSnapshotJsonCommand(stageCommand);
    const stagedOutputPaths = bindStage(result, kind, generation, contract.complete, expectedFileCount, pageStageBinding);
    pagePaths.push(...stagedOutputPaths);
    if (contract.complete) {
      if (pagePaths.length === 1) return Object.freeze(pagePaths);
      stage = "pages-aggregate-stage";
      const sourceSegments = pagePaths.map(path => " --source " + quote(path)).join("");
      const cursorSegments = requestCursors.map(value => " --request-cursor " + quote(value)).join("");
      const aggregateCommand = kind === "positions" ? launcher +
        " broker_snapshot.py stage --kind positions --generation " + generation + sourceSegments +
        " --auto-output-scratch " + quote(scratch) + cursorSegments : launcher +
        " broker_snapshot.py stage --kind orders --generation " + generation + sourceSegments +
        " --auto-output-scratch " + quote(scratch) + cursorSegments;
      const aggregateResult = await runSnapshotJsonCommand(aggregateCommand);
      return bindStage(aggregateResult, kind, generation, true, pagePaths.length, null);
    }
    cursor = contract.next_cursor;
  }
  stage = "page-contract";
  halt("snapshot-failure", "snapshot-validation-failed");
};

const readClock = async label => {
  stage = label;
  const result = await runSnapshotJsonCommand(launcher +
    " market_clock.py --json --expected-constants-sha256 " + quote(constantsReceipt.source_sha256));
  if (result.process.exit_code === 2) halt("configuration-halt", "configuration-invalid");
  if (result.process.exit_code !== 0) halt();
  const receipt = result.receipt;
  if (!plain(receipt)) halt();
  if (receipt.constants_sha256 !== constantsReceipt.source_sha256)
    halt("configuration-halt", "configuration-invalid");
  if (!clockShape(receipt)) halt();
  if (receipt.date_et !== startClock.date_et || Date.parse(receipt.utc) < Date.parse(startClock.utc))
    halt("snapshot-failure", "snapshot-second-attempt-failed", true);
  return receipt;
};

const transitionGeneration = async outcome => {
  stage = outcome === null ? "authorize-generation-b" : "finish-generation-b";
  if (outcome !== null) generationFinishAttempted = true;
  const action = outcome === null ? "authorize-generation-b" : "finish-generation-b";
  const result = await runSnapshotJsonCommand(launcher + " broker_snapshot.py " + action +
    " --scratch " + quote(scratch) + (outcome === null ? "" : " --outcome " + outcome));
  const receipt = result.receipt;
  const keys = outcome === null ? ["schema_version", "action", "ok", "scratch", "state", "reason", "idempotent"] :
    ["schema_version", "action", "ok", "scratch", "state", "outcome", "idempotent"];
  if (result.process.exit_code !== 0 || !exactKeys(receipt, keys) || receipt.schema_version !== 1 ||
      receipt.action !== action || receipt.ok !== true || receipt.scratch !== scratch ||
      typeof receipt.idempotent !== "boolean" ||
      (outcome === null ? receipt.state !== "generation-b-authorized" || receipt.reason !== "daily-loss-semantic-invalid" :
        receipt.state !== "generation-b-exhausted" || receipt.outcome !== outcome)) halt();
  if (outcome !== null) generationFinished = true;
};

const renewForGenerationB = async () => {
  stage = "generation-b-renewal";
  const lease = requireActive();
  const result = await runSnapshotJsonCommand(launcher + " run_lock.py renew --token " + quote(lease.run_lock_token));
  const receipt = result.receipt;
  if (result.process.exit_code !== 0 ||
      !exactKeys(receipt, ["schema_version", "action", "ok", "token", "renewed_at", "expires_at", "lease_seconds"]) ||
      receipt.schema_version !== 1 || receipt.action !== "renew" || receipt.ok !== true ||
      receipt.token !== lease.run_lock_token || !textValue(receipt.renewed_at) || !textValue(receipt.expires_at) ||
      !Number.isFinite(Date.parse(receipt.renewed_at)) || !Number.isFinite(Date.parse(receipt.expires_at)) ||
      Date.parse(receipt.expires_at) <= Date.now() ||
      Date.parse(receipt.expires_at) <= Date.parse(receipt.renewed_at) ||
      !Number.isSafeInteger(receipt.lease_seconds) || receipt.lease_seconds <= 0) {
    // A failed renewal is the prescribed fencing-loss path; never release a
    // potentially newer owner's lease or issue another broker request.
    store(LEASE_KEY, {schema_version: 1, phase: "lease-lost", invocation_id: invocationId});
    halt("lease-lost", "lease-renewal-failed");
  }
};

const bindCalculation = (commandResult, generation, finalClock) => {
  const receipt = commandResult.receipt;
  if (commandResult.process.exit_code !== 0 || !exactKeys(receipt,
    ["schema_version", "action", "ok", "mode", "generation", "invocation_id", "constants_sha256",
      "daily_loss_halt_pct", "stop_count_halt", "daily_loss_tripped", "stop_count_tripped", "entry_guard_outcome", "result"]) ||
      receipt.schema_version !== 1 || receipt.action !== "daily-loss" || receipt.ok !== true ||
      receipt.mode !== "calculation" || receipt.generation !== generation || receipt.invocation_id !== invocationId ||
      receipt.constants_sha256 !== constantsReceipt.source_sha256 ||
      receipt.daily_loss_halt_pct !== constantsReceipt.values.DAILY_LOSS_HALT_PCT ||
      receipt.stop_count_halt !== constantsReceipt.values.STOP_COUNT_HALT ||
      typeof receipt.daily_loss_tripped !== "boolean" || typeof receipt.stop_count_tripped !== "boolean" ||
      receipt.entry_guard_outcome !== (receipt.daily_loss_tripped || receipt.stop_count_tripped ? "tripped" : "clear")) return false;
  const result = receipt.result;
  if (!plain(result) || result.schema_version !== 1 || result.trading_date_et !== startClock.date_et ||
      result.stop_count_date_pt !== startClock.date_pt || result.as_of_utc !== finalClock.utc ||
      typeof result.halt_new_buys !== "boolean" ||
      result.status !== (result.halt_new_buys ? "tripped" : "clear") ||
      receipt.daily_loss_tripped !== result.halt_new_buys ||
      !Number.isSafeInteger(result.stop_fills_today) || result.stop_fills_today < 0 ||
      !Array.isArray(result.stopped_out_symbols) ||
      result.stopped_out_symbols.some((symbol, index, symbols) =>
        !textValue(symbol) || symbol.trim() !== symbol ||
        (index > 0 && symbols[index - 1] >= symbol))) return false;
  return true;
};

const completeTerminal = async failure => {
  stage = "complete-second";
  const lease = requireActive();
  const outcome = failure.classification === "snapshot-failure" ? "snapshot-terminal" : "coordination-terminal";
  const result = await runSnapshotJsonCommand(launcher + " run_lifecycle.py complete-second --invocation-id " +
    quote(invocationId) + " --run-token " + quote(lease.run_lock_token) + " --outcome " + outcome);
  const receipt = result.receipt;
  if (result.process.exit_code !== 0 || !exactKeys(receipt,
      ["schema_version", "action", "ok", "invocation_id", "phase", "outcome", "recorded", "sequence", "lease_renewed"]) ||
      receipt.schema_version !== 1 || receipt.action !== "complete-second" || receipt.ok !== true ||
      receipt.invocation_id !== invocationId || receipt.phase !== "daily-loss" || receipt.outcome !== outcome ||
      typeof receipt.recorded !== "boolean" || !Number.isSafeInteger(receipt.sequence) || receipt.sequence < 1 ||
      receipt.lease_renewed !== true) halt();
  saveTransport({second_completion_receipt: receipt});
};

try {
  state = load(STATE_KEY);
  bootstrap = load(BOOTSTRAP_KEY);
  invocationId = state && state.lease_binding && state.lease_binding.invocation_id;
  // The execution fence prevents an interrupted or repeated driver invocation
  // from replaying broker reads. It is not a source-journal sequence authority.
  const priorExecution = load(EXECUTION_KEY);
  if (priorExecution !== undefined && priorExecution !== null) {
    stage = "execution-already-started";
    return compactFailure(new Failure());
  }
  if (!plain(state) || state.schema_version !== 1 || state.phase !== "transport-bound" ||
      !plain(bootstrap) || bootstrap.schema_version !== 1 || bootstrap.phase !== "context-bound" ||
      !textValue(invocationId) || !textValue(state.python_exe) || !textValue(state.project_root) ||
      !plain(state.receipt) || !textValue(state.receipt.scratch) ||
      !textValue(state.receipt.scratch_id) || !textValue(state.receipt.source_root) ||
      !textValue(state.receipt.source_root_id) ||
      !textValue(state.account_number) || state.agentic_allowed !== true ||
      !plain(bootstrap.constants_receipt) || !sha256(bootstrap.constants_receipt.source_sha256) ||
      !plain(bootstrap.constants_receipt.values) ||
      !textValue(bootstrap.constants_receipt.values.DAILY_LOSS_HALT_PCT) ||
      !Number.isSafeInteger(bootstrap.constants_receipt.values.STOP_COUNT_HALT) ||
      !clockShape(bootstrap.start_clock) ||
      bootstrap.start_clock.constants_sha256 !== bootstrap.constants_receipt.source_sha256) halt();
  pythonExe = state.python_exe;
  projectRoot = state.project_root;
  scratch = state.receipt.scratch;
  startClock = bootstrap.start_clock;
  constantsReceipt = bootstrap.constants_receipt;
  isWindows = /^[A-Za-z]:[\\/]/.test(pythonExe);
  const psq = value => "'" + String(value).replaceAll("'", "''") + "'";
  const shq = value => "'" + String(value).replaceAll("'", "'\"'\"'") + "'";
  quote = isWindows ? psq : shq;
  launcher = (isWindows ? "& " : "") + quote(pythonExe);
  requireActive();
  const second = state.second_entry_receipt;
  if (!exactKeys(second, ["schema_version", "action", "ok", "invocation_id", "phase", "recorded", "event_sequence",
      "entry_eligible", "daily_loss_attempted", "scratch_id", "source_root_id", "constants_sha256",
      "daily_loss_halt_pct", "stop_count_halt", "lease_renewed"]) ||
      second.schema_version !== 1 || second.action !== "enter-second" || second.ok !== true ||
      second.invocation_id !== invocationId || second.phase !== "daily-loss" ||
      typeof second.recorded !== "boolean" || !Number.isSafeInteger(second.event_sequence) || second.event_sequence < 1 ||
      second.entry_eligible !== true || second.daily_loss_attempted !== true ||
      second.scratch_id !== state.receipt.scratch_id || second.source_root_id !== state.receipt.source_root_id ||
      second.constants_sha256 !== constantsReceipt.source_sha256 ||
      second.daily_loss_halt_pct !== constantsReceipt.values.DAILY_LOSS_HALT_PCT ||
      second.stop_count_halt !== constantsReceipt.values.STOP_COUNT_HALT || second.lease_renewed !== true ||
      Object.prototype.hasOwnProperty.call(state, "daily_loss_receipt") ||
      Object.prototype.hasOwnProperty.call(state, "second_completion_receipt")) halt();
  store(EXECUTION_KEY, {schema_version: 1, invocation_id: invocationId, status: "running"});
  fenceStarted = true;
  const fence = load(EXECUTION_KEY);
  if (!plain(fence) || fence.schema_version !== 1 || fence.invocation_id !== invocationId || fence.status !== "running") halt();
  stage = "broker-tool-resolution";
  const resolveRead = name => {
    const matches = Array.isArray(allTools) ? allTools.filter(tool => tool && tool.name === name) : [];
    if (matches.length !== 1 || typeof tools[name] !== "function") halt();
    return tools[name].bind(tools);
  };
  const reads = Object.freeze({
    positions: resolveRead("mcp__robinhood_mcp__get_equity_positions"),
    orders: resolveRead("mcp__robinhood_mcp__get_equity_orders"),
    quotes: resolveRead("mcp__robinhood_mcp__get_equity_quotes"),
    portfolio: resolveRead("mcp__robinhood_mcp__get_portfolio")
  });
  if (typeof tools.apply_patch !== "function" || typeof tools.exec_command !== "function" ||
      typeof tools.write_stdin !== "function") halt();
  const scratchChild = basename => scratch + (scratch.includes("\\") ? "\\" : "/") + basename;
  for (const generation of ["A", "B"]) {
    activeGeneration = generation;
    try {
      const discoveryPositions = await collectPages(generation, "discovery", "positions", reads.positions);
      const discoveryOrders = await collectPages(generation, "discovery", "orders", reads.orders);
      const discoveryClock = await readClock("discovery-clock");
      stage = "symbol-discovery";
      const symbolCommand = launcher + " daily_loss.py" +
        " --positions" + discoveryPositions.map(path => " " + quote(path)).join("") +
        " --orders" + discoveryOrders.map(path => " " + quote(path)).join("") +
        " --snapshot-generation " + generation + " --trading-date " + quote(startClock.date_et) +
        " --stop-date-pt " + quote(startClock.date_pt) + " --as-of-utc " + quote(discoveryClock.utc) +
        " --symbols-out " + quote(scratchChild("daily-loss-" + generation.toLowerCase() + "-symbols.json")) + " --failure-json";
      const symbolCommandResult = await runSnapshotJsonCommand(symbolCommand);
      const symbolFailure = bindDailyLossCommandFailure(symbolCommandResult, "discovery", generation);
      if (symbolFailure !== null) routeFailure(symbolFailure);
      const symbolBinding = bindDiscoveredSymbols(symbolCommandResult, startClock.date_et, discoveryClock.utc);
      if (!symbolBinding.ok) halt();
      const requiredQuoteSymbols = symbolBinding.symbols;
      let quotePaths = [];
      for (let offset = 0, index = 0; offset < requiredQuoteSymbols.length; offset += 20, index += 1) {
        const brokerArguments = Object.freeze({symbols: Object.freeze(requiredQuoteSymbols.slice(offset, offset + 20))});
        const sourceReservation = await readCommittedSource(generation, "marks", "quotes", index, reads.quotes, brokerArguments);
        stage = "quotes-stage";
        const result = await runSnapshotJsonCommand(launcher + " broker_snapshot.py stage --kind quotes --generation " +
          generation + " --source-purpose " + quote(sourceReservation.purpose) + " --auto-output-scratch " + quote(scratch));
        quotePaths.push(...bindStage(result, "quotes", generation, true, 1, null));
      }
      if (quotePaths.length > 1) {
        stage = "quotes-aggregate-stage";
        const expectedFileCount = quotePaths.length;
        const result = await runSnapshotJsonCommand(launcher + " broker_snapshot.py stage --kind quotes --generation " +
          generation + quotePaths.map(path => " --source " + quote(path)).join("") + " --auto-output-scratch " + quote(scratch));
        quotePaths = bindStage(result, "quotes", generation, true, expectedFileCount, null);
      }
      const finalClock = await readClock("final-clock");
      if (Date.parse(finalClock.utc) < Date.parse(discoveryClock.utc))
        halt("snapshot-failure", "snapshot-second-attempt-failed", true);
      const portfolioSource = await readCommittedSource(generation, "final", "portfolio", 0, reads.portfolio,
        Object.freeze({account_number: state.account_number}));
      stage = "portfolio-stage";
      const portfolioResult = await runSnapshotJsonCommand(launcher +
        " broker_snapshot.py stage --kind portfolio --generation " + generation +
        " --source-purpose " + quote(portfolioSource.purpose) + " --auto-output-scratch " + quote(scratch));
      const portfolioPaths = bindStage(portfolioResult, "portfolio", generation, true, 1, null);
      const finalPositions = await collectPages(generation, "final", "positions", reads.positions);
      const finalOrders = await collectPages(generation, "final", "orders", reads.orders);
      stage = "daily-loss-calculation";
      const lease = requireActive();
      const positionSegments = Object.freeze([" --positions", ...finalPositions.map(path => " " + quote(path))]);
      const orderSegments = Object.freeze([" --orders", ...finalOrders.map(path => " " + quote(path))]);
      const quoteSegments = Object.freeze(quotePaths.length === 0 ? [] :
        [" --quotes", ...quotePaths.map(path => " " + quote(path))]);
      const evaluationCommandParts = Object.freeze([
        launcher + " daily_loss.py", " --portfolio " + quote(portfolioPaths[0]),
        ...positionSegments, ...orderSegments, ...quoteSegments,
        " --snapshot-generation " + generation + " --trading-date " + quote(startClock.date_et) +
        " --stop-date-pt " + quote(startClock.date_pt) + " --as-of-utc " + quote(finalClock.utc) +
        " --expected-constants-sha256 " + quote(constantsReceipt.source_sha256) +
        " --invocation-id " + quote(invocationId) + " --run-token " + quote(lease.run_lock_token) +
        " --json-out " + quote(scratchChild("daily-loss-" + generation.toLowerCase() + ".json")) + " --failure-json"
      ]);
      const evaluationCommand = evaluationCommandParts.join("");
      let checkpointRetryUsed = false;
      while (true) {
        const currentLease = requireActive();
        if (currentLease.run_lock_token !== lease.run_lock_token) halt();
        let commandResult;
        try { commandResult = await runSnapshotJsonCommand(evaluationCommand); }
        catch (ignored) { checkpointUncertain = true; }
        const failure = commandResult ? bindDailyLossCommandFailure(commandResult, "calculation", generation) : null;
        if (failure && failure.recovery_action !== "retry-identical-calculation") {
          if (checkpointUncertain) halt();
          routeFailure(failure);
        }
        if (commandResult && failure === null && bindCalculation(commandResult, generation, finalClock)) {
          checkpointUncertain = false;
          calculationBound = true;
          saveTransport({daily_loss_receipt: commandResult.receipt});
          if (generation === "B") await transitionGeneration("completed");
          const compact = Object.freeze({schema_version: 1, action: "daily-loss-orchestration", ok: true,
            generation, entry_guard_outcome: commandResult.receipt.entry_guard_outcome});
          saveTransport({daily_loss_orchestration: compact, daily_loss_connector_retries: Object.freeze([...connectorRetries])});
          store(EXECUTION_KEY, {schema_version: 1, invocation_id: invocationId, status: "finished"});
          return compact;
        }
        checkpointUncertain = true;
        if (checkpointRetryUsed) halt();
        checkpointRetryUsed = true;
      }
    } catch (error) {
      if (!(error instanceof Failure) || !error.semantic) throw error;
      if (generation === "B") {
        await transitionGeneration("failed");
        throw error;
      }
      await transitionGeneration(null);
      activeGeneration = "B";
      await renewForGenerationB();
    }
  }
  halt();
} catch (error) {
  let failure = error instanceof Failure ? error : new Failure();
  if (fenceStarted && !checkpointUncertain && activeGeneration === "B" &&
      !generationFinished && !generationFinishAttempted) {
    try { await transitionGeneration(calculationBound ? "completed" : "failed"); }
    catch (transitionError) {
      failure = transitionError instanceof Failure ? transitionError : new Failure();
    }
  }
  if (fenceStarted && !checkpointUncertain && !calculationBound && failure.classification !== "lease-lost") {
    try {
      await completeTerminal(failure);
    } catch (terminalError) {
      failure = terminalError instanceof Failure ? terminalError : new Failure();
    }
  }
  const compact = compactFailure(failure);
  if (fenceStarted) {
    // Publishing diagnostics does not require ownership; it carries no token,
    // payload, account, path, or clear/tripped claim and never changes phases.
    const current = load(STATE_KEY);
    if (plain(current) && plain(current.lease_binding) && current.lease_binding.invocation_id === invocationId)
      store(STATE_KEY, {...current, daily_loss_orchestration: compact,
        daily_loss_connector_retries: Object.freeze([...connectorRetries])});
    store(EXECUTION_KEY, {schema_version: 1, invocation_id: invocationId, status: "finished"});
  }
  return compact;
}
}
