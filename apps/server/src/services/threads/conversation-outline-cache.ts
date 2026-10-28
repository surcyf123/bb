import {
  getDatabaseDataVersion,
  getThreadEventRewriteGeneration,
  getThreadEventStructuralGeneration,
  type DbConnection,
} from "@bb/db";
import type { ThreadConversationOutlineItem } from "@bb/server-contract";
import {
  MIN_AGENT_MESSAGE_DELTAS_FOR_SUMMARY_COMPACTION,
  type ThreadEventWithMeta,
} from "@bb/thread-view";

interface ConversationOutlineProjection {
  events: ThreadEventWithMeta[];
  items: {
    item: ThreadConversationOutlineItem;
    sourceSeqStart: number;
    sourceSeqEnd: number;
    trailing: boolean;
  }[];
}

export interface ConversationOutlineSelection {
  events: ThreadEventWithMeta[];
  project: () => ConversationOutlineProjection["items"];
}

interface Checkpoint {
  agentMessageDeltaCount: number;
  items: ThreadConversationOutlineItem[];
  trailingItems: ThreadConversationOutlineItem[];
  sequenceStart: number;
  turnIds: Set<string>;
  requestIds: Set<string>;
  itemIds: Set<string>;
  prefixFingerprint: string;
}

interface Entry {
  agentMessageDeltaCount: number;
  checkpoint: Checkpoint;
  contextBoundarySeq: number;
  dataVersion: number;
  externalUserBoundarySequence: number | null;
  generation: number;
  structuralGeneration: number;
  key: string;
  maxSeq: number;
  chars: number;
}

interface OutlineCache {
  entries: Map<string, Entry>;
  chars: number;
}

const caches = new WeakMap<DbConnection, OutlineCache>();
const DEFAULT_MAX_ENTRIES = 16;
const DEFAULT_MAX_CHARS = 8_000_000;
let maxEntries = DEFAULT_MAX_ENTRIES;
let maxChars = DEFAULT_MAX_CHARS;
let requireUnchangedDataVersion = true;
let auditRate = 0;
let auditBurst = 0;
let audits = 0;
let auditMismatches = 0;
const AUDIT_SUMMARY_INTERVAL = 50;

interface ConversationOutlineAuditReporter {
  mismatch(details: Record<string, unknown>): void;
  summary(details: { audits: number; mismatches: number }): void;
}

let auditReporter: ConversationOutlineAuditReporter | null = null;
let deferAudits = false;
let deferredAudits: (() => string | null)[] = [];

export function configureConversationOutlineCache(options: {
  maxEntries: number;
  maxChars: number;
  requireUnchangedDataVersion: boolean;
  auditRate: number;
  auditBurst: number;
  auditReporter: ConversationOutlineAuditReporter | null;
  deferAudits?: boolean;
}): void {
  maxEntries = options.maxEntries;
  maxChars = options.maxChars;
  requireUnchangedDataVersion = options.requireUnchangedDataVersion;
  auditRate = options.auditRate;
  auditBurst = options.auditBurst;
  auditReporter = options.auditReporter;
  deferAudits = options.deferAudits ?? false;
  deferredAudits = [];
}

export function runDeferredConversationOutlineAudits(): string[] {
  const mismatched: string[] = [];
  for (const audit of deferredAudits.splice(0)) {
    const threadId = audit();
    if (threadId !== null) mismatched.push(threadId);
  }
  return mismatched;
}

const PRUNABLE_EVENT_TYPES = [
  "item/agentMessage/delta",
  "item/backgroundTask/progress",
  "item/commandExecution/outputDelta",
  "item/reasoning/summaryTextDelta",
  "item/reasoning/textDelta",
  "provider/rateLimits/updated",
  "thread/contextWindowUsage/updated",
  "thread/tokenUsage/updated",
  "turn/diff/updated",
] as const;

function readCount(row: unknown, field: string): number | null {
  if (typeof row !== "object" || row === null || !(field in row))
    throw new Error(`Expected ${field}`);
  const value: unknown = Reflect.get(row, field);
  if (value === null) return null;
  if (typeof value !== "number") throw new Error(`Expected ${field}`);
  return value;
}

function prefixFingerprint(
  db: DbConnection,
  threadId: string,
  sequence: number,
): string {
  const row: unknown = db.$client
    .prepare(
      `SELECT COUNT(*) AS count, MAX(rowid) AS maxRowid FROM events INDEXED BY events_thread_type_sequence_idx WHERE thread_id = ? AND type NOT IN (${PRUNABLE_EVENT_TYPES.map(() => "?").join(", ")}) AND sequence < ?`,
    )
    .get(threadId, ...PRUNABLE_EVENT_TYPES, sequence);
  return `${readCount(row, "count")}:${readCount(row, "maxRowid") ?? 0}`;
}

function reconcilePrunedDeltas(
  db: DbConnection,
  threadId: string,
  contextBoundarySeq: number,
  entry: Entry,
): boolean {
  const row: unknown = db.$client
    .prepare(
      "SELECT COUNT(*) AS count FROM events INDEXED BY events_thread_type_sequence_idx WHERE thread_id = ? AND type = 'item/agentMessage/delta' AND sequence >= ? AND sequence < ?",
    )
    .get(threadId, contextBoundarySeq, entry.checkpoint.sequenceStart);
  const remaining = readCount(row, "count") ?? 0;
  const removed = entry.checkpoint.agentMessageDeltaCount - remaining;
  const total = entry.agentMessageDeltaCount - removed;
  if (
    removed < 0 ||
    entry.agentMessageDeltaCount >= MIN_AGENT_MESSAGE_DELTAS_FOR_SUMMARY_COMPACTION !==
      total >= MIN_AGENT_MESSAGE_DELTAS_FOR_SUMMARY_COMPACTION
  )
    return false;
  entry.checkpoint.agentMessageDeltaCount = remaining;
  entry.agentMessageDeltaCount = total;
  return true;
}

interface ItemReferences {
  own: string[];
  parents: string[];
}

function itemReferences(event: ThreadEventWithMeta["event"]): ItemReferences {
  const own: string[] = [];
  const parents: string[] = [];
  if ("itemId" in event && typeof event.itemId === "string") own.push(event.itemId);
  if ("parentToolCallId" in event && typeof event.parentToolCallId === "string")
    parents.push(event.parentToolCallId);
  if ("item" in event && typeof event.item === "object" && event.item !== null) {
    if ("id" in event.item && typeof event.item.id === "string")
      own.push(event.item.id);
    if (
      "parentToolCallId" in event.item &&
      typeof event.item.parentToolCallId === "string"
    )
      parents.push(event.item.parentToolCallId);
  }
  return { own, parents };
}

function referencedRequestId(
  event: ThreadEventWithMeta["event"],
): string | null {
  if (
    event.type === "client/turn/requested" ||
    event.type === "client/turn/rejected"
  ) {
    return event.requestId;
  }
  return event.type === "turn/input/accepted" ? event.clientRequestId : null;
}

function isThreadError(event: ThreadEventWithMeta["event"]): boolean {
  return (
    event.scope.kind === "thread" &&
    (event.type === "system/error" ||
      event.type === "provider/error" ||
      event.type === "system/thread/interrupted")
  );
}

function canReuse(
  checkpoint: Checkpoint,
  events: ThreadEventWithMeta[],
): boolean {
  let hasTailTurn = false;
  return events.every(({ event }) => {
    const references = itemReferences(event);
    if (
      references.own.some((id) => checkpoint.itemIds.has(id)) ||
      references.parents.some((id) => checkpoint.itemIds.has(id))
    )
      return false;
    if (event.type === "turn/started") hasTailTurn = true;
    if (isThreadError(event) && !hasTailTurn) return false;
    if (
      event.scope.kind === "turn" &&
      checkpoint.turnIds.has(event.scope.turnId)
    )
      return false;
    const requestId = referencedRequestId(event);
    if (requestId !== null && checkpoint.requestIds.has(requestId))
      return false;
    if (
      event.type === "client/turn/requested" &&
      "expectedTurnId" in event.target
    ) {
      const turnId = event.target.expectedTurnId;
      if (turnId !== null && checkpoint.turnIds.has(turnId)) return false;
    }
    return true;
  });
}

function sortBySource(
  items: ConversationOutlineProjection["items"],
): ConversationOutlineProjection["items"] {
  return items
    .map((row, index) => ({ row, index }))
    .sort(
      (left, right) =>
        left.row.sourceSeqStart - right.row.sourceSeqStart ||
        left.row.sourceSeqEnd - right.row.sourceSeqEnd ||
        left.index - right.index,
    )
    .map(({ row }) => row);
}

function latestValidBoundary(
  projection: ConversationOutlineProjection,
  previous: Checkpoint,
): number {
  const firstOwned = new Map<string, number>();
  const lastReferenced = new Map<string, number>();
  const reference = (key: string, seq: number, owned: boolean) => {
    if (owned && !firstOwned.has(key)) firstOwned.set(key, seq);
    lastReferenced.set(key, Math.max(lastReferenced.get(key) ?? seq, seq));
  };
  const requestedAt = new Map<string, number>();
  const resolvedAt = new Map<string, number>();
  const pendingOrder: string[] = [];
  let rejectionAwaitsCompanionError = false;
  const turnStartedAt = new Map<string, number>();
  const completedTurns: { seq: number; turnId: string }[] = [];
  const threadErrors: number[] = [];
  const turnStarts: number[] = [];
  let lastLifecycle = -1;
  for (const { event, meta } of projection.events) {
    const seq = meta.seq;
    const references = itemReferences(event);
    for (const id of references.own) reference(`item:${id}`, seq, true);
    for (const id of references.parents) reference(`item:${id}`, seq, false);
    if (event.scope.kind === "turn") {
      reference(`turn:${event.scope.turnId}`, seq, true);
      if (!turnStartedAt.has(event.scope.turnId))
        turnStartedAt.set(event.scope.turnId, seq);
      if (event.type === "turn/completed")
        completedTurns.push({ seq, turnId: event.scope.turnId });
    }
    if (
      event.type === "client/turn/requested" &&
      "expectedTurnId" in event.target &&
      event.target.expectedTurnId !== null
    )
      reference(`turn:${event.target.expectedTurnId}`, seq, false);
    const requestId = referencedRequestId(event);
    if (requestId !== null) reference(`request:${requestId}`, seq, true);
    if (
      event.type === "client/turn/requested" &&
      (event.target.kind === "steer" ||
        (event.target.kind === "auto" && event.target.expectedTurnId !== null))
    ) {
      requestedAt.set(event.requestId, seq);
      pendingOrder.push(event.requestId);
    }
    if (event.type === "turn/input/accepted" && !resolvedAt.has(event.clientRequestId))
      resolvedAt.set(event.clientRequestId, seq);
    if (event.type === "client/turn/rejected" && !resolvedAt.has(event.requestId))
      resolvedAt.set(event.requestId, seq);
    if (
      event.type === "system/error" &&
      event.code === "thread_command_failed" &&
      !rejectionAwaitsCompanionError
    ) {
      let pending = pendingOrder.pop();
      while (pending !== undefined && resolvedAt.has(pending))
        pending = pendingOrder.pop();
      if (pending !== undefined) resolvedAt.set(pending, seq);
    }
    if (event.type !== "item/agentMessage/delta")
      rejectionAwaitsCompanionError = event.type === "client/turn/rejected";
    if (isThreadError(event)) threadErrors.push(seq);
    if (event.type === "turn/started") turnStarts.push(seq);
    if (
      event.type === "system/interaction/lifecycle" &&
      event.scope.kind === "thread"
    )
      lastLifecycle = seq;
  }
  const end = (projection.events.at(-1)?.meta.seq ?? previous.sequenceStart) + 1;
  const blocked = new Map<number, number>();
  const block = (after: number, through: number) => {
    if (through <= after) return;
    blocked.set(after + 1, (blocked.get(after + 1) ?? 0) + 1);
    blocked.set(through + 1, (blocked.get(through + 1) ?? 0) - 1);
  };
  for (const [key, first] of firstOwned) block(first, lastReferenced.get(key)!);
  for (const [requestId, seq] of requestedAt)
    block(seq, resolvedAt.get(requestId) ?? end);
  const completedTurnIds = new Set(completedTurns.map(({ turnId }) => turnId));
  for (const [turnId, seq] of turnStartedAt) {
    if (completedTurnIds.has(turnId)) continue;
    const quietSince = lastReferenced.get(`turn:${turnId}`)!;
    const superseded = completedTurns.find(
      (completion) =>
        completion.seq > quietSince &&
        turnStartedAt.get(completion.turnId)! > quietSince,
    );
    block(seq, Math.max(lastLifecycle, superseded?.seq ?? end));
  }
  for (const { sourceSeqStart, sourceSeqEnd } of projection.items)
    block(sourceSeqStart, sourceSeqEnd);
  const changes = [...blocked.keys()].sort((left, right) => left - right);
  let depth = 0;
  let change = 0;
  let nextError = 0;
  let nextStart = 0;
  let boundary = previous.sequenceStart;
  for (const { meta } of projection.events) {
    const seq = meta.seq;
    while (change < changes.length && changes[change]! <= seq) {
      depth += blocked.get(changes[change]!)!;
      change += 1;
    }
    while (nextError < threadErrors.length && threadErrors[nextError]! < seq)
      nextError += 1;
    while (nextStart < turnStarts.length && turnStarts[nextStart]! < seq)
      nextStart += 1;
    if (
      depth === 0 &&
      seq > boundary &&
      (threadErrors[nextError] ?? Infinity) >= (turnStarts[nextStart] ?? Infinity)
    )
      boundary = seq;
  }
  return boundary;
}

function nextCheckpoint(
  projection: ConversationOutlineProjection,
  previous: Checkpoint,
): Checkpoint {
  const boundary = latestValidBoundary(projection, previous);
  if (boundary <= previous.sequenceStart) return previous;
  const turnIds = new Set(previous.turnIds);
  const requestIds = new Set(previous.requestIds);
  const itemIds = new Set(previous.itemIds);
  for (const { event, meta } of projection.events) {
    if (meta.seq >= boundary) continue;
    if (event.scope.kind === "turn") turnIds.add(event.scope.turnId);
    const requestId = referencedRequestId(event);
    if (requestId !== null) requestIds.add(requestId);
    for (const id of itemReferences(event).own) itemIds.add(id);
  }
  const checkpoint: Checkpoint = {
    agentMessageDeltaCount:
      previous.agentMessageDeltaCount +
      projection.events.filter(
        ({ event, meta }) =>
          meta.seq < boundary && event.type === "item/agentMessage/delta",
      ).length,
    items: previous.items,
    trailingItems: previous.trailingItems,
    sequenceStart: boundary,
    turnIds,
    requestIds,
    itemIds,
    prefixFingerprint: "",
  };
  const tailEvents = projection.events.filter(
    ({ meta }) => meta.seq >= boundary,
  );
  if (!canReuse(checkpoint, tailEvents)) return previous;
  if (
    projection.items.some(
      ({ sourceSeqStart, sourceSeqEnd }) =>
        sourceSeqStart < boundary && sourceSeqEnd >= boundary,
    )
  )
    return previous;
  const frozen = { main: [...previous.items], trailing: [...previous.trailingItems] };
  const reachedTail = { main: false, trailing: false };
  for (const row of projection.items) {
    const group = row.trailing ? "trailing" : "main";
    if (row.sourceSeqStart >= boundary) reachedTail[group] = true;
    else {
      if (reachedTail[group]) return previous;
      frozen[group].push(row.item);
    }
  }
  checkpoint.items = frozen.main;
  checkpoint.trailingItems = frozen.trailing;
  return checkpoint;
}

export function projectConversationOutlineIncrementally(args: {
  db: DbConnection;
  threadId: string;
  key: string;
  maxSeq: number;
  contextBoundarySeq: number;
  resolveExternalUserBoundarySequence: () => number | null;
  select: (
    sequenceStart: number,
    precedingAgentMessageDeltaCount: number,
  ) => ConversationOutlineSelection;
}): ThreadConversationOutlineItem[] {
  let cache = caches.get(args.db);
  if (cache === undefined) {
    cache = { entries: new Map(), chars: 0 };
    caches.set(args.db, cache);
  }
  const dataVersion = getDatabaseDataVersion(args.db);
  const generation = getThreadEventRewriteGeneration(args.threadId);
  const structuralGeneration = getThreadEventStructuralGeneration(args.threadId);
  const entry = cache.entries.get(args.threadId);
  if (entry !== undefined) {
    cache.entries.delete(args.threadId);
    cache.chars -= entry.chars;
  }
  const empty: Checkpoint = {
    agentMessageDeltaCount: 0,
    items: [],
    trailingItems: [],
    sequenceStart: args.contextBoundarySeq,
    turnIds: new Set(),
    requestIds: new Set(),
    itemIds: new Set(),
    prefixFingerprint: "",
  };
  const reusable =
    entry !== undefined &&
    entry.key === args.key &&
    (!requireUnchangedDataVersion || entry.dataVersion === dataVersion) &&
    entry.contextBoundarySeq === args.contextBoundarySeq &&
    entry.structuralGeneration === structuralGeneration &&
    entry.maxSeq <= args.maxSeq &&
    (entry.generation === generation ||
      (prefixFingerprint(args.db, args.threadId, entry.checkpoint.sequenceStart) ===
        entry.checkpoint.prefixFingerprint &&
        reconcilePrunedDeltas(
          args.db,
          args.threadId,
          args.contextBoundarySeq,
          entry,
        )));
  const externalUserBoundarySequence =
    reusable && entry.externalUserBoundarySequence !== null
      ? entry.externalUserBoundarySequence
      : args.resolveExternalUserBoundarySequence();
  let checkpoint =
    reusable && entry.externalUserBoundarySequence === externalUserBoundarySequence
      ? entry.checkpoint
      : empty;
  let selection = args.select(
    checkpoint.sequenceStart,
    checkpoint.agentMessageDeltaCount,
  );
  let agentMessageDeltaCount =
    checkpoint.agentMessageDeltaCount +
    selection.events.filter(
      ({ event }) => event.type === "item/agentMessage/delta",
    ).length;
  const crossedCompactionThreshold =
    entry !== undefined &&
    entry.agentMessageDeltaCount <
      MIN_AGENT_MESSAGE_DELTAS_FOR_SUMMARY_COMPACTION &&
    agentMessageDeltaCount >= MIN_AGENT_MESSAGE_DELTAS_FOR_SUMMARY_COMPACTION;
  if (
    checkpoint !== empty &&
    (crossedCompactionThreshold || !canReuse(checkpoint, selection.events))
  ) {
    checkpoint = empty;
    selection = args.select(checkpoint.sequenceStart, 0);
    agentMessageDeltaCount = selection.events.filter(
      ({ event }) => event.type === "item/agentMessage/delta",
    ).length;
  }
  const projected = selection.project();
  const main = projected.filter(({ trailing }) => !trailing);
  const trailing = projected.filter(({ trailing }) => trailing);
  const projection = {
    events: selection.events,
    items: [
      ...(checkpoint !== empty &&
      externalUserBoundarySequence !== null &&
      externalUserBoundarySequence <= checkpoint.sequenceStart
        ? sortBySource(main)
        : main),
      ...trailing,
    ],
  };
  let items = [
    ...checkpoint.items,
    ...projection.items
      .filter(({ trailing }) => !trailing)
      .map(({ item }) => item),
    ...checkpoint.trailingItems,
    ...trailing.map(({ item }) => item),
  ];
  let next = nextCheckpoint(projection, checkpoint);
  if (
    checkpoint !== empty &&
    Math.random() < (audits < auditBurst ? 1 : auditRate)
  ) {
    const auditedItems = items;
    const checkpointStart = checkpoint.sequenceStart;
    const audit = (deferred: boolean) => {
      const fullItems = args
        .select(args.contextBoundarySeq, 0)
        .project()
        .map(({ item }) => item);
      audits += 1;
      const mismatch =
        JSON.stringify(fullItems) !== JSON.stringify(auditedItems);
      if (mismatch) {
        auditMismatches += 1;
        let firstDiff = 0;
        while (
          firstDiff < auditedItems.length &&
          firstDiff < fullItems.length &&
          JSON.stringify(auditedItems[firstDiff]) ===
            JSON.stringify(fullItems[firstDiff])
        )
          firstDiff += 1;
        auditReporter?.mismatch({
          threadId: args.threadId,
          deferred,
          checkpointStart,
          incrementalItems: auditedItems.length,
          fullItems: fullItems.length,
          firstDiff,
        });
      }
      if (audits % AUDIT_SUMMARY_INTERVAL === 0)
        auditReporter?.summary({ audits, mismatches: auditMismatches });
      return { mismatch, fullItems };
    };
    if (deferAudits) {
      const auditCache = cache;
      deferredAudits.push(() => {
        if (!audit(true).mismatch) return null;
        const stale = auditCache.entries.get(args.threadId);
        if (stale !== undefined) {
          auditCache.entries.delete(args.threadId);
          auditCache.chars -= stale.chars;
        }
        return args.threadId;
      });
    } else {
      const result = audit(false);
      if (result.mismatch) {
        items = result.fullItems;
        next = empty;
      }
    }
  }
  if (next !== checkpoint && next !== empty && next.prefixFingerprint === "") {
    next.prefixFingerprint = prefixFingerprint(
      args.db,
      args.threadId,
      next.sequenceStart,
    );
  }
  if (next.sequenceStart > args.contextBoundarySeq) {
    const chars =
      next === entry?.checkpoint
        ? entry.chars
        : JSON.stringify(next.items).length +
          JSON.stringify(next.trailingItems).length +
          [...next.turnIds, ...next.requestIds, ...next.itemIds].reduce(
            (sum, id) => sum + id.length,
            0,
          );
    if (chars <= maxChars) {
      cache.entries.set(args.threadId, {
        agentMessageDeltaCount,
        checkpoint: next,
        contextBoundarySeq: args.contextBoundarySeq,
        dataVersion,
        externalUserBoundarySequence,
        generation,
        structuralGeneration,
        key: args.key,
        maxSeq: args.maxSeq,
        chars,
      });
      cache.chars += chars;
    }
  }
  while (cache.entries.size > maxEntries || cache.chars > maxChars) {
    const oldest = cache.entries.keys().next().value;
    if (oldest === undefined) break;
    cache.chars -= cache.entries.get(oldest)!.chars;
    cache.entries.delete(oldest);
  }
  return items;
}
