import {
  getDatabaseDataVersion,
  getThreadEventRewriteGeneration,
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
  }[];
}

export interface ConversationOutlineSelection {
  events: ThreadEventWithMeta[];
  project: () => ConversationOutlineProjection["items"];
}

export interface ConversationOutlineProjectionState {
  includeNestedEvents: boolean;
  summaryCompactionEnabled: boolean;
}

interface Checkpoint {
  agentMessageDeltaCount: number;
  items: ThreadConversationOutlineItem[];
  sequenceStart: number;
  turnIds: Set<string>;
  requestIds: Set<string>;
  parentItemIds: Set<string>;
  itemIds: Set<string>;
  eventsBefore: number;
}

interface Entry {
  agentMessageDeltaCount: number;
  checkpoint: Checkpoint;
  historySequenceStart: number;
  dataVersion: number;
  generation: number;
  key: string;
  maxSeq: number;
  chars: number;
  projectionState: ConversationOutlineProjectionState;
}

interface OutlineCache {
  entries: Map<string, Entry>;
  chars: number;
}

const caches = new WeakMap<DbConnection, OutlineCache>();
const DEFAULT_MAX_ENTRIES = 16;
const DEFAULT_MAX_CHARS = 8_000_000;
const AUDIT_SUMMARY_INTERVAL = 50;
let maxEntries = DEFAULT_MAX_ENTRIES;
let maxChars = DEFAULT_MAX_CHARS;
let requireUnchangedDataVersion = true;
let auditRate = 0;
let auditBurst = 0;
let audits = 0;
let auditMismatches = 0;

interface ConversationOutlineAuditReporter {
  mismatch(details: Record<string, unknown>): void;
  summary(details: { audits: number; mismatches: number }): void;
}

let auditReporter: ConversationOutlineAuditReporter | null = null;

export function configureConversationOutlineCache(options: {
  maxEntries: number;
  maxChars: number;
  requireUnchangedDataVersion: boolean;
  auditRate: number;
  auditBurst: number;
  auditReporter: ConversationOutlineAuditReporter | null;
}): void {
  maxEntries = options.maxEntries;
  maxChars = options.maxChars;
  requireUnchangedDataVersion = options.requireUnchangedDataVersion;
  auditRate = options.auditRate;
  auditBurst = options.auditBurst;
  auditReporter = options.auditReporter;
}

function countEventsBefore(
  db: DbConnection,
  threadId: string,
  sequence: number,
): number {
  const row: unknown = db.$client
    .prepare(
      "SELECT COUNT(*) AS count FROM events INDEXED BY events_thread_sequence_idx WHERE thread_id = ? AND sequence < ?",
    )
    .get(threadId, sequence);
  if (
    typeof row !== "object" ||
    row === null ||
    !("count" in row) ||
    typeof row.count !== "number"
  ) {
    throw new Error("Expected an event count");
  }
  return row.count;
}

function ownItemIds(event: ThreadEventWithMeta["event"]): string[] {
  const ids: string[] = [];
  if ("itemId" in event && typeof event.itemId === "string") ids.push(event.itemId);
  if ("item" in event && typeof event.item.id === "string") ids.push(event.item.id);
  return ids;
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

function parentItemId(event: ThreadEventWithMeta["event"]): string | null {
  if ("item" in event && "parentToolCallId" in event.item)
    return event.item.parentToolCallId ?? null;
  return "parentToolCallId" in event ? (event.parentToolCallId ?? null) : null;
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
    if (event.type === "turn/started") hasTailTurn = true;
    if (ownItemIds(event).some((id) => checkpoint.itemIds.has(id)))
      return false;
    const parentId = parentItemId(event);
    if (parentId !== null && checkpoint.parentItemIds.has(parentId))
      return false;
    if (
      "item" in event &&
      event.item.type === "toolCall" &&
      checkpoint.parentItemIds.has(event.item.id)
    )
      return false;
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

function nextCheckpoint(
  projection: ConversationOutlineProjection,
  previous: Checkpoint,
  orderingBoundarySequence: number | null,
): Checkpoint {
  const activeTurns = new Set<string>();
  const pendingRequests = new Set<string>();
  let completedBoundary = previous.sequenceStart;
  let boundary = previous.sequenceStart;
  for (const { event, meta } of projection.events) {
    if (isThreadError(event)) {
      completedBoundary = boundary;
    }
    if (
      event.type === "client/turn/requested" &&
      (event.target.kind === "steer" ||
        (event.target.kind === "auto" && event.target.expectedTurnId !== null))
    )
      pendingRequests.add(event.requestId);
    if (event.type === "turn/input/accepted")
      pendingRequests.delete(event.clientRequestId);
    if (event.type === "client/turn/rejected")
      pendingRequests.delete(event.requestId);
    if (event.scope.kind !== "turn") continue;
    if (event.type === "turn/started") {
      if (
        activeTurns.size === 0 &&
        (orderingBoundarySequence === null ||
          completedBoundary <= orderingBoundarySequence)
      )
        boundary = completedBoundary;
      activeTurns.add(event.scope.turnId);
    }
    if (event.type === "turn/completed") {
      activeTurns.delete(event.scope.turnId);
      if (activeTurns.size === 0 && pendingRequests.size === 0)
        completedBoundary = meta.seq + 1;
    }
  }
  if (boundary <= previous.sequenceStart) return previous;
  const turnIds = new Set(previous.turnIds);
  const requestIds = new Set(previous.requestIds);
  const parentItemIds = new Set(previous.parentItemIds);
  const itemIds = new Set(previous.itemIds);
  for (const { event, meta } of projection.events) {
    if (meta.seq >= boundary) continue;
    for (const id of ownItemIds(event)) itemIds.add(id);
    if (event.scope.kind === "turn") turnIds.add(event.scope.turnId);
    const parentId = parentItemId(event);
    if (parentId !== null) parentItemIds.add(parentId);
    if (
      "item" in event &&
      (event.item.type === "toolCall" ||
        event.item.type === "backgroundTask" ||
        event.item.type === "delegation")
    )
      parentItemIds.add(event.item.id);
    const requestId = referencedRequestId(event);
    if (requestId !== null) requestIds.add(requestId);
  }
  const checkpoint: Checkpoint = {
    agentMessageDeltaCount:
      previous.agentMessageDeltaCount +
      projection.events.filter(
        ({ event, meta }) =>
          meta.seq < boundary && event.type === "item/agentMessage/delta",
      ).length,
    items: previous.items,
    sequenceStart: boundary,
    turnIds,
    requestIds,
    parentItemIds,
    itemIds,
    eventsBefore: -1,
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
  let reachedTail = false;
  const frozen: ThreadConversationOutlineItem[] = [];
  for (const row of projection.items) {
    if (row.sourceSeqStart >= boundary) reachedTail = true;
    else {
      if (reachedTail) return previous;
      frozen.push(row.item);
    }
  }
  checkpoint.items = [...previous.items, ...frozen];
  return checkpoint;
}

export function projectConversationOutlineIncrementally(args: {
  db: DbConnection;
  threadId: string;
  key: string;
  maxSeq: number;
  historySequenceStart: number;
  orderingBoundarySequence: number | null;
  resolveProjectionState: (
    sequenceStart: number,
    previous: ConversationOutlineProjectionState | null,
  ) => ConversationOutlineProjectionState;
  select: (
    sequenceStart: number,
    precedingAgentMessageDeltaCount: number,
    projectionState: ConversationOutlineProjectionState,
  ) => ConversationOutlineSelection;
}): ThreadConversationOutlineItem[] {
  let cache = caches.get(args.db);
  if (cache === undefined) {
    cache = { entries: new Map(), chars: 0 };
    caches.set(args.db, cache);
  }
  const dataVersion = getDatabaseDataVersion(args.db);
  const generation = getThreadEventRewriteGeneration(args.threadId);
  const entry = cache.entries.get(args.threadId);
  if (entry !== undefined) {
    cache.entries.delete(args.threadId);
    cache.chars -= entry.chars;
  }
  const empty: Checkpoint = {
    agentMessageDeltaCount: 0,
    items: [],
    sequenceStart: args.historySequenceStart,
    turnIds: new Set(),
    requestIds: new Set(),
    parentItemIds: new Set(),
    itemIds: new Set(),
    eventsBefore: -1,
  };
  const canReuseEntry =
    entry !== undefined &&
    entry.key === args.key &&
    (!requireUnchangedDataVersion || entry.dataVersion === dataVersion) &&
    entry.generation === generation &&
    entry.historySequenceStart === args.historySequenceStart &&
    (args.orderingBoundarySequence === null ||
      entry.checkpoint.sequenceStart <= args.orderingBoundarySequence) &&
    entry.maxSeq <= args.maxSeq &&
    countEventsBefore(args.db, args.threadId, entry.checkpoint.sequenceStart) ===
      entry.checkpoint.eventsBefore;
  const previousState = canReuseEntry ? entry.projectionState : null;
  const projectionState = args.resolveProjectionState(
    canReuseEntry ? entry.maxSeq + 1 : args.historySequenceStart,
    previousState,
  );
  let checkpoint =
    canReuseEntry &&
    previousState?.includeNestedEvents ===
      projectionState.includeNestedEvents &&
    previousState.summaryCompactionEnabled ===
      projectionState.summaryCompactionEnabled
      ? entry.checkpoint
      : empty;
  let selection = args.select(
    checkpoint.sequenceStart,
    checkpoint.agentMessageDeltaCount,
    projectionState,
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
    selection = args.select(checkpoint.sequenceStart, 0, projectionState);
    agentMessageDeltaCount = selection.events.filter(
      ({ event }) => event.type === "item/agentMessage/delta",
    ).length;
  }
  const projection = { events: selection.events, items: selection.project() };
  let items = [
    ...checkpoint.items,
    ...projection.items.map(({ item }) => item),
  ];
  let next = nextCheckpoint(
    projection,
    checkpoint,
    args.orderingBoundarySequence,
  );
  if (
    checkpoint !== empty &&
    Math.random() < (audits < auditBurst ? 1 : auditRate)
  ) {
    const fullItems = args
      .select(args.historySequenceStart, 0, projectionState)
      .project()
      .map(({ item }) => item);
    audits += 1;
    if (JSON.stringify(fullItems) !== JSON.stringify(items)) {
      auditMismatches += 1;
      let firstDiff = 0;
      while (
        firstDiff < items.length &&
        firstDiff < fullItems.length &&
        JSON.stringify(items[firstDiff]) === JSON.stringify(fullItems[firstDiff])
      )
        firstDiff += 1;
      auditReporter?.mismatch({
        threadId: args.threadId,
        checkpointStart: checkpoint.sequenceStart,
        incrementalItems: items.length,
        fullItems: fullItems.length,
        firstDiff,
      });
      items = fullItems;
      next = empty;
    }
    if (audits % AUDIT_SUMMARY_INTERVAL === 0)
      auditReporter?.summary({ audits, mismatches: auditMismatches });
  }
  if (next !== checkpoint && next !== empty && next.eventsBefore === -1) {
    next.eventsBefore = countEventsBefore(
      args.db,
      args.threadId,
      next.sequenceStart,
    );
  }
  if (next.sequenceStart > args.historySequenceStart) {
    const chars =
      next === entry?.checkpoint
        ? entry.chars
        : JSON.stringify(next.items).length +
          [
            ...next.turnIds,
            ...next.requestIds,
            ...next.parentItemIds,
            ...next.itemIds,
          ].reduce(
            (sum, id) => sum + id.length,
            0,
          );
    if (chars <= maxChars) {
      cache.entries.set(args.threadId, {
        agentMessageDeltaCount,
        checkpoint: next,
        historySequenceStart: args.historySequenceStart,
        dataVersion,
        generation,
        key: args.key,
        maxSeq: args.maxSeq,
        chars,
        projectionState,
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
