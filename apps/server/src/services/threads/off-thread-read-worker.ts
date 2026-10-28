import { parentPort, workerData } from "node:worker_threads";
import { z } from "zod";
import { completedTurnDisplaySchema } from "@bb/domain";
import type { Thread } from "@bb/domain";
import {
  createConnection,
  getLatestStoredConversationOutlineSequence,
  getLatestThreadSequence,
  getThread,
  syncThreadEventRewriteGeneration,
} from "@bb/db";
import type { OffThreadTimelineOptions } from "./off-thread-reads.js";
import { setStoredEventDecodeCacheLimits } from "./stored-event-decode-cache.js";
import {
  configureConversationOutlineCache,
  runDeferredConversationOutlineAudits,
} from "./conversation-outline-cache.js";
import {
  buildThreadTimelineWithProfile,
  loadThreadConversationOutline,
} from "./timeline.js";
import {
  clearTimelineOrderingContextCache,
  configureTimelineOrderingContextCache,
} from "./timeline-context-order.js";
import {
  DEFAULT_MAX_INLINE_OUTPUT_CHARS,
  truncateTimelineResponseOutputs,
} from "./timeline-output-truncation.js";
import { previewTimelineResponseOutputs } from "./timeline-output-preview.js";

const OUTLINE_CACHE_MAX_ENTRIES = 32;
const OUTLINE_PREWARM_INTERVAL_MS = 2_000;
const OUTLINE_PREWARM_WINDOW_MS = 120_000;
const OUTLINE_PREWARM_MAX_THREADS = 8;
const WORKER_DECODE_CACHE_MAX_ENTRIES = 600_000;
const WORKER_DECODE_CACHE_MAX_DATA_CHARS = 400_000_000;
const WORKER_OUTLINE_CHECKPOINT_MAX_ENTRIES = 32;
const WORKER_OUTLINE_CHECKPOINT_MAX_CHARS = 512_000_000;
const WORKER_OUTLINE_AUDIT_RATE = 0.02;
const WORKER_OUTLINE_AUDIT_BURST = 20;

const baseRequestSchema = z.object({
  id: z.number().int(),
  threadId: z.string(),
  rewriteGeneration: z.number().int().nonnegative(),
  structuralGeneration: z.number().int().nonnegative(),
});
const requestSchema = z.discriminatedUnion("kind", [
  baseRequestSchema.extend({
    kind: z.literal("outline"),
    outlineOptions: z.object({
      completedTurnDisplay: completedTurnDisplaySchema,
      includeClearedContextHistory: z.boolean(),
      providerDisplayName: z.string().optional(),
    }),
  }),
  baseRequestSchema.extend({
    kind: z.literal("timeline"),
    thread: z.custom<Thread>(
      (value) =>
        typeof value === "object" &&
        value !== null &&
        "id" in value &&
        typeof value.id === "string",
    ),
    options: z.custom<OffThreadTimelineOptions>(
      (value) =>
        typeof value === "object" &&
        value !== null &&
        "maxSeq" in value &&
        "page" in value,
    ),
  }),
]);
type ReadRequest = z.infer<typeof requestSchema>;

const { databasePath } = z
  .object({ databasePath: z.string() })
  .parse(workerData);
const port = parentPort;
if (port === null) throw new Error("Off-thread read worker requires a parent");

setStoredEventDecodeCacheLimits({
  maxEntries: WORKER_DECODE_CACHE_MAX_ENTRIES,
  maxDataChars: WORKER_DECODE_CACHE_MAX_DATA_CHARS,
});
configureConversationOutlineCache({
  maxEntries: WORKER_OUTLINE_CHECKPOINT_MAX_ENTRIES,
  maxChars: WORKER_OUTLINE_CHECKPOINT_MAX_CHARS,
  requireUnchangedDataVersion: false,
  auditRate: WORKER_OUTLINE_AUDIT_RATE,
  auditBurst: WORKER_OUTLINE_AUDIT_BURST,
  auditReporter: {
    mismatch: (details) =>
      console.warn(`[bb-outline-audit] mismatch ${JSON.stringify(details)}`),
    summary: (details) =>
      console.info(`[bb-outline-audit] summary ${JSON.stringify(details)}`),
  },
  deferAudits: true,
});
configureTimelineOrderingContextCache({ requireUnchangedDataVersion: false });
const db = createConnection(databasePath);
db.$client.pragma("query_only = ON");
const outlineItemsCache = new Map<string, string>();
const seenGenerations = new Map<string, number>();

function syncGeneration(
  threadId: string,
  generation: number,
  structuralGeneration: number,
): void {
  const previous = seenGenerations.get(threadId);
  if (previous !== undefined && previous !== generation) {
    clearTimelineOrderingContextCache(db);
  }
  seenGenerations.set(threadId, generation);
  syncThreadEventRewriteGeneration(threadId, generation, structuralGeneration);
}

function runDeferredAudits(): void {
  let mismatched: string[] = [];
  try {
    mismatched = runDeferredConversationOutlineAudits();
  } catch (error) {
    console.warn(
      `[bb-outline-audit] deferred audit failed ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  for (const threadId of mismatched) {
    for (const key of [...outlineItemsCache.keys()]) {
      const parsed: unknown = JSON.parse(key);
      if (Array.isArray(parsed) && parsed[0] === threadId)
        outlineItemsCache.delete(key);
    }
  }
}

function readOutline(
  request: OutlineRequest,
  reply?: (result: string) => void,
): string {
  return db.$client.transaction(() => {
    const thread = getThread(db, request.threadId);
    if (thread === null || thread.deletedAt !== null) {
      throw new Error("Thread unavailable");
    }
    const maxSeq = getLatestThreadSequence(db, { threadId: thread.id });
    const outlineSequence = getLatestStoredConversationOutlineSequence(db, {
      threadId: thread.id,
    });
    const cacheKey = JSON.stringify([
      thread.id,
      request.rewriteGeneration,
      outlineSequence,
      thread.providerId,
      thread.status,
      thread.title,
      thread.titleFallback,
      request.outlineOptions,
    ]);
    let itemsJson = outlineItemsCache.get(cacheKey);
    if (itemsJson === undefined) {
      const response = loadThreadConversationOutline(db, thread, {
        ...request.outlineOptions,
        maxSeq,
        outlineSequence,
      });
      itemsJson = JSON.stringify(response.items);
    } else {
      outlineItemsCache.delete(cacheKey);
    }
    outlineItemsCache.set(cacheKey, itemsJson);
    while (outlineItemsCache.size > OUTLINE_CACHE_MAX_ENTRIES) {
      const oldest = outlineItemsCache.keys().next().value;
      if (oldest === undefined) break;
      outlineItemsCache.delete(oldest);
    }
    const result = `{"items":${itemsJson},"maxSeq":${JSON.stringify(maxSeq)}}`;
    reply?.(result);
    runDeferredAudits();
    return result;
  })();
}

function buildTimeline(request: Extract<ReadRequest, { kind: "timeline" }>) {
  const { options } = request;
  const { profile, response } = buildThreadTimelineWithProfile(
    db,
    request.thread,
    { ...options, maxInlineOutputChars: DEFAULT_MAX_INLINE_OUTPUT_CHARS },
  );
  const truncated = truncateTimelineResponseOutputs(
    response,
    DEFAULT_MAX_INLINE_OUTPUT_CHARS,
  );
  return {
    full: options.includeNestedRows
      ? truncated
      : previewTimelineResponseOutputs(truncated),
    profile,
  };
}

type OutlineRequest = Extract<ReadRequest, { kind: "outline" }>;
const recentOutlineRequests = new Map<
  string,
  { request: OutlineRequest; at: number }
>();

function rememberOutlineRequest(request: OutlineRequest): void {
  const key = JSON.stringify([request.threadId, request.outlineOptions]);
  recentOutlineRequests.delete(key);
  recentOutlineRequests.set(key, { request, at: Date.now() });
  while (recentOutlineRequests.size > OUTLINE_PREWARM_MAX_THREADS) {
    const oldest = recentOutlineRequests.keys().next().value;
    if (oldest === undefined) break;
    recentOutlineRequests.delete(oldest);
  }
}

setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of recentOutlineRequests) {
    if (now - entry.at > OUTLINE_PREWARM_WINDOW_MS) {
      recentOutlineRequests.delete(key);
      continue;
    }
    try {
      readOutline(entry.request);
    } catch {
      recentOutlineRequests.delete(key);
    }
  }
}, OUTLINE_PREWARM_INTERVAL_MS);

port.on("message", (message: unknown) => {
  const parsed = requestSchema.safeParse(message);
  if (!parsed.success) {
    if (
      typeof message === "object" &&
      message !== null &&
      "id" in message &&
      typeof message.id === "number"
    ) {
      port.postMessage({ id: message.id, error: "Invalid off-thread read request" });
    }
    return;
  }
  const request = parsed.data;
  let replied = false;
  try {
    syncGeneration(
      request.threadId,
      request.rewriteGeneration,
      request.structuralGeneration,
    );
    if (request.kind === "timeline") {
      port.postMessage({ id: request.id, result: buildTimeline(request) });
      return;
    }
    rememberOutlineRequest(request);
    readOutline(request, (result) => {
      replied = true;
      port.postMessage({ id: request.id, result });
    });
  } catch (error) {
    if (replied) return;
    port.postMessage({
      id: request.id,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});
