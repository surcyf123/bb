import { parentPort, workerData } from "node:worker_threads";
import { z } from "zod";
import type { Thread } from "@bb/domain";
import {
  createConnection,
  pinDatabaseDataVersion,
  syncThreadEventRewriteGeneration,
} from "@bb/db";
import type {
  OffThreadOutlineOptions,
  OffThreadTimelineOptions,
} from "./off-thread-reads.js";
import {
  buildThreadTimelineWithProfile,
  loadThreadConversationOutline,
} from "./timeline.js";
import { forgetTimelineGroupingContexts } from "./timeline-context-order.js";
import { shortenTimelineResponseOutputs } from "./timeline-output-shortening.js";
import { forgetTimelineProjectionFolds } from "./timeline-projection-fold.js";
import { forgetLatestTimelineSelections } from "./timeline-selection-memo.js";

const historySchema = z.object({
  compactions: z.number().int().nonnegative(),
  rewriteGeneration: z.number().int().nonnegative(),
  rewrites: z.number().int().nonnegative(),
});
const threadSchema = z.custom<Thread>(
  (value) =>
    typeof value === "object" &&
    value !== null &&
    "id" in value &&
    typeof value.id === "string",
);
const requestSchema = z.discriminatedUnion("kind", [
  z.object({
    id: z.number().int(),
    kind: z.literal("outline"),
    history: historySchema,
    thread: threadSchema,
    options: z.custom<OffThreadOutlineOptions>(
      (value) =>
        typeof value === "object" &&
        value !== null &&
        "maxSeq" in value &&
        "outlineSequence" in value,
    ),
  }),
  z.object({
    id: z.number().int(),
    kind: z.literal("timeline"),
    history: historySchema,
    thread: threadSchema,
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

pinDatabaseDataVersion();
const db = createConnection(databasePath);
const seenHistory = new Map<string, { compactions: number; rewrites: number }>();

function syncHistory(request: ReadRequest): void {
  const threadId = request.thread.id;
  const { compactions, rewriteGeneration, rewrites } = request.history;
  const seen = seenHistory.get(threadId);
  if (seen !== undefined && seen.rewrites !== rewrites) {
    forgetTimelineGroupingContexts(db, threadId);
  }
  if (
    seen !== undefined &&
    (seen.rewrites !== rewrites || seen.compactions !== compactions)
  ) {
    forgetLatestTimelineSelections(db, threadId);
    forgetTimelineProjectionFolds(db, threadId);
  }
  seenHistory.set(threadId, { compactions, rewrites });
  syncThreadEventRewriteGeneration(threadId, rewriteGeneration);
}

function buildTimeline(request: Extract<ReadRequest, { kind: "timeline" }>) {
  const { options } = request;
  const { expiresAt, response } = buildThreadTimelineWithProfile(
    db,
    request.thread,
    options,
  );
  return {
    expiresAt,
    window: shortenTimelineResponseOutputs(response, {
      maxInlineOutputChars: options.maxInlineOutputChars,
      preview: options.includeNestedRows !== true,
    }),
  };
}

port.on("message", (message: unknown) => {
  const parsed = requestSchema.safeParse(message);
  if (!parsed.success) {
    if (
      typeof message === "object" &&
      message !== null &&
      "id" in message &&
      typeof message.id === "number"
    ) {
      port.postMessage({
        id: message.id,
        error: "Invalid off-thread read request",
      });
    }
    return;
  }
  const request = parsed.data;
  try {
    syncHistory(request);
    port.postMessage({
      id: request.id,
      result:
        request.kind === "timeline"
          ? buildTimeline(request)
          : loadThreadConversationOutline(db, request.thread, request.options)
              .items,
    });
  } catch (error) {
    port.postMessage({
      id: request.id,
      error: error instanceof Error ? error.message : String(error),
    });
  }
});
