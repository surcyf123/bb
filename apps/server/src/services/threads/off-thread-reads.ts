import { Worker } from "node:worker_threads";
import type { Thread } from "@bb/domain";
import type { ThreadConversationOutlineResponse } from "@bb/server-contract";
import type { ServerLogger } from "../../types.js";
import type {
  BuildThreadTimelineOptions,
  ThreadTimelineWindow,
} from "./timeline.js";

const READ_TIMEOUT_MS = 180_000;
const FAILURES_BEFORE_PAUSE = 3;
const PAUSE_AFTER_FAILURES_MS = 5 * 60_000;

export type OffThreadReadKind = "outline" | "timeline";

export type OffThreadTimelineOptions = BuildThreadTimelineOptions & {
  maxInlineOutputChars: number;
};

export interface OffThreadTimelineResult {
  expiresAt: number | null;
  window: ThreadTimelineWindow;
}

export interface OffThreadOutlineOptions {
  completedTurnDisplay: BuildThreadTimelineOptions["completedTurnDisplay"];
  includeClearedContextHistory: boolean;
  maxSeq: number;
  outlineSequence: number;
  providerDisplayName?: string;
}

export interface OffThreadHistoryState {
  compactions: number;
  rewriteGeneration: number;
  rewrites: number;
}

export type OffThreadReadRequest =
  | {
      kind: "outline";
      history: OffThreadHistoryState;
      options: OffThreadOutlineOptions;
      thread: Thread;
    }
  | {
      kind: "timeline";
      history: OffThreadHistoryState;
      options: OffThreadTimelineOptions;
      thread: Thread;
    };

export interface OffThreadReadTarget {
  databasePath: string;
  logger: Pick<ServerLogger, "warn">;
  workerExecArgv?: string[];
  workerUrl?: URL;
}

interface PendingRead {
  reject: (error: Error) => void;
  resolve: (result: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface Reader {
  fail: (error: Error) => void;
  inFlight: Map<string, Promise<unknown>>;
  nextId: number;
  pending: Map<number, PendingRead>;
  worker: Worker;
}

const readers = new Map<string, Reader>();
const historyCountsByThreadId = new Map<
  string,
  { compactions: number; rewrites: number }
>();
let consecutiveFailures = 0;
let pausedUntil = 0;

export function noteOffThreadHistoryChange(
  threadId: string,
  change: "compacted" | "rewritten",
): void {
  const counts = historyCountsByThreadId.get(threadId) ?? {
    compactions: 0,
    rewrites: 0,
  };
  if (change === "rewritten") counts.rewrites += 1;
  else counts.compactions += 1;
  historyCountsByThreadId.set(threadId, counts);
}

export function forgetOffThreadHistory(threadId: string): void {
  historyCountsByThreadId.delete(threadId);
}

export function offThreadHistoryState(
  threadId: string,
  rewriteGeneration: number,
): OffThreadHistoryState {
  const counts = historyCountsByThreadId.get(threadId);
  return {
    compactions: counts?.compactions ?? 0,
    rewriteGeneration,
    rewrites: counts?.rewrites ?? 0,
  };
}

export function resolveOffThreadReadTarget(deps: {
  db: { $client: { memory: boolean; name: string } };
  logger: Pick<ServerLogger, "warn">;
}): OffThreadReadTarget | null {
  return deps.db.$client.memory
    ? null
    : { databasePath: deps.db.$client.name, logger: deps.logger };
}

function defaultWorkerUrl(): URL {
  return new URL("./off-thread-read-worker.js", import.meta.url);
}

function getReader(
  target: OffThreadReadTarget,
  kind: OffThreadReadKind,
): Reader {
  const workerUrl = target.workerUrl ?? defaultWorkerUrl();
  const key = `${kind}\0${target.databasePath}\0${workerUrl.href}`;
  const existing = readers.get(key);
  if (existing !== undefined) return existing;
  const worker = new Worker(workerUrl, {
    workerData: { databasePath: target.databasePath },
    execArgv: target.workerExecArgv ?? [],
  });
  worker.unref();
  const reader: Reader = {
    worker,
    nextId: 0,
    pending: new Map(),
    inFlight: new Map(),
    fail: (error) => {
      if (readers.get(key) === reader) readers.delete(key);
      for (const pending of reader.pending.values()) {
        clearTimeout(pending.timer);
        pending.reject(error);
      }
      reader.pending.clear();
      reader.inFlight.clear();
    },
  };
  readers.set(key, reader);
  worker.on("error", (error) => reader.fail(error));
  worker.on("exit", () => reader.fail(new Error(`${kind} read worker exited`)));
  worker.on("message", (message: unknown) => {
    if (
      typeof message !== "object" ||
      message === null ||
      !("id" in message) ||
      typeof message.id !== "number"
    ) {
      return;
    }
    const pending = reader.pending.get(message.id);
    if (pending === undefined) return;
    reader.pending.delete(message.id);
    clearTimeout(pending.timer);
    if ("error" in message) pending.reject(new Error(String(message.error)));
    else if ("result" in message) pending.resolve(message.result);
    else pending.reject(new Error(`${kind} read worker sent no result`));
  });
  return reader;
}

async function requestOffThread(
  target: OffThreadReadTarget,
  request: OffThreadReadRequest,
  dedupeKey: string,
): Promise<unknown> {
  if (Date.now() < pausedUntil) return null;
  try {
    const reader = getReader(target, request.kind);
    let promise = reader.inFlight.get(dedupeKey);
    if (promise === undefined) {
      const id = ++reader.nextId;
      promise = new Promise<unknown>((resolve, reject) => {
        const timer = setTimeout(() => {
          reader.fail(new Error(`${request.kind} read worker timed out`));
          void reader.worker.terminate();
        }, READ_TIMEOUT_MS);
        reader.pending.set(id, { resolve, reject, timer });
        reader.worker.postMessage({ id, ...request });
      });
      reader.inFlight.set(dedupeKey, promise);
      const settled = promise;
      void settled
        .finally(() => {
          if (reader.inFlight.get(dedupeKey) === settled) {
            reader.inFlight.delete(dedupeKey);
          }
        })
        .catch(() => {});
    }
    const result = await promise;
    consecutiveFailures = 0;
    return result;
  } catch (error) {
    consecutiveFailures += 1;
    if (consecutiveFailures >= FAILURES_BEFORE_PAUSE) {
      pausedUntil = Date.now() + PAUSE_AFTER_FAILURES_MS;
      consecutiveFailures = 0;
    }
    target.logger.warn(
      {
        err: error instanceof Error ? error.message : String(error),
        kind: request.kind,
        threadId: request.thread.id,
      },
      "Off-thread read failed; using the in-process path",
    );
    return null;
  }
}

export async function loadConversationOutlineOffThread(
  target: OffThreadReadTarget,
  request: Omit<Extract<OffThreadReadRequest, { kind: "outline" }>, "kind">,
  dedupeKey: string,
): Promise<ThreadConversationOutlineResponse["items"] | null> {
  const result = await requestOffThread(
    target,
    { kind: "outline", ...request },
    dedupeKey,
  );
  return Array.isArray(result)
    ? (result as ThreadConversationOutlineResponse["items"])
    : null;
}

export async function buildThreadTimelineOffThread(
  target: OffThreadReadTarget,
  request: Omit<Extract<OffThreadReadRequest, { kind: "timeline" }>, "kind">,
  dedupeKey: string,
): Promise<OffThreadTimelineResult | null> {
  const result = await requestOffThread(
    target,
    { kind: "timeline", ...request },
    dedupeKey,
  );
  if (
    typeof result !== "object" ||
    result === null ||
    !("window" in result) ||
    !("expiresAt" in result)
  ) {
    return null;
  }
  return result as OffThreadTimelineResult;
}

export async function closeOffThreadReaders(): Promise<void> {
  const active = [...readers.values()];
  readers.clear();
  consecutiveFailures = 0;
  pausedUntil = 0;
  await Promise.all(active.map(({ worker }) => worker.terminate()));
}
