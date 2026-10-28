import { Worker } from "node:worker_threads";
import type { ThreadTimelineResponse } from "@bb/server-contract";
import type {
  CompletedTurnDisplay,
  ProviderComposerCommand,
  Thread,
} from "@bb/domain";
import type { ServerLogger } from "../../types.js";
import type { ThreadTimelineBuildProfile } from "./timeline.js";
import type { ThreadTimelinePageRequest } from "./timeline-pagination.js";

const READ_TIMEOUT_MS = 120_000;
const FAILURES_BEFORE_PAUSE = 3;
const PAUSE_AFTER_FAILURES_MS = 5 * 60_000;

export type OffThreadReadKind = "outline" | "timeline";

export interface OffThreadTimelineOptions {
  completedTurnDisplay: CompletedTurnDisplay;
  deferContent: boolean;
  eventBudget: number;
  includeClearedContextHistory: boolean;
  includeDiagnosticOperations: boolean;
  includeNestedRows: boolean;
  maxSeq: number;
  page: ThreadTimelinePageRequest;
  planCommand: ProviderComposerCommand | null;
  providerDisplayName?: string;
  summaryOnly: boolean;
}

export interface OffThreadTimelineResult {
  full: ThreadTimelineResponse;
  profile: ThreadTimelineBuildProfile;
}

export interface OffThreadOutlineOptions {
  completedTurnDisplay: CompletedTurnDisplay;
  includeClearedContextHistory: boolean;
  providerDisplayName?: string;
}

export type OffThreadReadRequest =
  | {
      kind: "outline";
      threadId: string;
      rewriteGeneration: number;
      structuralGeneration: number;
      outlineOptions: OffThreadOutlineOptions;
    }
  | {
      kind: "timeline";
      threadId: string;
      rewriteGeneration: number;
      structuralGeneration: number;
      thread: Thread;
      options: OffThreadTimelineOptions;
    };

interface OffThreadReadTarget {
  databasePath: string;
  logger: Pick<ServerLogger, "warn">;
  workerUrl?: URL;
  workerExecArgv?: string[];
}

interface PendingRead {
  resolve: (result: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface Reader {
  worker: Worker;
  nextId: number;
  pending: Map<number, PendingRead>;
  inFlight: Map<string, Promise<unknown>>;
  fail: (error: Error) => void;
}

const readers = new Map<string, Reader>();
let consecutiveFailures = 0;
let pausedUntil = 0;

function defaultWorkerUrl(): URL {
  return new URL("./off-thread-read-worker.js", import.meta.url);
}

function getReader(target: OffThreadReadTarget, kind: OffThreadReadKind): Reader {
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
    const inFlightKey = JSON.stringify([request.rewriteGeneration, dedupeKey]);
    let promise = reader.inFlight.get(inFlightKey);
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
      reader.inFlight.set(inFlightKey, promise);
      const settled = promise;
      void settled
        .finally(() => {
          if (reader.inFlight.get(inFlightKey) === settled) {
            reader.inFlight.delete(inFlightKey);
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
        threadId: request.threadId,
      },
      "Off-thread read failed; using the in-process path",
    );
    return null;
  }
}

export async function readConversationOutlineOffThread(
  target: OffThreadReadTarget,
  request: Omit<Extract<OffThreadReadRequest, { kind: "outline" }>, "kind">,
): Promise<string | null> {
  const result = await requestOffThread(
    target,
    { kind: "outline", ...request },
    JSON.stringify([request.threadId, request.outlineOptions]),
  );
  return typeof result === "string" ? result : null;
}

export async function buildThreadTimelineOffThread(
  target: OffThreadReadTarget,
  request: Omit<Extract<OffThreadReadRequest, { kind: "timeline" }>, "kind">,
  cacheKey: string,
): Promise<OffThreadTimelineResult | null> {
  const result = await requestOffThread(
    target,
    { kind: "timeline", ...request },
    cacheKey,
  );
  if (
    typeof result !== "object" ||
    result === null ||
    !("full" in result) ||
    !("profile" in result)
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
