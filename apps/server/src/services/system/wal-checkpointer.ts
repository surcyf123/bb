import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";
import { z } from "zod";
import type { DbConnection } from "@bb/db";
import type { ServerLogger } from "../../types.js";

const CHECKPOINT_INTERVAL_MS = 1_000;
const CHECKPOINT_STALE_MS = 60_000;
const SLOW_CHECKPOINT_MS = 1_000;
const DEFAULT_WAL_AUTOCHECKPOINT_PAGES = 1_000;
const MAX_WATCHDOG_INTERVAL_MS = 10_000;

const workerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const Database = require(workerData.modulePath);
const db = new Database(workerData.filename, { fileMustExist: true, timeout: 0 });
setInterval(() => {
  const started = Date.now();
  try {
    const row = db.pragma("wal_checkpoint(PASSIVE)")[0] ?? {};
    parentPort.postMessage({ ok: true, durationMs: Date.now() - started, busy: row.busy, log: row.log, checkpointed: row.checkpointed });
  } catch (error) {
    parentPort.postMessage({ ok: false, durationMs: Date.now() - started, error: String(error && error.message || error) });
  }
}, workerData.intervalMs);
`;

const checkpointResultSchema = z.object({
  ok: z.boolean(),
  durationMs: z.number(),
  busy: z.number().optional(),
  log: z.number().optional(),
  checkpointed: z.number().optional(),
  error: z.string().optional(),
});

interface WalCheckpointerOptions {
  intervalMs?: number;
  staleMs?: number;
}

export interface WalCheckpointer {
  stop(): void;
  readonly stopped: boolean;
}

export function startBackgroundWalCheckpointer(
  db: DbConnection,
  logger: Pick<ServerLogger, "info" | "warn">,
  options: WalCheckpointerOptions = {},
): WalCheckpointer | null {
  const client = db.$client;
  if (client.memory) return null;
  const intervalMs = options.intervalMs ?? CHECKPOINT_INTERVAL_MS;
  const staleMs = options.staleMs ?? CHECKPOINT_STALE_MS;
  let worker: Worker;
  try {
    worker = new Worker(workerSource, {
      eval: true,
      execArgv: [],
      workerData: {
        filename: client.name,
        modulePath: createRequire(import.meta.url).resolve("better-sqlite3"),
        intervalMs,
      },
    });
  } catch (error) {
    logger.warn(
      { err: error instanceof Error ? error.message : String(error) },
      "Background WAL checkpointer could not start; keeping automatic checkpoints",
    );
    return null;
  }
  worker.unref();
  client.pragma("wal_autocheckpoint = 0");

  let lastOkAt = Date.now();
  let stopped = false;
  let watchdog: ReturnType<typeof setInterval> | undefined;
  const stop = (reason: string | null) => {
    if (stopped) return;
    stopped = true;
    clearInterval(watchdog);
    if (client.open) {
      client.pragma(`wal_autocheckpoint = ${DEFAULT_WAL_AUTOCHECKPOINT_PAGES}`);
    }
    void worker.terminate();
    if (reason !== null) {
      logger.warn(
        { reason },
        "Background WAL checkpointer stopped; restored automatic checkpoints",
      );
    }
  };

  worker.on("message", (message: unknown) => {
    const result = checkpointResultSchema.safeParse(message);
    if (!result.success || !result.data.ok) return;
    lastOkAt = Date.now();
    if (result.data.durationMs >= SLOW_CHECKPOINT_MS) {
      logger.info(
        {
          durationMs: result.data.durationMs,
          log: result.data.log,
          checkpointed: result.data.checkpointed,
          busy: result.data.busy,
        },
        "Slow background WAL checkpoint",
      );
    }
  });
  worker.on("error", (error) => stop(`error: ${error.message}`));
  worker.on("exit", () => stop("exit"));
  watchdog = setInterval(
    () => {
      if (Date.now() - lastOkAt > staleMs) stop("stale");
    },
    Math.min(MAX_WATCHDOG_INTERVAL_MS, staleMs),
  );
  watchdog.unref();

  return {
    stop: () => stop(null),
    get stopped() {
      return stopped;
    },
  };
}
