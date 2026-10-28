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
const RESTART_WINDOW_MS = 10 * 60_000;
const MAX_RESTARTS_PER_WINDOW = 5;

const checkpointWorkerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const Database = require(workerData.modulePath);
const db = new Database(workerData.filename, { fileMustExist: true, timeout: 0 });
setInterval(() => {
  const started = Date.now();
  parentPort.postMessage({ phase: "start" });
  try {
    const row = db.pragma("wal_checkpoint(PASSIVE)")[0] ?? {};
    parentPort.postMessage({ ok: true, durationMs: Date.now() - started, busy: row.busy, log: row.log, checkpointed: row.checkpointed });
  } catch (error) {
    parentPort.postMessage({ ok: false, durationMs: Date.now() - started, error: String(error && error.message || error) });
  }
}, workerData.intervalMs);
`;

const checkpointStartSchema = z.object({ phase: z.literal("start") });

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
  workerSource?: string;
}

export interface WalCheckpointer {
  stop(): void;
  readonly stopped: boolean;
  readonly restarts: number;
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
  const workerSource = options.workerSource ?? checkpointWorkerSource;
  const modulePath = createRequire(import.meta.url).resolve("better-sqlite3");

  let worker: Worker | null = null;
  let lastOkAt = Date.now();
  let checkpointStartedAt: number | null = null;
  let longCheckpointLogged = false;
  let stopped = false;
  let watchdog: ReturnType<typeof setInterval> | undefined;
  const restarts: number[] = [];

  const stop = (reason: string | null) => {
    if (stopped) return;
    stopped = true;
    clearInterval(watchdog);
    if (client.open) {
      client.pragma(`wal_autocheckpoint = ${DEFAULT_WAL_AUTOCHECKPOINT_PAGES}`);
    }
    if (worker !== null) void worker.terminate();
    if (reason !== null) {
      logger.warn(
        { reason },
        "Background WAL checkpointer stopped; restored automatic checkpoints",
      );
    }
  };

  const spawn = () => {
    const next = new Worker(workerSource, {
      eval: true,
      execArgv: [],
      workerData: { filename: client.name, modulePath, intervalMs },
    });
    next.unref();
    next.on("message", (message: unknown) => {
      if (worker !== next) return;
      if (checkpointStartSchema.safeParse(message).success) {
        checkpointStartedAt = Date.now();
        return;
      }
      checkpointStartedAt = null;
      longCheckpointLogged = false;
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
    next.on("error", (error) => {
      if (worker === next) restart(`error: ${error.message}`);
    });
    next.on("exit", () => {
      if (worker === next) restart("exit");
    });
    worker = next;
    lastOkAt = Date.now();
    checkpointStartedAt = null;
    longCheckpointLogged = false;
  };

  const restart = (reason: string) => {
    if (stopped) return;
    const now = Date.now();
    while (restarts.length > 0 && now - restarts[0]! > RESTART_WINDOW_MS) {
      restarts.shift();
    }
    if (restarts.length >= MAX_RESTARTS_PER_WINDOW) {
      stop(`${reason}; ${restarts.length} restarts in ${RESTART_WINDOW_MS / 60_000} min`);
      return;
    }
    restarts.push(now);
    const previous = worker;
    worker = null;
    if (previous !== null) void previous.terminate();
    try {
      spawn();
      logger.warn({ reason }, "Background WAL checkpointer restarted");
    } catch (error) {
      stop(
        `restart failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  };

  try {
    spawn();
  } catch (error) {
    logger.warn(
      { err: error instanceof Error ? error.message : String(error) },
      "Background WAL checkpointer could not start; keeping automatic checkpoints",
    );
    return null;
  }
  client.pragma("wal_autocheckpoint = 0");

  watchdog = setInterval(
    () => {
      const now = Date.now();
      if (checkpointStartedAt !== null) {
        if (!longCheckpointLogged && now - checkpointStartedAt > staleMs) {
          longCheckpointLogged = true;
          logger.info(
            { runningMs: now - checkpointStartedAt },
            "Background WAL checkpoint still running",
          );
        }
        return;
      }
      if (now - lastOkAt > staleMs) restart("stale");
    },
    Math.min(MAX_WATCHDOG_INTERVAL_MS, staleMs),
  );
  watchdog.unref();

  return {
    stop: () => stop(null),
    get stopped() {
      return stopped;
    },
    get restarts() {
      return restarts.length;
    },
  };
}
