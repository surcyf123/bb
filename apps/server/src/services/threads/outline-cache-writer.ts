import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";
import { z } from "zod";
interface OutlineWrite {
  threadId: string;
  projectionKey: string;
  itemsJson: string;
}

export interface SlowOutlineWrite {
  threadId: string;
  characters: number;
  durationMs: number;
  startedAt: string;
  ok: boolean;
}

interface OutlineCacheWriterOptions {
  onSlowWrite?: (write: SlowOutlineWrite) => void;
  slowWriteMs?: number;
}

const workerResultSchema = z.object({
  ok: z.boolean(),
  durationMs: z.number(),
  startedAt: z.number(),
  characters: z.number(),
  threadId: z.string(),
});

const workerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const Database = require(workerData.modulePath);
const db = new Database(workerData.filename, { fileMustExist: true, timeout: 50 });
db.pragma("foreign_keys = ON");
db.pragma("synchronous = NORMAL");
const write = db.prepare('INSERT INTO thread_conversation_outlines (thread_id, projection_key, items_json) VALUES (?, ?, ?) ON CONFLICT(thread_id) DO UPDATE SET projection_key = excluded.projection_key, items_json = excluded.items_json');
parentPort.on("message", (record) => {
  const startedAt = Date.now();
  const started = performance.now();
  let ok = true;
  try {
    write.run(record.threadId, record.projectionKey, record.itemsJson);
  } catch {
    ok = false;
  }
  parentPort.postMessage({
    ok,
    durationMs: performance.now() - started,
    startedAt,
    characters: record.itemsJson.length,
    threadId: record.threadId,
  });
});
`;

const MAX_PENDING_CHARACTERS = 8_000_000;
const MAX_PENDING_THREADS = 16;
const SLOW_WRITE_MS = 250;

export class OutlineCacheWriter {
  private readonly pending = new Map<string, OutlineWrite>();
  private pendingCharacters = 0;
  private worker: Worker | null = null;
  private inFlight: OutlineWrite | null = null;
  private scheduled: NodeJS.Immediate | null = null;
  private idleTimer: NodeJS.Timeout | null = null;
  private closed = false;
  private writes = 0;
  private failures = 0;
  private readonly waiters = new Set<() => void>();

  constructor(
    private readonly filename: string,
    private readonly options: OutlineCacheWriterOptions = {},
  ) {}

  status() {
    return {
      pendingThreads: this.pending.size,
      pendingCharacters: this.pendingCharacters,
      inFlight: this.inFlight !== null,
      writes: this.writes,
      failures: this.failures,
    };
  }

  enqueue(record: OutlineWrite): void {
    if (this.closed || record.itemsJson.length > MAX_PENDING_CHARACTERS) return;
    const previous = this.pending.get(record.threadId);
    if (previous) this.pendingCharacters -= previous.itemsJson.length;
    this.pending.delete(record.threadId);
    this.pending.set(record.threadId, record);
    this.pendingCharacters += record.itemsJson.length;
    while (
      this.pending.size > MAX_PENDING_THREADS ||
      this.pendingCharacters > MAX_PENDING_CHARACTERS
    ) {
      const oldest = this.pending.values().next().value;
      if (!oldest) break;
      this.pending.delete(oldest.threadId);
      this.pendingCharacters -= oldest.itemsJson.length;
    }
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = null;
    if (!this.scheduled && !this.inFlight) {
      this.scheduled = setImmediate(() => {
        this.scheduled = null;
        this.dispatch();
      });
    }
  }

  flush(): Promise<void> {
    if (!this.pending.size && !this.inFlight) return Promise.resolve();
    return new Promise((resolve) => this.waiters.add(resolve));
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.scheduled) clearImmediate(this.scheduled);
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.scheduled = null;
    this.idleTimer = null;
    this.pending.clear();
    this.pendingCharacters = 0;
    this.inFlight = null;
    const worker = this.worker;
    this.worker = null;
    if (worker) await worker.terminate();
    this.resolveWaiters();
  }

  private dispatch(): void {
    if (this.closed || this.inFlight) return;
    const next = this.pending.values().next().value;
    if (!next) {
      this.resolveWaiters();
      if (this.worker && !this.idleTimer) {
        this.idleTimer = setTimeout(() => {
          const worker = this.worker;
          this.worker = null;
          this.idleTimer = null;
          if (worker) void worker.terminate();
        }, 30_000);
        this.idleTimer.unref();
      }
      return;
    }
    this.pending.delete(next.threadId);
    this.pendingCharacters -= next.itemsJson.length;
    this.inFlight = next;
    try {
      if (!this.worker) {
        const worker = new Worker(workerSource, {
          eval: true,
          execArgv: [],
          workerData: {
            filename: this.filename,
            modulePath: createRequire(import.meta.url).resolve("better-sqlite3"),
          },
        });
        this.worker = worker;
        worker.on("message", (message: unknown) => {
          if (this.worker !== worker) return;
          const result = workerResultSchema.safeParse(message);
          if (result.success && result.data.ok) this.writes++;
          else this.failures++;
          if (
            result.success &&
            result.data.durationMs >= (this.options.slowWriteMs ?? SLOW_WRITE_MS)
          ) {
            this.options.onSlowWrite?.({
              threadId: result.data.threadId,
              characters: result.data.characters,
              durationMs: Math.round(result.data.durationMs),
              startedAt: new Date(result.data.startedAt).toISOString(),
              ok: result.data.ok,
            });
          }
          this.inFlight = null;
          this.dispatch();
        });
        worker.on("error", () => this.failed(worker));
        worker.on("exit", () => this.failed(worker));
        worker.unref();
      }
      this.worker.postMessage(next);
    } catch {
      if (this.worker) this.failed(this.worker);
      else {
        this.inFlight = null;
        this.pending.clear();
        this.pendingCharacters = 0;
        this.resolveWaiters();
      }
    }
  }

  private failed(worker: Worker): void {
    if (this.worker !== worker) return;
    this.failures++;
    this.worker = null;
    this.inFlight = null;
    this.pending.clear();
    this.pendingCharacters = 0;
    void worker.terminate();
    this.resolveWaiters();
  }

  private resolveWaiters(): void {
    for (const resolve of this.waiters) resolve();
    this.waiters.clear();
  }
}

