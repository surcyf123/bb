import { EventEmitter } from "node:events";
import { Worker } from "node:worker_threads";
import { TUNNEL_WORKER_SOURCE } from "./generated/tunnel-worker-source.generated.js";
import type {
  OriginSnapshot,
  WorkerInbound,
  WorkerInit,
  WorkerOutbound,
} from "./isolated-protocol.js";
import type { TunnelClientLogger } from "./logger.js";

export type { OriginSnapshot, ResolvedOrigin } from "./isolated-protocol.js";

const CONNECTING = 0;
const OPEN = 1;
const CLOSING = 2;
const CLOSED = 3;
const WORKER_EXIT_CLOSE_CODE = 1006;

interface IsolatedTunnelOptions {
  url: string;
  headers: Record<string, string>;
  handshakeTimeoutMs: number;
  log: TunnelClientLogger;
  onRemoteClientsChange?: (remoteClients: number) => void;
  onActivity?: (at: number) => void;
}

export interface IsolatedTunnelError extends Error {
  code?: string;
}

export interface IsolatedTunnelEvents {
  open: [];
  "unexpected-response": [status: number];
  error: [error: IsolatedTunnelError];
  close: [code: number, reason: string];
}

export class IsolatedTunnel extends EventEmitter<IsolatedTunnelEvents> {
  private readonly worker: Worker;
  private state = CONNECTING;
  private closeEmitted = false;

  constructor(private readonly options: IsolatedTunnelOptions) {
    super();
    const init: WorkerInit = {
      url: options.url,
      headers: options.headers,
      handshakeTimeoutMs: options.handshakeTimeoutMs,
    };
    this.worker = new Worker(TUNNEL_WORKER_SOURCE, {
      eval: true,
      workerData: init,
      execArgv: [],
    });
    this.worker.unref();
    this.worker.on("message", (message: WorkerOutbound) =>
      this.onWorkerMessage(message),
    );
    this.worker.on("error", (error: Error) => {
      this.options.log.warn(`tunnel worker failed: ${error.message}`);
    });
    this.worker.on("exit", () => {
      this.state = CLOSED;
      this.emitClose(WORKER_EXIT_CLOSE_CODE, "");
    });
  }

  get readyState(): number {
    return this.state;
  }

  get isOpen(): boolean {
    return this.state === OPEN;
  }

  setOrigins(snapshot: OriginSnapshot): void {
    this.post({ t: "origins", snapshot });
  }

  disposeSession(): void {
    this.post({ t: "dispose-session" });
  }

  close(code: number, reason: string): void {
    if (this.state === CLOSED) return;
    this.state = CLOSING;
    this.post({ t: "close", code, reason });
  }

  terminate(): void {
    if (this.state === CLOSED) return;
    this.state = CLOSING;
    this.post({ t: "terminate" });
    void this.worker.terminate();
  }

  private post(message: WorkerInbound): void {
    if (this.state === CLOSED) return;
    this.worker.postMessage(message);
  }

  private emitClose(code: number, reason: string): void {
    if (this.closeEmitted) return;
    this.closeEmitted = true;
    this.emit("close", code, reason);
  }

  private onWorkerMessage(message: WorkerOutbound): void {
    switch (message.t) {
      case "open":
        if (this.state === CONNECTING) this.state = OPEN;
        this.emit("open");
        return;
      case "unexpected-response":
        this.emit("unexpected-response", message.status);
        return;
      case "error": {
        const error: IsolatedTunnelError = new Error(message.message);
        if (message.code !== undefined) error.code = message.code;
        this.emit("error", error);
        return;
      }
      case "close":
        this.state = CLOSED;
        this.emitClose(message.code, message.reason);
        void this.worker.terminate();
        return;
      case "remote-clients":
        this.options.onRemoteClientsChange?.(message.count);
        return;
      case "activity":
        this.options.onActivity?.(message.at);
        return;
      case "log":
        if (message.level === "warn") this.options.log.warn(message.message);
        else this.options.log.info?.(message.message);
        return;
    }
  }
}
