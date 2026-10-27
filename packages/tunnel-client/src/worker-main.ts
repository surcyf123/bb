import { parentPort, workerData } from "node:worker_threads";
import { WebSocket as NodeWebSocket } from "ws";
import { TunnelSession, type StreamOriginResult } from "./session.js";
import type {
  OriginSnapshot,
  WorkerInbound,
  WorkerInit,
  WorkerOutbound,
} from "./isolated-protocol.js";

const ACTIVITY_REPORT_INTERVAL_MS = 1_000;

const port = parentPort;
if (port === null) throw new Error("tunnel worker requires a parent");
const init = workerData as WorkerInit;
const post = (message: WorkerOutbound): void => port.postMessage(message);

let origins: OriginSnapshot = { default: null, ports: [] };
let session: TunnelSession | undefined;
let lastActivityReportAt = 0;

function resolveOrigin(target: string | undefined): StreamOriginResult {
  if (target === undefined) {
    return origins.default === null
      ? { kind: "unregistered" }
      : { kind: "ok", resolved: origins.default };
  }
  const wanted = Number(target);
  for (const [sharePort, resolved] of origins.ports) {
    if (sharePort === wanted) return { kind: "ok", resolved };
  }
  return { kind: "unregistered" };
}

const tunnel = new NodeWebSocket(init.url, {
  headers: init.headers,
  handshakeTimeout: init.handshakeTimeoutMs,
});

const log = {
  info: (message: string) => post({ t: "log", level: "info", message }),
  warn: (message: string) => post({ t: "log", level: "warn", message }),
};

tunnel.on("open", () => {
  session = new TunnelSession({
    tunnel,
    log,
    resolveOrigin,
    onRemoteClientsChange: (count) => post({ t: "remote-clients", count }),
    onActivity: (at) => {
      if (at - lastActivityReportAt < ACTIVITY_REPORT_INTERVAL_MS) return;
      lastActivityReportAt = at;
      post({ t: "activity", at });
    },
  });
  session.start();
  post({ t: "open" });
});
tunnel.on("unexpected-response", (_request, response) => {
  response.resume();
  post({ t: "unexpected-response", status: response.statusCode ?? 0 });
});
tunnel.on("error", (error: Error) => {
  const code = (error as NodeJS.ErrnoException).code;
  post({ t: "error", message: error.message, ...(code ? { code } : {}) });
});
tunnel.on("close", (code: number, reason: Buffer) => {
  session?.dispose();
  session = undefined;
  post({ t: "close", code, reason: reason.toString() });
  port.close();
});

port.on("message", (message: WorkerInbound) => {
  switch (message.t) {
    case "origins":
      origins = message.snapshot;
      return;
    case "dispose-session":
      session?.dispose();
      session = undefined;
      return;
    case "close":
      tunnel.close(message.code, message.reason);
      return;
    case "terminate":
      tunnel.terminate();
      return;
  }
});
