export interface ResolvedOrigin {
  origin: string;
  publicOrigin: string;
  host?: string;
}

export interface OriginSnapshot {
  default: ResolvedOrigin | null;
  ports: Array<[number, ResolvedOrigin]>;
}

export interface WorkerInit {
  url: string;
  headers: Record<string, string>;
  handshakeTimeoutMs: number;
}

export type WorkerInbound =
  | { t: "origins"; snapshot: OriginSnapshot }
  | { t: "dispose-session" }
  | { t: "close"; code: number; reason: string }
  | { t: "terminate" };

export type WorkerOutbound =
  | { t: "open" }
  | { t: "unexpected-response"; status: number }
  | { t: "error"; message: string; code?: string }
  | { t: "close"; code: number; reason: string }
  | { t: "remote-clients"; count: number }
  | { t: "activity"; at: number }
  | { t: "log"; level: "info" | "warn"; message: string };
