import { EventEmitter } from "node:events";
import { createServer, type Server } from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { WebSocket as NodeWebSocket } from "ws";
import { decodeFrame, encodeFrame } from "@bb/tunnel-contract";
import { TunnelSession } from "../src/session.js";

const RESPONSE_BYTES = 8 * 1024 * 1024;

class SlowTunnel extends EventEmitter {
  readonly readyState = 1;
  bufferedAmount = 0;
  peakBufferedAmount = 0;
  bodyEnded = false;
  readonly terminate = vi.fn();

  send(data: Uint8Array | string): void {
    if (typeof data === "string") return;
    const frame = decodeFrame(data);
    if (frame.type === "body-end") this.bodyEnded = true;
    this.bufferedAmount += data.byteLength;
    this.peakBufferedAmount = Math.max(
      this.peakBufferedAmount,
      this.bufferedAmount,
    );
  }

  drain(bytes: number): void {
    this.bufferedAmount = Math.max(0, this.bufferedAmount - bytes);
  }
}

let server: Server | undefined;

afterEach(async () => {
  const current = server;
  server = undefined;
  if (current) await new Promise<void>((resolve) => current.close(() => resolve()));
});

describe("tunnel send backpressure", () => {
  it("stops reading the origin while the tunnel send buffer is full", async () => {
    server = createServer((_request, response) => {
      response.writeHead(200, { "content-type": "application/octet-stream" });
      response.end(Buffer.alloc(RESPONSE_BYTES, 7));
    });
    await new Promise<void>((resolve) => server!.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (address === null || typeof address === "string") {
      throw new Error("expected a TCP address");
    }
    const tunnel = new SlowTunnel();
    const session = new TunnelSession({
      tunnel: tunnel as unknown as NodeWebSocket,
      log: { warn: () => {} },
      resolveOrigin: () => ({
        kind: "ok",
        resolved: {
          origin: `http://127.0.0.1:${address.port}`,
          publicOrigin: "https://handle.getbb.app",
        },
      }),
    });
    session.start();
    tunnel.emit(
      "message",
      Buffer.from(
        encodeFrame({
          type: "open-http",
          streamId: 3,
          method: "GET",
          path: "/large",
          headers: [],
          hasBody: false,
        }),
      ),
      true,
    );

    const drainer = setInterval(() => tunnel.drain(256 * 1024), 5);
    try {
      await vi.waitFor(() => expect(tunnel.bodyEnded).toBe(true), {
        timeout: 10_000,
      });
    } finally {
      clearInterval(drainer);
      session.dispose();
    }
    expect(tunnel.peakBufferedAmount).toBeLessThan(RESPONSE_BYTES / 2);
  });
});
