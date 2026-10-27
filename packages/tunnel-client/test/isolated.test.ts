import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import { afterEach, describe, expect, it } from "vitest";
import { encodeFrame } from "@bb/tunnel-contract";
import { IsolatedTunnel } from "../src/isolated.js";

const GATE_SOURCE = `
const http = require("node:http");
const { WebSocketServer } = require("ws");
const readline = require("node:readline");
const origin = http.createServer((request, response) => {
  response.writeHead(200, { "content-type": "text/plain" });
  response.end("origin ok");
});
origin.listen(0, "127.0.0.1", () => {
  const gate = new WebSocketServer({ host: "127.0.0.1", port: 0 }, () => {
    console.log(JSON.stringify({ origin: origin.address().port, gate: gate.address().port }));
  });
  gate.on("connection", (socket) => {
    socket.on("message", (data, isBinary) => {
      if (!isBinary) {
        if (data.toString() === "bbt:hb") socket.send("bbt:hb-ack");
        return;
      }
      if (sentAt !== null) {
        console.log(JSON.stringify({ firstResponseMs: Date.now() - sentAt }));
        sentAt = null;
      }
    });
    let sentAt = null;
    readline.createInterface({ input: process.stdin }).on("line", (hex) => {
      sentAt = Date.now();
      socket.send(Buffer.from(hex, "hex"));
    });
  });
});
`;

let gateProcess: ChildProcessWithoutNullStreams | undefined;
let tunnel: IsolatedTunnel | undefined;

afterEach(() => {
  tunnel?.terminate();
  tunnel = undefined;
  gateProcess?.kill();
  gateProcess = undefined;
});

function blockEventLoop(durationMs: number): void {
  const until = Date.now() + durationMs;
  while (Date.now() < until) {
    Math.sqrt(Math.random());
  }
}

describe("IsolatedTunnel", () => {
  it("keeps proxying while the main event loop is blocked", async () => {
    const child = spawn(process.execPath, ["-e", GATE_SOURCE], {
      cwd: new URL("..", import.meta.url),
    });
    gateProcess = child;
    const lines = createInterface({ input: child.stdout })[
      Symbol.asyncIterator
    ]();
    const nextLine = async (): Promise<Record<string, number>> => {
      const line = await lines.next();
      if (line.done === true) throw new Error("gate exited");
      return JSON.parse(line.value) as Record<string, number>;
    };
    const ports = await nextLine();

    const opened = new Promise<void>((resolve, reject) => {
      tunnel = new IsolatedTunnel({
        url: `ws://127.0.0.1:${ports.gate}/__tunnel`,
        headers: {},
        handshakeTimeoutMs: 5_000,
        log: { warn: () => {} },
      });
      tunnel.setOrigins({
        default: {
          origin: `http://127.0.0.1:${ports.origin}`,
          publicOrigin: "https://example.getbb.app",
        },
        ports: [],
      });
      tunnel.once("open", resolve);
      tunnel.once("close", () => reject(new Error("tunnel closed")));
    });
    await opened;

    const frame = encodeFrame({
      type: "open-http",
      streamId: 1,
      method: "GET",
      path: "/",
      headers: [],
      hasBody: false,
    });
    child.stdin.write(`${Buffer.from(frame).toString("hex")}\n`);
    blockEventLoop(2_000);

    const { firstResponseMs } = await nextLine();
    expect(firstResponseMs).toBeLessThan(1_000);
  }, 20_000);
});
