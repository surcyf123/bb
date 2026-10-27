import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createConnection, type DbConnection } from "@bb/db";
import { startBackgroundWalCheckpointer } from "../../src/services/system/wal-checkpointer.js";

const directories: string[] = [];
const connections: DbConnection[] = [];

afterEach(() => {
  for (const db of connections.splice(0)) db.$client.close();
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function fileDatabase() {
  const directory = mkdtempSync(join(tmpdir(), "bb-wal-checkpointer-"));
  directories.push(directory);
  const path = join(directory, "bb.db");
  const db = createConnection(path);
  connections.push(db);
  db.$client.exec("CREATE TABLE fill (value TEXT)");
  const insert = db.$client.prepare("INSERT INTO fill (value) VALUES (?)");
  const fill = (rows: number) => {
    for (let index = 0; index < rows; index += 1) insert.run("x".repeat(4_000));
  };
  return { db, path, fill };
}

function autocheckpointPages(db: DbConnection): unknown {
  return db.$client.pragma("wal_autocheckpoint", { simple: true });
}

function logger() {
  return { info: vi.fn(), warn: vi.fn() };
}

describe("background WAL checkpointer", { timeout: 30_000 }, () => {
  it("takes checkpoints off the writing connection and reuses the WAL", async () => {
    const { db, path, fill } = fileDatabase();
    const log = logger();
    const checkpointer = startBackgroundWalCheckpointer(db, log, { intervalMs: 50 });
    try {
      expect(autocheckpointPages(db)).toBe(0);
      fill(3_000);
      const walAfterFirstBatch = statSync(`${path}-wal`).size;
      await delay(400);
      fill(3_000);
      expect(statSync(`${path}-wal`).size).toBeLessThan(walAfterFirstBatch * 1.5);
    } finally {
      checkpointer?.stop();
    }
    expect(autocheckpointPages(db)).toBe(1_000);
    expect(log.warn).not.toHaveBeenCalled();
  });

  it("restores automatic checkpoints when the worker stops reporting", async () => {
    const { db } = fileDatabase();
    const log = logger();
    const checkpointer = startBackgroundWalCheckpointer(db, log, {
      intervalMs: 5_000,
      staleMs: 200,
    });
    await delay(1_000);
    expect(checkpointer?.stopped).toBe(true);
    expect(autocheckpointPages(db)).toBe(1_000);
    expect(log.warn).toHaveBeenCalledWith(
      { reason: "stale" },
      "Background WAL checkpointer stopped; restored automatic checkpoints",
    );
  });

  it("does nothing for an in-memory database", () => {
    const db = createConnection(":memory:");
    connections.push(db);
    expect(startBackgroundWalCheckpointer(db, logger())).toBeNull();
  });
});
