import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { describe, expect, it } from "vitest";
import { eq } from "drizzle-orm";
import {
  createConnection,
  createProject,
  createThread,
  getThreadConversationOutlineRecord,
  migrate,
  noopNotifier,
  threads,
  upsertHost,
} from "@bb/db";
import {
  OutlineCacheWriter,
  type SlowOutlineWrite,
} from "../../../src/services/threads/outline-cache-writer.js";

function setup() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bb-outline-writer-"));
  const filename = path.join(dir, "bb.db");
  const db = createConnection(filename);
  migrate(db);
  const host = upsertHost(db, noopNotifier, { name: "cache-host" });
  const { project } = createProject(db, noopNotifier, {
    name: "cache-project",
    source: { type: "local_path", hostId: host.id, path: "/tmp/cache" },
  });
  const addThread = () =>
    createThread(db, noopNotifier, {
      projectId: project.id,
      providerId: "codex",
      status: "idle",
    });
  const thread = addThread();
  const writer = new OutlineCacheWriter(filename);
  return {
    db,
    thread,
    writer,
    addThread,
    async cleanup() {
      await writer.close();
      db.$client.close();
      fs.rmSync(dir, { recursive: true, force: true });
    },
  };
}

describe("outline cache worker", () => {
  it("coalesces revisions and persists only the newest projection", async () => {
    const test = setup();
    try {
      for (let revision = 0; revision < 100; revision++) {
        test.writer.enqueue({
          threadId: test.thread.id,
          projectionKey: String(revision),
          itemsJson: JSON.stringify([{ preview: String(revision) }]),
        });
      }
      expect(getThreadConversationOutlineRecord(test.db, test.thread.id)).toBeNull();
      await test.writer.flush();
      expect(test.writer.status().writes).toBe(1);
      expect(getThreadConversationOutlineRecord(test.db, test.thread.id)).toEqual({
        projectionKey: "99",
        itemsJson: '[{"preview":"99"}]',
      });
    } finally {
      await test.cleanup();
    }
  });

  it("keeps the event loop responsive during a writer lock and recovers after rollback", async () => {
    const test = setup();
    try {
      test.db.$client.exec("BEGIN IMMEDIATE");
      test.writer.enqueue({ threadId: test.thread.id, projectionKey: "locked", itemsJson: "[]" });
      let ticks = 0;
      const timer = setInterval(() => ticks++, 5);
      try {
        await test.writer.flush();
      } finally {
        clearInterval(timer);
      }
      expect(ticks).toBeGreaterThan(5);
      expect(test.writer.status().failures).toBe(1);
      expect(getThreadConversationOutlineRecord(test.db, test.thread.id)).toBeNull();
      test.db.$client.exec("ROLLBACK");
      test.writer.enqueue({ threadId: test.thread.id, projectionKey: "unlocked", itemsJson: "[]" });
      await test.writer.flush();
      expect(getThreadConversationOutlineRecord(test.db, test.thread.id)?.projectionKey).toBe("unlocked");
    } finally {
      if (test.db.$client.inTransaction) test.db.$client.exec("ROLLBACK");
      await test.cleanup();
    }
  });

  it("cannot resurrect deleted threads and continues with other writes", async () => {
    const test = setup();
    try {
      const retained = test.addThread();
      test.writer.enqueue({ threadId: test.thread.id, projectionKey: "deleted", itemsJson: "[]" });
      test.db.delete(threads).where(eq(threads.id, test.thread.id)).run();
      test.writer.enqueue({ threadId: retained.id, projectionKey: "retained", itemsJson: "[]" });
      await test.writer.flush();
      expect(getThreadConversationOutlineRecord(test.db, test.thread.id)).toBeNull();
      expect(getThreadConversationOutlineRecord(test.db, retained.id)?.projectionKey).toBe("retained");
    } finally {
      await test.cleanup();
    }
  });

  it("bounds the pending queue and drops oversized optional records", async () => {
    const test = setup();
    try {
      const created = Array.from({ length: 20 }, () => test.addThread());
      for (const thread of created) {
        test.writer.enqueue({ threadId: thread.id, projectionKey: "bounded", itemsJson: "[]" });
      }
      test.writer.enqueue({ threadId: test.thread.id, projectionKey: "oversized", itemsJson: "x".repeat(8_000_001) });
      await test.writer.flush();
      expect(created.filter((thread) => getThreadConversationOutlineRecord(test.db, thread.id))).toHaveLength(16);
      expect(getThreadConversationOutlineRecord(test.db, test.thread.id)).toBeNull();
    } finally {
      await test.cleanup();
    }
  });

  it("treats worker startup failure and shutdown as disposable cache loss", async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "bb-outline-missing-"));
    const writer = new OutlineCacheWriter(path.join(dir, "missing.db"));
    try {
      writer.enqueue({ threadId: "missing", projectionKey: "missing", itemsJson: "[]" });
      await writer.flush();
      await writer.close();
      writer.enqueue({ threadId: "closed", projectionKey: "closed", itemsJson: "[]" });
      await writer.flush();
      await delay(1);
      expect(fs.existsSync(path.join(dir, "missing.db"))).toBe(false);
    } finally {
      await writer.close();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it("reports writes slower than the threshold with their size and outcome", async () => {
    const test = setup();
    const reported: SlowOutlineWrite[] = [];
    const writer = new OutlineCacheWriter(test.db.$client.name, {
      onSlowWrite: (write) => reported.push(write),
      slowWriteMs: 0,
    });
    try {
      writer.enqueue({ threadId: test.thread.id, projectionKey: "timed", itemsJson: "[1,2,3]" });
      await writer.flush();
      expect(reported).toHaveLength(1);
      expect(reported[0]).toMatchObject({
        threadId: test.thread.id,
        characters: 7,
        ok: true,
      });
      expect(reported[0]?.durationMs).toBeGreaterThanOrEqual(0);
      expect(Number.isNaN(Date.parse(reported[0]?.startedAt ?? ""))).toBe(false);
      expect(writer.status().writes).toBe(1);
    } finally {
      await writer.close();
      await test.cleanup();
    }
  });
});
