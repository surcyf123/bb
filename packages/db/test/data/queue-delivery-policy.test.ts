import { afterEach, describe, expect, it, vi } from "vitest";
import { defaultAppSettings, turnScope, threadScope } from "@bb/domain";
import { createMigratedConnection } from "../helpers/migrated-connection.js";
import { noopNotifier } from "../../src/notifier.js";
import { createProject } from "../../src/data/projects.js";
import { upsertHost } from "../../src/data/hosts.js";
import { createThread } from "../../src/data/threads.js";
import { setAppSettings } from "../../src/data/app-settings.js";
import { threads, events, childNotificationOutbox, childNotificationReceipts } from "../../src/schema.js";
import { enqueueDurableChildNotification, listUnreceiptedChildTurnCompletions } from "../../src/data/child-notifications.js";
import { claimNextQueuedThreadMessageGroup, claimQueuedThreadMessageGroup, clearQueuedThreadMessageWaitingOn, createQueuedThreadMessage, deleteClaimedQueuedThreadMessageBatchInTransaction, getQueuedThreadMessage, holdQueuedThreadMessageForEdit, listQueuedThreadMessages, releaseStaleQueuedMessageClaims, type CreateQueuedThreadMessageInput } from "../../src/data/queued-thread-messages.js";

import { insertEvents, appendStoredThreadEvent, appendStoredThreadEventInTransaction, copyStoredThreadEventsInTransaction } from "../../src/data/events.js";
import { createConnection } from "../../src/connection.js";
import { eq } from "drizzle-orm";

const connections: ReturnType<typeof createMigratedConnection>[] = [];
afterEach(() => { for (const db of connections.splice(0)) db.$client.close(); vi.useRealTimers(); });

function setup() {
  const db = createMigratedConnection(); connections.push(db);
  const host = upsertHost(db, noopNotifier, { name: "host" });
  const { project } = createProject(db, noopNotifier, { name: "project", source: { type: "local_path", hostId: host.id, path: "/tmp/queue-policy" } });
  const thread = createThread(db, noopNotifier, { projectId: project.id, providerId: "codex" });
  db.update(threads).set({ status: "idle" }).where(eq(threads.id, thread.id)).run();
  setAppSettings(db, { ...defaultAppSettings, batchReadyQueuedMessages: true, readyQueueSenderCap: 3, childOutcomeBatchWindowMs: 300_000 });
  const input = (text: string, changes: Partial<CreateQueuedThreadMessageInput> = {}): CreateQueuedThreadMessageInput => ({
    threadId: thread.id, content: [{ type: "text", text, mentions: [] }], senderThreadId: "thr_sender_a", model: "test", reasoningLevel: "medium", permissionMode: "full", serviceTier: "default", waitingOn: { kind: "thread-busy" }, sendAt: null, payload: { kind: "inline" }, systemNotice: null, ...changes,
  });
  const enqueue = (text: string, changes: Partial<CreateQueuedThreadMessageInput> = {}) => createQueuedThreadMessage(db, noopNotifier, input(text, changes));
  const child = (sourceKey: string, text: string, delayMs = 300_000) => enqueueDurableChildNotification(db, noopNotifier, {
    sourceKey, childThreadId: sourceKey.split(":")[0]!, delayMs,
    message: input(text, { senderThreadId: null, systemNotice: { kind: delayMs ? "child-completed" : "child-failed", subject: { kind: "thread", threadId: sourceKey.split(":")[0]!, threadName: "child", outcomes: [{ threadId: sourceKey.split(":")[0]!, status: delayMs ? "completed" : "failed" }] } } }),
  });
  return { db, thread, enqueue, child, project };
}

describe("durable queue delivery", () => {
  it("retains original completion deadlines after recovery and shortens a newer pending batch", () => {
    vi.useFakeTimers(); vi.setSystemTime(1_800_000_000_000);
    const { db, thread, project, child } = setup();
    const worker = createThread(db, noopNotifier, { projectId: project.id, providerId: "codex", parentThreadId: thread.id });
    const source = (turnId: string) => {
      appendStoredThreadEvent(db, noopNotifier, { threadId: worker.id, scope: turnScope(turnId), type: "turn/started", data: { providerThreadId: "provider" } });
      appendStoredThreadEvent(db, noopNotifier, { threadId: worker.id, scope: turnScope(turnId), type: "turn/completed", data: { providerThreadId: "provider", status: "completed" } });
      return listUnreceiptedChildTurnCompletions(db).find((row) => row.turnId === turnId)!.sourceKey;
    };
    const oldest = source("oldest");
    vi.advanceTimersByTime(400_000);
    const older = source("older");
    vi.advanceTimersByTime(100_000);
    const current = child("current:turn", "current");
    const recovered = child(older, "older");
    expect(recovered.queuedMessageId).toBe(current.queuedMessageId);
    expect(recovered.sendAt).toBe(Date.now() + 200_000);
    expect(getQueuedThreadMessage(db, recovered.queuedMessageId)?.sendAt).toBe(recovered.sendAt);
    const overdue = child(oldest, "oldest");
    expect(overdue.sendAt).toBe(Date.now());
    expect(overdue.queuedMessageId).not.toBe(current.queuedMessageId);
  });

  it("claims compatible ready senders together while retaining all row identities and scheduled boundaries", () => {
    const { db, thread, enqueue } = setup();
    const a = enqueue("a");
    const scheduled = enqueue("scheduled", { sendAt: Date.now() + 60_000, waitingOn: { kind: "time" } });
    const b = enqueue("b", { senderThreadId: "thr_sender_b" });
    const differentPermission = enqueue("permission boundary", { permissionMode: "accept-edits" });
    const human = enqueue("human", { senderThreadId: null });
    const batch = claimNextQueuedThreadMessageGroup(db, noopNotifier, thread.id, () => true)!;
    expect(batch.map((r) => r.id)).toEqual([a.id, b.id]);
    expect(batch.map((r) => r.senderThreadId)).toEqual(["thr_sender_a", "thr_sender_b"]);
    expect(listQueuedThreadMessages(db, thread.id).map((r) => r.id)).toEqual([scheduled.id, differentPermission.id, human.id]);
    db.transaction((tx) => expect(deleteClaimedQueuedThreadMessageBatchInTransaction(tx, { queuedMessages: batch })).toBe(true));
    const telemetry = db.select().from(events).all().filter((e) => e.type === "provider/unhandled" && JSON.parse(e.data).rawType === "queue/dispatched").map((e) => JSON.parse(e.data).rawEvent.params);
    expect(telemetry.map((e) => e.queuedMessageId).sort()).toEqual([a.id, b.id].sort());
    expect(telemetry.every((e) => typeof e.createdAt === "number" && e.dispatchedAt >= e.createdAt)).toBe(true);
  });

  it("keeps a due scheduled message separate even after its wait is cleared", () => {
    const { db, thread, enqueue } = setup();
    const a = enqueue("ready");
    const scheduled = enqueue("scheduled", { sendAt: Date.now() - 1, waitingOn: { kind: "time" } });
    clearQueuedThreadMessageWaitingOn(db, noopNotifier, { id: scheduled.id, threadId: thread.id });
    expect(claimNextQueuedThreadMessageGroup(db, noopNotifier, thread.id, () => true)?.map((r) => r.id)).toEqual([a.id]);
    expect(getQueuedThreadMessage(db, scheduled.id)?.originallyScheduledAt).not.toBeNull();
  });

  it("coalesces only the fourth compatible sender message and preserves every text and enqueue receipt", () => {
    const { db, thread, enqueue } = setup();
    const first = enqueue("one"); enqueue("two"); const third = enqueue("three");
    const fourth = enqueue("four");
    expect(fourth.id).toBe(third.id);
    const rows = listQueuedThreadMessages(db, thread.id);
    expect(rows).toHaveLength(3);
    expect(rows[0]?.id).toBe(first.id);
    expect(rows[2]?.content).toContain("three"); expect(rows[2]?.content).toContain("four");
    const enqueued = db.select().from(events).all().filter((e) => e.type === "provider/unhandled" && JSON.parse(e.data).rawType === "queue/enqueued").map((e) => JSON.parse(e.data).rawEvent.params);
    expect(enqueued).toHaveLength(4);
    expect(new Set(enqueued.map((e) => e.enqueueId)).size).toBe(4);
    expect(enqueued[3]).toMatchObject({ queuedMessageId: third.id, merged: true });
    expect(enqueue("different sender", { senderThreadId: "thr_sender_b" }).id).not.toBe(third.id);
    expect(enqueue("scheduled", { sendAt: Date.now() + 1_000, waitingOn: { kind: "time" } }).id).not.toBe(third.id);
  });

  it("never coalesces or drains across a live edit hold", () => {
    const { db, thread, enqueue } = setup();
    const a = enqueue("a"); enqueue("b"); const c = enqueue("c");
    holdQueuedThreadMessageForEdit(db, { id: c.id, threadId: thread.id, heldUntil: Date.now() + 60_000 });
    expect(enqueue("d").id).not.toBe(c.id);
    const batch = claimNextQueuedThreadMessageGroup(db, noopNotifier, thread.id, () => true)!;
    expect(batch[0]?.id).toBe(a.id); expect(batch).toHaveLength(2);
    expect(getQueuedThreadMessage(db, c.id)?.content).not.toContain('"d"');
  });

  it("persists every child result immediately with a fixed deadline and sends urgent failures separately", () => {
    vi.useFakeTimers(); vi.setSystemTime(1_800_000_000_000);
    const { db, thread, child } = setup();
    const a = child("child-a:turn-1", "first result");
    vi.advanceTimersByTime(200_000);
    const b = child("child-b:turn-1", "second result");
    expect(b.queuedMessageId).toBe(a.queuedMessageId); expect(b.sendAt).toBe(a.sendAt);
    const urgent = child("child-a:turn-2", "failed", 0);
    expect(urgent.queuedMessageId).not.toBe(a.queuedMessageId); expect(urgent.sendAt).toBe(Date.now());
    const rows = listQueuedThreadMessages(db, thread.id);
    expect(rows).toHaveLength(2); expect(rows[0]?.content).toContain("first result"); expect(rows[0]?.content).toContain("second result");
    expect(JSON.parse(rows[0]!.systemNotice!).subject.outcomes).toHaveLength(2);
    expect(db.select().from(childNotificationReceipts).all()).toHaveLength(3);
  });

  it("replays a source event idempotently before and after dispatch, and recovers abandoned claims", () => {
    const { db, thread, child } = setup();
    const a = child("child:turn-1", "result");
    expect(child("child:turn-1", "result").duplicate).toBe(true);
    const claimed = claimQueuedThreadMessageGroup(db, noopNotifier, a.queuedMessageId, { kind: "explicit-send" })!;
    expect(releaseStaleQueuedMessageClaims(db, noopNotifier, { claimedBefore: Date.now() + 1, protectedClaimTokens: [] })).toBe(1);
    const replayed = claimQueuedThreadMessageGroup(db, noopNotifier, a.queuedMessageId, { kind: "explicit-send" })!;
    expect(replayed[0]?.claimToken).not.toBe(claimed[0]?.claimToken);
    expect(() => db.transaction((tx) => { deleteClaimedQueuedThreadMessageBatchInTransaction(tx, { queuedMessages: replayed }); throw new Error("crash before commit"); })).toThrow("crash before commit");
    expect(getQueuedThreadMessage(db, a.queuedMessageId)).not.toBeNull();
    db.transaction((tx) => expect(deleteClaimedQueuedThreadMessageBatchInTransaction(tx, { queuedMessages: replayed })).toBe(true));
    expect(child("child:turn-1", "result").duplicate).toBe(true);
    expect(listQueuedThreadMessages(db, thread.id)).toHaveLength(0);
  });
  it("recovers committed child completions after restart even when their timestamp precedes an empty scan", () => {
    vi.useFakeTimers(); vi.setSystemTime(1_800_000_000_000);
    const { db, thread, project } = setup();
    const child = createThread(db, noopNotifier, { projectId: project.id, providerId: "codex", parentThreadId: thread.id });
    const scope = turnScope("turn-restart");
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope, type: "turn/started", data: { providerThreadId: "provider" } });
    expect(listUnreceiptedChildTurnCompletions(db)).toEqual([]);
    vi.setSystemTime(Date.now() - 60_000);
    expect(() => db.transaction((tx) => {
      appendStoredThreadEventInTransaction(tx, { threadId: child.id, scope, type: "turn/completed", data: { providerThreadId: "provider", status: "completed" } });
      expect(tx.select().from(childNotificationOutbox).all()).toHaveLength(1);
      throw new Error("crash before source commit");
    })).toThrow("crash before source commit");
    expect(listUnreceiptedChildTurnCompletions(db)).toEqual([]);
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope, type: "turn/completed", data: { providerThreadId: "provider", status: "completed" } });
    const restarted = createConnection(db.$client.serialize()); connections.push(restarted);
    const [pending] = listUnreceiptedChildTurnCompletions(restarted);
    expect(pending).toMatchObject({ childThreadId: child.id, parentThreadId: thread.id, turnId: "turn-restart" });
    expect(listUnreceiptedChildTurnCompletions(restarted)).toHaveLength(1);
    const result = enqueueDurableChildNotification(restarted, noopNotifier, {
      sourceKey: pending!.sourceKey, childThreadId: child.id, delayMs: 300_000,
      message: { threadId: thread.id, content: [{ type: "text", text: "recovered", mentions: [] }], senderThreadId: null, model: "test", reasoningLevel: "medium", permissionMode: "full", serviceTier: "default", waitingOn: null, sendAt: null, payload: { kind: "inline" }, systemNotice: null },
    });
    expect(result.duplicate).toBe(false);
    expect(listUnreceiptedChildTurnCompletions(restarted)).toEqual([]);
    expect(getQueuedThreadMessage(restarted, result.queuedMessageId)?.content).toContain("recovered");
  });

  it("does not replay copied history, nested turns or manual stops, but retains command failures", () => {
    const { db, thread, project } = setup();
    const child = createThread(db, noopNotifier, { projectId: project.id, providerId: "codex", parentThreadId: thread.id });
    const scope = turnScope("turn-manual");
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope, type: "turn/started", data: { providerThreadId: "provider" } });
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope: threadScope(), type: "system/thread/interrupted", data: { reason: "manual-stop" } });
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope, type: "turn/completed", data: { providerThreadId: "provider", status: "interrupted" } });
    expect(listUnreceiptedChildTurnCompletions(db)).toEqual([]);
    const target = createThread(db, noopNotifier, { projectId: project.id, providerId: "codex", parentThreadId: thread.id });
    db.transaction((tx) => copyStoredThreadEventsInTransaction(tx, { rows: tx.select().from(events).where(eq(events.threadId, child.id)).all(), targetThreadId: target.id, targetEnvironmentId: null }));
    const nested = turnScope("turn-nested");
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope: nested, type: "turn/started", data: { providerThreadId: "provider", parentToolCallId: "tool-nested" } });
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope: nested, type: "turn/completed", data: { providerThreadId: "provider", status: "completed" } });
    expect(listUnreceiptedChildTurnCompletions(db)).toEqual([]);
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope: threadScope(), type: "system/error", data: { code: "thread_command_failed", message: "failure", detail: "test" } });
    expect(listUnreceiptedChildTurnCompletions(db)).toHaveLength(1);
    expect(JSON.parse(listUnreceiptedChildTurnCompletions(db)[0]!.data).status).toBe("failed");
  });

  it("does not capture nested completion sharing a root turn ID", () => {
    const { db, thread, project } = setup();
    const child = createThread(db, noopNotifier, { projectId: project.id, providerId: "codex", parentThreadId: thread.id });
    const scope = turnScope("shared-turn");
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope, type: "turn/started", data: { providerThreadId: "provider" } });
    insertEvents(db, noopNotifier, [{ threadId: child.id, scope, sequence: 2, type: "turn/completed", itemId: null, itemKind: null, parentToolCallId: "nested-call", data: JSON.stringify({ providerThreadId: "provider", status: "completed" }) }]);
    expect(listUnreceiptedChildTurnCompletions(db)).toEqual([]);
    appendStoredThreadEvent(db, noopNotifier, { threadId: child.id, scope, type: "turn/completed", data: { providerThreadId: "provider", status: "completed" } });
    expect(listUnreceiptedChildTurnCompletions(db)).toHaveLength(1);
  });

  it.each(["manual-stop", "host-daemon-restarted"] as const)("recovers the final interruption reason after the real lifecycle event order: %s", (reason) => {
    const { db, thread, project } = setup();
    const child = createThread(db, noopNotifier, { projectId: project.id, providerId: "codex", parentThreadId: thread.id });
    const scope = turnScope("turn-late-reason");
    db.transaction((tx) => {
      appendStoredThreadEventInTransaction(tx, { threadId: child.id, scope, type: "turn/started", data: { providerThreadId: "provider" } });
      appendStoredThreadEventInTransaction(tx, { threadId: child.id, scope, type: "turn/completed", data: { providerThreadId: "provider", status: "interrupted" } });
      appendStoredThreadEventInTransaction(tx, { threadId: child.id, scope, type: "system/error", data: { code: "thread_command_failed", message: "interrupted" } });
      appendStoredThreadEventInTransaction(tx, { threadId: child.id, scope: threadScope(), type: "system/thread/interrupted", data: { reason } });
    });
    const restarted = createConnection(db.$client.serialize()); connections.push(restarted);
    const pending = listUnreceiptedChildTurnCompletions(restarted);
    expect(pending).toHaveLength(reason === "manual-stop" ? 0 : 1);
    if (reason !== "manual-stop") {
      expect(JSON.parse(pending[0]!.data).status).toBe("interrupted");
      expect(JSON.parse(pending[0]!.interruptionData!)).toEqual({ reason });
    }
  });

  it("bounds automatic prompts and coalesced rows without discarding overflow", () => {
    const { db, thread, enqueue } = setup();
    const large = "x".repeat(24000);
    for (let i = 0; i < 6; i++) enqueue(large + i);
    const rows = listQueuedThreadMessages(db, thread.id);
    expect(rows).toHaveLength(4);
    expect(rows.every((row) => row.content.length < 64000)).toBe(true);
    const claimed = claimNextQueuedThreadMessageGroup(db, noopNotifier, thread.id, () => true)!;
    expect(claimed).toHaveLength(2);
    expect(listQueuedThreadMessages(db, thread.id)).toHaveLength(2);
    expect([...rows].map((row) => row.content).join("")).toContain(large + "5");
  });

  it("does not mutate a child outcome row while it is held for editing", () => {
    const { db, thread, child } = setup();
    const first = child("child-a:turn-edit", "original");
    holdQueuedThreadMessageForEdit(db, { id: first.queuedMessageId, threadId: thread.id, heldUntil: Date.now() + 60000 });
    const second = child("child-b:turn-edit", "new outcome");
    expect(second.queuedMessageId).not.toBe(first.queuedMessageId);
    expect(getQueuedThreadMessage(db, first.queuedMessageId)?.content).not.toContain("new outcome");
  });

});
