import { and, asc, eq, gt, isNull, or, lte, sql } from "drizzle-orm";
import { flattenPromptInputGroups, promptInputSchema, queuedMessageSystemNoticeSchema, type SystemMessageSubject } from "@bb/domain";
import type { DbConnection } from "../connection.js";
import type { DbNotifier } from "../notifier.js";
import { threads, childNotificationOutbox, childNotificationReceipts, queuedThreadMessages } from "../schema.js";
import { createQueuedThreadMessageInTransaction, recordQueueEnqueued, type CreateQueuedThreadMessageInput } from "./queued-thread-messages.js";

export function enqueueDurableChildNotification(
  db: DbConnection,
  notifier: DbNotifier,
  args: {
    sourceKey: string;
    childThreadId: string;
    delayMs: number;
    message: CreateQueuedThreadMessageInput;
  },
): { queuedMessageId: string; duplicate: boolean; sendAt: number } {
  const result = db.transaction((tx) => {
    const existing = tx.select().from(childNotificationReceipts).where(eq(childNotificationReceipts.sourceKey, args.sourceKey)).get();
    if (existing) {
      tx.delete(childNotificationOutbox).where(eq(childNotificationOutbox.sourceKey, args.sourceKey)).run();
      return { queuedMessageId: existing.queuedMessageId, duplicate: true, sendAt: 0 };
    }
    const now = Date.now();
    const source = tx.select({ createdAt: childNotificationOutbox.createdAt }).from(childNotificationOutbox)
      .where(eq(childNotificationOutbox.sourceKey, args.sourceKey)).get();
    const dueAt = Math.max(now, (source?.createdAt ?? now) + args.delayMs);
    const batch = dueAt > now ? tx.select({ row: queuedThreadMessages }).from(queuedThreadMessages)
      .innerJoin(childNotificationReceipts, eq(childNotificationReceipts.queuedMessageId, queuedThreadMessages.id))
      .where(and(
        eq(queuedThreadMessages.threadId, args.message.threadId),
        isNull(queuedThreadMessages.claimedAt),
        or(isNull(queuedThreadMessages.editHeldUntil), lte(queuedThreadMessages.editHeldUntil, now)),
        isNull(queuedThreadMessages.failureReason),
        gt(queuedThreadMessages.sendAt, now),
        eq(queuedThreadMessages.model, args.message.model),
        eq(queuedThreadMessages.reasoningLevel, args.message.reasoningLevel),
        eq(queuedThreadMessages.permissionMode, args.message.permissionMode),
        eq(queuedThreadMessages.serviceTier, args.message.serviceTier),
        sql`length(${queuedThreadMessages.content}) + ${JSON.stringify(args.message.content).length} <= 64000`,
        sql`json_extract(${queuedThreadMessages.systemNotice}, '$.kind') IN ('child-completed', 'child-outcome-batch')`,
      )).orderBy(asc(queuedThreadMessages.createdAt)).limit(1).get()?.row : undefined;
    let queuedMessageId: string;
    if (batch) {
      const oldNotice = queuedMessageSystemNoticeSchema.parse(JSON.parse(batch.systemNotice!));
      const outcomes = [...outcomesOf(oldNotice.subject), ...outcomesOf(args.message.systemNotice?.subject ?? null)];
      const content = flattenPromptInputGroups([promptInputSchema.array().parse(JSON.parse(batch.content)), args.message.content]);
      tx.update(queuedThreadMessages).set({
        content: JSON.stringify(content),
        sendAt: Math.min(batch.sendAt!, dueAt),
        originallyScheduledAt: Math.min(batch.sendAt!, dueAt),
        systemNotice: JSON.stringify({ kind: "child-outcome-batch", subject: { kind: "thread-batch", count: outcomes.length, outcomes } }),
        updatedAt: Math.max(now, batch.updatedAt + 1),
      }).where(eq(queuedThreadMessages.id, batch.id)).run();
      queuedMessageId = batch.id;
      recordQueueEnqueued(tx, batch, args.sourceKey, now, true);
    } else {
      queuedMessageId = createQueuedThreadMessageInTransaction(tx, {
        ...args.message,
        waitingOn: { kind: "time" },
        sendAt: dueAt,
      }).id;
    }
    tx.insert(childNotificationReceipts).values({
      sourceKey: args.sourceKey, parentThreadId: args.message.threadId,
      childThreadId: args.childThreadId, queuedMessageId, createdAt: now,
    }).run();
    tx.delete(childNotificationOutbox).where(eq(childNotificationOutbox.sourceKey, args.sourceKey)).run();
    return { queuedMessageId, duplicate: false, sendAt: Math.min(batch?.sendAt ?? dueAt, dueAt) };
  }, { behavior: "immediate" });
  if (!result.duplicate) notifier.notifyThread(args.message.threadId, ["queue-changed"]);
  return result;
}

function outcomesOf(subject: SystemMessageSubject | null) {
  return subject && (subject.kind === "thread" || subject.kind === "thread-batch") ? subject.outcomes ?? [] : [];
}

export function listUnreceiptedChildTurnCompletions(db: DbConnection) {
  return db.transaction((tx) => {
    tx.run(sql`DELETE FROM bb_local_child_notification_outbox WHERE
      EXISTS (SELECT 1 FROM bb_local_child_notification_receipts receipt WHERE receipt.source_key = bb_local_child_notification_outbox.source_key)
      OR EXISTS (SELECT 1 FROM threads parent WHERE parent.id = bb_local_child_notification_outbox.parent_thread_id AND (parent.archived_at IS NOT NULL OR parent.deleted_at IS NOT NULL))
      OR (json_extract(data, '$.status') = 'interrupted' AND json_extract(interruption_data, '$.reason') = 'manual-stop')`);
    return tx.select({
      childThreadId: childNotificationOutbox.childThreadId, parentThreadId: childNotificationOutbox.parentThreadId,
      projectId: threads.projectId, title: threads.title, turnId: childNotificationOutbox.turnId,
      data: childNotificationOutbox.data, sourceKey: childNotificationOutbox.sourceKey, interruptionData: childNotificationOutbox.interruptionData,
    }).from(childNotificationOutbox).innerJoin(threads, eq(threads.id, childNotificationOutbox.childThreadId))
      .orderBy(asc(childNotificationOutbox.createdAt)).limit(100).all();
  }, { behavior: "immediate" });
}
