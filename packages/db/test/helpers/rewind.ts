import type { DbConnection } from "../../src/index.js";

export function dropPluginEnabledFollowsDefaultColumn(db: DbConnection): void {
  db.$client.exec("DROP TABLE IF EXISTS bb_local_child_notification_outbox; DROP TABLE IF EXISTS bb_local_child_notification_receipts;");
  const queueColumns = db.$client.prepare<[], { name: string }>("PRAGMA table_info(queued_thread_messages)").all();
  if (queueColumns.some((column) => column.name === "bb_local_originally_scheduled_at")) db.$client.exec("ALTER TABLE queued_thread_messages DROP COLUMN bb_local_originally_scheduled_at");
  const columns = db.$client
    .prepare<[], { name: string }>("PRAGMA table_info(plugins)")
    .all();
  if (columns.some((column) => column.name === "enabled_follows_default")) {
    db.$client.exec("ALTER TABLE plugins DROP COLUMN enabled_follows_default");
  }
}
