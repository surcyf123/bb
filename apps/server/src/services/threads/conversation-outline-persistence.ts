import {
  upsertThreadConversationOutlineRecord,
  type DbConnection,
} from "@bb/db";
import {
  OutlineCacheWriter,
  type SlowOutlineWrite,
} from "./outline-cache-writer.js";

const writers = new WeakMap<DbConnection, OutlineCacheWriter>();
let reportSlowWrite: ((write: SlowOutlineWrite) => void) | undefined;

export function setOutlineCacheSlowWriteReporter(
  report: (write: SlowOutlineWrite) => void,
): void {
  reportSlowWrite = report;
}

export function persistThreadConversationOutline(
  db: DbConnection,
  record: Parameters<typeof upsertThreadConversationOutlineRecord>[1],
): void {
  if (db.$client.memory) {
    upsertThreadConversationOutlineRecord(db, record);
    return;
  }
  let writer = writers.get(db);
  if (!writer) {
    writer = new OutlineCacheWriter(db.$client.name, {
      onSlowWrite: (write) => reportSlowWrite?.(write),
    });
    writers.set(db, writer);
  }
  writer.enqueue(record);
}
