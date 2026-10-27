import type { DbConnection } from "../connection.js";

interface Cutover {
  id: string;
  from_event_rowid: number;
  through_event_rowid: number | null;
  state: string;
}

interface Source {
  rowid: number;
  id: string;
  thread_id: string;
  parent_thread_id: string;
  turn_id: string | null;
  sequence: number;
  type: string;
  data: string;
  created_at: number;
}

export interface QueueCutoverRecovery {
  phase: "none" | "complete" | "held" | "retry";
  recovered: number;
  held: number;
  error?: string;
}

export function recoverQueueCutover(db: DbConnection): QueueCutoverRecovery {
  try {
    return recover(db);
  } catch (error) {
    return { phase: "retry", recovered: 0, held: 0, error: error instanceof Error ? error.message : String(error) };
  }
}

function recover(db: DbConnection): QueueCutoverRecovery {
  const client = db.$client;
  if (!client.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bb_local_queue_cutovers'").get()) {
    return { phase: "none", recovered: 0, held: 0 };
  }
  const intents = client.prepare("SELECT * FROM bb_local_queue_cutovers WHERE state IN ('armed','retry') ORDER BY created_at").all() as Cutover[];
  const synchronous = client.pragma("synchronous", { simple: true }) as number;
  if (intents.length) client.pragma("synchronous = FULL");
  try {
  let recovered = 0;
  let held = 0;
  for (const intent of intents) {
    try {
      client.transaction(() => {
        client.prepare("UPDATE bb_local_queue_cutovers SET through_event_rowid=COALESCE(through_event_rowid,(SELECT COALESCE(MAX(rowid),0) FROM events)),updated_at=? WHERE id=?").run(Date.now(), intent.id);
      }).immediate();
      const captured = client.prepare("SELECT * FROM bb_local_queue_cutovers WHERE id=?").get(intent.id) as Cutover;
      const result = client.transaction(() => {
        const span = (captured.through_event_rowid ?? 0) - captured.from_event_rowid;
        if (span > 10_000) {
          client.prepare("UPDATE bb_local_queue_cutovers SET state='held',error=?,updated_at=? WHERE id=?").run(`Cutover range ${span} exceeds the 10000-event recovery bound; retained for operator reconciliation`, Date.now(), intent.id);
          return { added: 0, ambiguous: 1 };
        }
        const volume = client.prepare("SELECT COALESCE(SUM(length(data)),0) AS bytes,COALESCE(MAX(length(data)),0) AS largest FROM events WHERE rowid>? AND rowid<=?").get(captured.from_event_rowid, captured.through_event_rowid) as { bytes: number; largest: number };
        if (volume.bytes > 8_000_000 || volume.largest > 64_000) {
          client.prepare("UPDATE bb_local_queue_cutovers SET state='held',error=?,updated_at=? WHERE id=?").run("Cutover payload exceeds the bounded startup budget; retained for operator reconciliation", Date.now(), intent.id);
          return { added: 0, ambiguous: 1 };
        }
        let added = 0;
        let ambiguous = 0;
        const sources = client.prepare(`SELECT e.rowid,e.id,e.thread_id,child.parent_thread_id,e.turn_id,e.sequence,e.type,e.data,e.created_at
          FROM events e JOIN threads child ON child.id=e.thread_id JOIN threads parent ON parent.id=child.parent_thread_id
          WHERE e.rowid>? AND e.rowid<=? AND child.origin_kind IS NULL AND child.deleted_at IS NULL
            AND parent.deleted_at IS NULL AND parent.archived_at IS NULL
            AND ((e.type='turn/completed' AND e.scope_kind='turn' AND e.parent_tool_call_id IS NULL
              AND EXISTS(SELECT 1 FROM events started WHERE started.thread_id=e.thread_id AND started.turn_id=e.turn_id AND started.type='turn/started' AND started.scope_kind='turn' AND started.parent_tool_call_id IS NULL))
              OR (e.type='system/error' AND json_extract(e.data,'$.code') IN ('thread_command_failed','thread_provisioning_failed')))
          ORDER BY e.sequence,e.rowid`).all(captured.from_event_rowid, captured.through_event_rowid) as Source[];
        const latest = new Map<string, Source>();
        for (const source of sources) {
          const payload = JSON.parse(source.data);
          if (source.type === "turn/completed" && !["completed", "failed", "interrupted"].includes(payload.status)) continue;
          if (source.type === "system/error" && source.turn_id !== null && client.prepare("SELECT 1 FROM events WHERE thread_id=? AND turn_id=? AND type='turn/completed' AND scope_kind='turn' AND parent_tool_call_id IS NULL AND json_extract(data,'$.status')='interrupted'").get(source.thread_id, source.turn_id)) continue;
          latest.set(source.thread_id, source);
        }
        for (const source of latest.values()) {
          const payload = JSON.parse(source.data);
          const status = source.type === "turn/completed" ? payload.status : "failed";
          if (!["completed", "failed", "interrupted"].includes(status)) continue;
          if (source.type === "system/error" && source.turn_id !== null && client.prepare("SELECT 1 FROM events WHERE thread_id=? AND turn_id=? AND type='turn/completed' AND scope_kind='turn' AND parent_tool_call_id IS NULL AND json_extract(data,'$.status')='interrupted'").get(source.thread_id, source.turn_id)) continue;
          const interruption = source.turn_id === null ? undefined : client.prepare(`SELECT intr.data FROM events intr
            WHERE intr.thread_id=? AND intr.type='system/thread/interrupted'
              AND intr.sequence>COALESCE((SELECT MAX(sequence) FROM events WHERE thread_id=? AND turn_id=? AND type='turn/started' AND parent_tool_call_id IS NULL),9223372036854775807)
              AND intr.sequence<COALESCE((SELECT MIN(sequence) FROM events WHERE thread_id=? AND type='turn/started' AND parent_tool_call_id IS NULL AND sequence>?),9223372036854775807)
            ORDER BY intr.sequence DESC LIMIT 1`).get(source.thread_id, source.thread_id, source.turn_id, source.thread_id, source.sequence) as { data: string } | undefined;
          if (status === "interrupted" && interruption && JSON.parse(interruption.data).reason === "manual-stop") continue;
          const key = `${source.parent_thread_id}:${source.thread_id}:${source.turn_id ?? source.sequence}:${status}${source.turn_id === null ? ":" : ""}`;
          if (client.prepare("SELECT 1 FROM bb_local_child_notification_receipts WHERE source_key=?").get(key)) continue;
          const notices = client.prepare(`SELECT id FROM events WHERE thread_id=? AND type='client/turn/requested' AND rowid>? AND rowid<=?
            AND EXISTS(SELECT 1 FROM json_each(json_extract(events.data,'$.systemMessageSubject.outcomes')) AS outcome WHERE json_extract(outcome.value,'$.threadId')=? AND json_extract(outcome.value,'$.status')=?) LIMIT 2`).all(source.parent_thread_id, source.rowid, captured.through_event_rowid, source.thread_id, status) as { id: string }[];
          const queued = client.prepare(`SELECT id FROM queued_thread_messages WHERE thread_id=? AND system_notice IS NOT NULL
            AND EXISTS(SELECT 1 FROM json_each(json_extract(queued_thread_messages.system_notice,'$.subject.outcomes')) AS outcome WHERE json_extract(outcome.value,'$.threadId')=? AND json_extract(outcome.value,'$.status')=?) LIMIT 2`).all(source.parent_thread_id, source.thread_id, status) as { id: string }[];
          const evidence = { events: notices.map((notice) => notice.id), queue: queued.map((notice) => notice.id) };
          if (evidence.events.length || evidence.queue.length) {
            client.prepare("INSERT OR IGNORE INTO bb_local_queue_cutover_holds(source_key,cutover_id,source_event_id,evidence,created_at) VALUES (?,?,?,?,?)").run(key, intent.id, source.id, JSON.stringify(evidence), Date.now());
            client.prepare("DELETE FROM bb_local_child_notification_outbox WHERE source_key=?").run(key);
            ambiguous += 1;
            continue;
          }
          const data = source.type === "turn/completed" ? source.data : JSON.stringify({ status, ...(payload.code === "thread_provisioning_failed" ? { failureContext: "failed during workspace setup before a turn began" } : {}) });
          added += client.prepare("INSERT OR IGNORE INTO bb_local_child_notification_outbox(source_key,parent_thread_id,child_thread_id,turn_id,data,interruption_data,created_at) VALUES (?,?,?,?,?,?,?)").run(key, source.parent_thread_id, source.thread_id, source.turn_id, data, interruption?.data ?? null, source.created_at).changes;
        }
        client.prepare("UPDATE bb_local_queue_cutovers SET state=?,error=NULL,updated_at=? WHERE id=?").run(ambiguous ? "held" : "complete", Date.now(), intent.id);
        return { added, ambiguous };
      }).immediate();
      recovered += result.added;
      held += result.ambiguous;
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      client.prepare("UPDATE bb_local_queue_cutovers SET state='retry',error=?,updated_at=? WHERE id=?").run(detail.slice(0, 2000), Date.now(), intent.id);
      return { phase: "retry", recovered, held, error: detail };
    }
  }
  const hasHolds = client.prepare("SELECT 1 FROM bb_local_queue_cutovers WHERE state='held' LIMIT 1").get();
  return { phase: hasHolds ? "held" : intents.length ? "complete" : "none", recovered, held };
  } finally {
    if (intents.length) client.pragma(`synchronous = ${synchronous}`);
  }
}
