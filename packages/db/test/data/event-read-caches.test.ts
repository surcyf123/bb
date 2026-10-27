import { describe, expect, it, vi } from "vitest";
import {
  encodeClientTurnRequestIdNumber,
  LOCAL_BASH_TASK_TYPE,
  threadScope,
  turnScope,
} from "@bb/domain";
import { noopNotifier } from "../../src/notifier.js";
import {
  getStoredTurnRequestEventForTurn,
  getStoredTurnRequestEventForTurnUncached,
  insertEvents,
  listActiveBackgroundTaskCountsByThreadIds,
  listActiveBackgroundTaskCountsByThreadIdsUncached,
} from "../../src/data/events.js";
import { bumpThreadEventRewriteGeneration } from "../../src/data/event-rewrite-generation.js";
import { createProject } from "../../src/data/projects.js";
import { createThread } from "../../src/data/threads.js";
import { upsertHost } from "../../src/data/hosts.js";
import { createMigratedConnection } from "../helpers/migrated-connection.js";

function setup() {
  const db = createMigratedConnection();
  const host = upsertHost(db, noopNotifier, { name: "cache-host" });
  const { project } = createProject(db, noopNotifier, {
    name: "cache-project",
    source: { type: "local_path", hostId: host.id, path: "/tmp/cache" },
  });
  const newThread = () =>
    createThread(db, noopNotifier, { projectId: project.id, providerId: "codex" });
  return { db, newThread };
}

function taskEvent(args: {
  threadId: string;
  sequence: number;
  type: "item/started" | "item/backgroundTask/completed";
  itemId: string;
}) {
  return {
    threadId: args.threadId,
    sequence: args.sequence,
    scope: turnScope("turn-1"),
    type: args.type,
    itemId: args.itemId,
    itemKind: "backgroundTask" as const,
    parentToolCallId: null,
    data: JSON.stringify({
      item: {
        id: args.itemId,
        type: "backgroundTask",
        taskType: LOCAL_BASH_TASK_TYPE,
        description: "fixture background task",
        status: args.type === "item/started" ? "pending" : "completed",
        taskStatus: args.type === "item/started" ? "running" : "completed",
        skipTranscript: false,
      },
    }),
  };
}

function countActiveQueries(db: ReturnType<typeof setup>["db"], work: () => void): number {
  const prepare = vi.spyOn(db.$client, "prepare");
  try {
    work();
    return prepare.mock.calls.filter(([source]) =>
      String(source).includes("latest_background_task_state"),
    ).length;
  } finally {
    prepare.mockRestore();
  }
}

describe("active background-task count cache", () => {
  it("reuses counts until that thread's background-task events change", () => {
    const { db, newThread } = setup();
    const busy = newThread();
    const quiet = newThread();
    insertEvents(db, noopNotifier, [
      taskEvent({ threadId: busy.id, sequence: 1, type: "item/started", itemId: "task:a" }),
      taskEvent({ threadId: quiet.id, sequence: 1, type: "item/started", itemId: "task:b" }),
    ]);
    const threadIds = [busy.id, quiet.id];
    const read = () => listActiveBackgroundTaskCountsByThreadIds(db, { threadIds });

    expect(read()).toEqual(listActiveBackgroundTaskCountsByThreadIdsUncached(db, { threadIds }));
    expect(countActiveQueries(db, read)).toBe(0);

    insertEvents(db, noopNotifier, [
      taskEvent({ threadId: busy.id, sequence: 2, type: "item/backgroundTask/completed", itemId: "task:a" }),
    ]);
    let rows: ReturnType<typeof read> = [];
    expect(countActiveQueries(db, () => { rows = read(); })).toBe(1);
    expect(rows).toEqual(listActiveBackgroundTaskCountsByThreadIdsUncached(db, { threadIds }));
    expect(rows.map((row) => row.threadId)).toEqual([quiet.id]);
  });

  it("recounts after background-task events are deleted or history is rewritten", () => {
    const { db, newThread } = setup();
    const thread = newThread();
    insertEvents(db, noopNotifier, [
      taskEvent({ threadId: thread.id, sequence: 1, type: "item/started", itemId: "task:a" }),
      taskEvent({ threadId: thread.id, sequence: 2, type: "item/backgroundTask/completed", itemId: "task:a" }),
    ]);
    const threadIds = [thread.id];
    expect(listActiveBackgroundTaskCountsByThreadIds(db, { threadIds })).toEqual([]);

    db.$client
      .prepare("DELETE FROM events WHERE thread_id = ? AND sequence = 2")
      .run(thread.id);
    expect(listActiveBackgroundTaskCountsByThreadIds(db, { threadIds })).toEqual(
      listActiveBackgroundTaskCountsByThreadIdsUncached(db, { threadIds }),
    );
    expect(listActiveBackgroundTaskCountsByThreadIds(db, { threadIds })).toHaveLength(1);

    bumpThreadEventRewriteGeneration(thread.id);
    expect(
      countActiveQueries(db, () => listActiveBackgroundTaskCountsByThreadIds(db, { threadIds })),
    ).toBe(1);
  });
});

describe("indexed turn-request lookup", () => {
  function turnEvents(threadId: string, turn: number, sequence: number) {
    const requestId = encodeClientTurnRequestIdNumber({ value: turn });
    return [
      {
        threadId,
        sequence,
        scope: threadScope(),
        type: "client/turn/requested" as const,
        itemKind: null,
        itemId: null,
        parentToolCallId: null,
        data: JSON.stringify({ requestId, input: [] }),
      },
      {
        threadId,
        sequence: sequence + 1,
        scope: turnScope(`turn-${turn}`),
        type: "turn/input/accepted" as const,
        itemKind: null,
        itemId: null,
        parentToolCallId: null,
        data: JSON.stringify({ clientRequestId: requestId }),
      },
    ];
  }

  it("matches the full scan for every turn as requests are appended", () => {
    const { db, newThread } = setup();
    const thread = newThread();
    for (let turn = 1; turn <= 20; turn += 1) {
      insertEvents(db, noopNotifier, turnEvents(thread.id, turn, turn * 2 - 1));
      for (let check = 1; check <= turn; check += 1) {
        const args = { threadId: thread.id, turnId: `turn-${check}` };
        const indexed = getStoredTurnRequestEventForTurn(db, args);
        expect(indexed).not.toBeNull();
        expect(indexed).toEqual(getStoredTurnRequestEventForTurnUncached(db, args));
      }
    }
    expect(
      getStoredTurnRequestEventForTurn(db, { threadId: thread.id, turnId: "turn-missing" }),
    ).toBeNull();
  });

  it("falls back when an indexed request was deleted and reindexes after a rewrite", () => {
    const { db, newThread } = setup();
    const thread = newThread();
    insertEvents(db, noopNotifier, [
      ...turnEvents(thread.id, 1, 1),
      ...turnEvents(thread.id, 2, 3),
    ]);
    const args = { threadId: thread.id, turnId: "turn-2" };
    expect(getStoredTurnRequestEventForTurn(db, args)?.sequence).toBe(3);

    db.$client
      .prepare("DELETE FROM events WHERE thread_id = ? AND sequence = 3")
      .run(thread.id);
    expect(getStoredTurnRequestEventForTurn(db, args)).toBeNull();
    expect(getStoredTurnRequestEventForTurnUncached(db, args)).toBeNull();

    insertEvents(db, noopNotifier, [
      {
        ...turnEvents(thread.id, 2, 3)[0]!,
        sequence: 3,
      },
    ]);
    bumpThreadEventRewriteGeneration(thread.id);
    expect(getStoredTurnRequestEventForTurn(db, args)?.sequence).toBe(3);
  });

  it("finds a request written at a reused sequence after a history rewrite", () => {
    const { db, newThread } = setup();
    const thread = newThread();
    insertEvents(db, noopNotifier, [
      ...turnEvents(thread.id, 1, 1),
      ...turnEvents(thread.id, 2, 3),
    ]);
    const second = { threadId: thread.id, turnId: "turn-2" };
    expect(getStoredTurnRequestEventForTurn(db, second)?.sequence).toBe(3);

    db.$client
      .prepare("DELETE FROM events WHERE thread_id = ? AND sequence >= 3")
      .run(thread.id);
    bumpThreadEventRewriteGeneration(thread.id);
    insertEvents(db, noopNotifier, turnEvents(thread.id, 3, 3));

    const third = { threadId: thread.id, turnId: "turn-3" };
    expect(getStoredTurnRequestEventForTurn(db, third)).toEqual(
      getStoredTurnRequestEventForTurnUncached(db, third),
    );
    expect(getStoredTurnRequestEventForTurn(db, third)?.sequence).toBe(3);
    expect(getStoredTurnRequestEventForTurn(db, second)).toBeNull();
    expect(
      getStoredTurnRequestEventForTurn(db, { threadId: thread.id, turnId: "turn-1" }),
    ).toEqual(
      getStoredTurnRequestEventForTurnUncached(db, {
        threadId: thread.id,
        turnId: "turn-1",
      }),
    );
  });
});
