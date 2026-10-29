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
  deleteThreadEventSuffixInTransaction,
  getLatestThreadSequence,
  insertEvents,
  listActiveBackgroundTaskCountsByThreadIds,
  listActiveBackgroundTaskCountsByThreadIdsUncached,
  listOpenTurnInputAcceptedRowsByThreadIds,
  listOpenTurnInputAcceptedRowsByThreadIdsUncached,
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

describe("incremental thread-list reads", () => {
  function createRandom(seed: number) {
    let state = seed;
    return () => {
      state = (state * 1103515245 + 12345) % 2147483648;
      return state / 2147483648;
    };
  }

  function randomEvent(random: () => number, threadId: string, sequence: number) {
    const turn = `turn-${1 + Math.floor(random() * 4)}`;
    const choice = random();
    const base = { threadId, sequence, itemKind: null, itemId: null, parentToolCallId: null };
    if (choice < 0.25) {
      return { ...base, scope: turnScope(turn), type: "turn/input/accepted" as const, data: JSON.stringify({ clientRequestId: encodeClientTurnRequestIdNumber({ value: sequence }) }) };
    }
    if (choice < 0.38) {
      return { ...base, scope: turnScope(turn), type: "turn/completed" as const, data: JSON.stringify({ status: "completed" }) };
    }
    if (choice < 0.45) {
      return { ...base, scope: threadScope(), type: "system/thread/interrupted" as const, data: "{}" };
    }
    if (choice < 0.7) {
      return taskEvent({ threadId, sequence, type: random() < 0.6 ? "item/started" : "item/backgroundTask/completed", itemId: `task:${Math.floor(random() * 5)}` });
    }
    return { ...base, scope: turnScope(turn), type: "item/agentMessage/delta" as const, itemId: "message", itemKind: "agentMessage" as const, data: JSON.stringify({ delta: "x", itemId: "message" }) };
  }

  it("matches the uncached queries across appends, idle threads and rewrites", () => {
    let openRowsSeen = 0;
    let countRowsSeen = 0;
    for (let seed = 1; seed <= 12; seed += 1) {
      const random = createRandom(seed);
      const { db, newThread } = setup();
      const threads = [newThread(), newThread(), newThread(), newThread()];
      const threadIds = threads.map((thread) => thread.id);
      for (let step = 0; step < 60; step += 1) {
        const thread = threads[Math.floor(random() * threads.length)]!;
        const latest = getLatestThreadSequence(db, { threadId: thread.id });
        if (step > 5 && random() < 0.06 && latest > 2) {
          const cutoffSequence = latest - Math.floor(random() * 3);
          db.transaction((tx) => {
            deleteThreadEventSuffixInTransaction(tx, {
              cutoffSequence,
              oldMaxSequence: latest,
              threadId: thread.id,
            });
          });
        } else if (random() < 0.8) {
          const count = 1 + Math.floor(random() * 3);
          insertEvents(
            db,
            noopNotifier,
            Array.from({ length: count }, (_, index) =>
              randomEvent(random, thread.id, latest + index + 1),
            ),
          );
        }
        for (const ids of [threadIds, threadIds.slice(0, 2)]) {
          const open = listOpenTurnInputAcceptedRowsByThreadIds(db, { threadIds: ids });
          expect(open, `open seed=${seed} step=${step}`).toEqual(
            listOpenTurnInputAcceptedRowsByThreadIdsUncached(db, { threadIds: ids }),
          );
          const counts = listActiveBackgroundTaskCountsByThreadIds(db, { threadIds: ids });
          expect(counts, `counts seed=${seed} step=${step}`).toEqual(
            listActiveBackgroundTaskCountsByThreadIdsUncached(db, { threadIds: ids }),
          );
          openRowsSeen += open.length;
          countRowsSeen += counts.length;
        }
      }
    }
    expect(openRowsSeen).toBeGreaterThan(100);
    expect(countRowsSeen).toBeGreaterThan(100);
  });

  it("skips background-task counting for threads without new events", () => {
    const { db, newThread } = setup();
    const idle = newThread();
    const busy = newThread();
    insertEvents(db, noopNotifier, [
      taskEvent({ threadId: idle.id, sequence: 1, type: "item/started", itemId: "task:a" }),
      taskEvent({ threadId: busy.id, sequence: 1, type: "item/started", itemId: "task:b" }),
    ]);
    const threadIds = [idle.id, busy.id];
    listActiveBackgroundTaskCountsByThreadIds(db, { threadIds });
    insertEvents(db, noopNotifier, [
      taskEvent({ threadId: busy.id, sequence: 2, type: "item/backgroundTask/completed", itemId: "task:b" }),
    ]);
    const prepare = vi.spyOn(db.$client, "prepare");
    try {
      expect(listActiveBackgroundTaskCountsByThreadIds(db, { threadIds })).toEqual(
        listActiveBackgroundTaskCountsByThreadIdsUncached(db, { threadIds }),
      );
      const watermark = prepare.mock.calls
        .map(([source]) => String(source))
        .find((source) => source.includes("COUNT(*) AS eventCount"));
      expect(watermark).toBeDefined();
      expect(prepare.mock.calls.filter(([source]) => String(source).includes("COUNT(*) AS eventCount"))).toHaveLength(1);
    } finally {
      prepare.mockRestore();
    }
  });

  it("reads only new accepted inputs for an open turn", () => {
    const { db, newThread } = setup();
    const thread = newThread();
    const accepted = (sequence: number, turn: string) => ({
      threadId: thread.id,
      sequence,
      scope: turnScope(turn),
      type: "turn/input/accepted" as const,
      itemKind: null,
      itemId: null,
      parentToolCallId: null,
      data: JSON.stringify({ clientRequestId: encodeClientTurnRequestIdNumber({ value: sequence }) }),
    });
    insertEvents(db, noopNotifier, [accepted(1, "turn-1"), accepted(2, "turn-2")]);
    const threadIds = [thread.id];
    expect(listOpenTurnInputAcceptedRowsByThreadIds(db, { threadIds })).toHaveLength(2);
    insertEvents(db, noopNotifier, [accepted(3, "turn-3")]);
    const prepare = vi.spyOn(db.$client, "prepare");
    try {
      expect(listOpenTurnInputAcceptedRowsByThreadIds(db, { threadIds })).toHaveLength(3);
      const accepts = prepare.mock.calls.filter(([source]) =>
        String(source).includes("completed_turn_for_accepted_input"),
      );
      expect(accepts).toHaveLength(1);
      expect(String(accepts[0]![0])).toMatch(/"sequence" > \?/u);
    } finally {
      prepare.mockRestore();
    }
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
