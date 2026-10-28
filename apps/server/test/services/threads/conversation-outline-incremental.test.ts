import { describe, expect, it } from "vitest";
import {
  THREAD_CONTEXT_CLEAR_OPERATION,
  type CompletedTurnDisplay,
} from "@bb/domain";
import {
  deleteThreadEventSuffixInTransaction,
  createThread,
  noopNotifier,
  getLatestStoredConversationOutlineSequence,
  getLatestThreadSequence,
} from "@bb/db";
import {
  buildThreadConversationOutline,
  loadThreadConversationOutline,
} from "../../../src/services/threads/timeline.js";
import { configureConversationOutlineCache } from "../../../src/services/threads/conversation-outline-cache.js";
import {
  appendRows,
  withTestThread,
  type RowSpec,
  type TestThread,
} from "../../helpers/timeline-cache-fixture.js";

function started(turnId: string): RowSpec {
  return { type: "turn/started", turnId };
}

function completed(turnId: string): RowSpec {
  return { type: "turn/completed", turnId, data: { status: "completed" } };
}

function message(turnId: string, text: string): RowSpec {
  return {
    type: "item/completed",
    turnId,
    itemId: `message-${turnId}`,
    itemKind: "agentMessage",
    data: { item: { id: `message-${turnId}`, type: "agentMessage", text } },
  };
}

function delta(turnId: string, text: string): RowSpec {
  return {
    type: "item/agentMessage/delta",
    turnId,
    itemId: `message-${turnId}`,
    data: { itemId: `message-${turnId}`, delta: text },
  };
}

function request(
  requestId: string,
  expectedTurnId: string | null = null,
): RowSpec {
  return {
    type: "client/turn/requested",
    data: {
      direction: "outbound",
      requestId,
      source: "tell",
      initiator: "user",
      senderThreadId: null,
      input: [{ type: "text", text: "User request" }],
      target:
        expectedTurnId === null
          ? { kind: "new-turn" }
          : { kind: "steer", expectedTurnId },
      request: { method: "turn/start", params: {} },
      execution: {
        model: "gpt-5",
        reasoningLevel: "medium",
        permissionMode: "workspace-write",
        source: "client/turn/requested",
        serviceTier: "auto",
      },
    },
  };
}

function agentRequest(requestId: string): RowSpec {
  const row = request(requestId);
  return { ...row, data: { ...row.data, initiator: "agent" } };
}

function accepted(requestId: string, turnId: string): RowSpec {
  return {
    type: "turn/input/accepted",
    turnId,
    data: { clientRequestId: requestId },
  };
}

function seed(testThread: TestThread, count = 3): void {
  appendRows(
    testThread,
    Array.from({ length: count }, (_, i) => {
      const turnId = `turn-${i}`;
      const requestId = `creq_${i.toString(8).replaceAll("0", "a").replaceAll("1", "b").padStart(10, "a")}`;
      return [
        request(requestId),
        started(turnId),
        accepted(requestId, turnId),
        message(turnId, `Answer ${i}`),
        completed(turnId),
      ];
    }).flat(),
  );
  appendRows(testThread, [started("live"), delta("live", "Live")]);
}

function load(
  testThread: TestThread,
  completedTurnDisplay: CompletedTurnDisplay = "collapse",
) {
  const threadId = testThread.thread.id;
  return loadThreadConversationOutline(testThread.db, testThread.thread, {
    completedTurnDisplay,
    maxSeq: getLatestThreadSequence(testThread.db, { threadId }),
    outlineSequence: getLatestStoredConversationOutlineSequence(testThread.db, {
      threadId,
    }),
  });
}

function expectMatchesFull(
  testThread: TestThread,
  completedTurnDisplay: CompletedTurnDisplay = "collapse",
) {
  const result = load(testThread, completedTurnDisplay);
  expect(result).toEqual(
    buildThreadConversationOutline(testThread.coldDb, testThread.thread, {
      completedTurnDisplay,
      maxSeq: result.maxSeq,
    }),
  );
  return result;
}

function countSelectedEventRows(
  testThread: TestThread,
  run: () => void,
): number {
  const raw = testThread.db.$client;
  const originalPrepare = raw.prepare.bind(raw);
  let count = 0;
  Object.defineProperty(raw, "prepare", {
    configurable: true,
    writable: true,
    value: (source: string) => {
      const statement = originalPrepare(source);
      if (source.includes('from "events"') && source.includes('"created_at"')) {
        const originalAll = statement.all.bind(statement);
        statement.all = function (...params: unknown[]) {
          const rows = originalAll(...params);
          count += rows.length;
          return rows;
        };
      }
      return statement;
    },
  });
  try {
    run();
  } finally {
    raw.prepare = originalPrepare;
  }
  return count;
}

describe("incremental conversation outlines", () => {
  it.each([true, false])(
    "checkpoints historical background work and rebuilds for late updates (settled=%s)",
    (settled) => {
      withTestThread((testThread) => {
        const work = (index: number, complete: boolean): RowSpec => {
          const id = `historical-work-${index}`;
          return {
            type: complete ? "item/completed" : "item/started",
            turnId: `historical-turn-${index}`,
            itemId: id,
            itemKind: "backgroundTask",
            data: {
              item: {
                id,
                type: "backgroundTask",
                taskType: "local_bash",
                description: "Fixture work",
                status: complete ? "completed" : "pending",
                taskStatus: complete ? "completed" : "running",
                skipTranscript: false,
              },
            },
          };
        };
        appendRows(testThread, [
          ...Array.from({ length: 100 }, (_, index) => [
            started(`historical-turn-${index}`),
            work(index, false),
            ...(index === 0 && !settled ? [] : [work(index, true)]),
            message(`historical-turn-${index}`, `Historical answer ${index}`),
            completed(`historical-turn-${index}`),
          ]).flat(),
          started("live"),
          delta("live", "Live"),
        ]);
        expectMatchesFull(testThread);
        appendRows(testThread, [delta("live", " continued")]);
        const ordinary = countSelectedEventRows(testThread, () =>
          expectMatchesFull(testThread),
        );
        expect(ordinary).toBeLessThan(20);
        appendRows(testThread, [
          {
            type: "turn/started",
            turnId: "late-child",
            parentToolCallId: "historical-work-0",
            data: { parentToolCallId: "historical-work-0" },
          },
          {
            ...message("late-child", "Late answer"),
            parentToolCallId: "historical-work-0",
            data: {
              item: {
                id: "late-message",
                type: "agentMessage",
                text: "Late answer",
                parentToolCallId: "historical-work-0",
              },
            },
          },
        ]);
        const late = countSelectedEventRows(testThread, () =>
          expectMatchesFull(testThread),
        );
        expect(late).toBeGreaterThan(100);
        appendRows(testThread, [work(0, false)]);
        expectMatchesFull(testThread);
        appendRows(testThread, [work(0, true)]);
        expectMatchesFull(testThread);
      });
    },
  );

  it.each([950, 1050])(
    "preserves previews across delta compaction with %i historical deltas",
    (deltaCount) => {
      withTestThread((testThread) => {
        appendRows(testThread, [
          started("old"),
          ...Array.from({ length: deltaCount }, () => delta("old", "word ")),
          message("old", "Complete"),
          completed("old"),
          started("live"),
          delta("live", "First"),
          delta("live", " second"),
        ]);
        expectMatchesFull(testThread);
        appendRows(testThread, [message("live", ""), completed("live")]);
        expectMatchesFull(testThread);
        appendRows(testThread, [
          started("next"),
          ...Array.from({ length: 100 }, () => delta("next", "more ")),
        ]);
        expectMatchesFull(testThread);
      });
    },
  );

  it("rebuilds when a previously unaccepted request joins a later turn", () => {
    withTestThread((testThread) => {
      appendRows(testThread, [request("creq_abcdefghij")]);
      seed(testThread);
      expectMatchesFull(testThread);
      appendRows(testThread, [
        accepted("creq_abcdefghij", "live"),
        delta("live", " accepted"),
      ]);
      expectMatchesFull(testThread);
    });
  });

  it("only reads the live tail after a long completed history, including across turn boundaries", () => {
    withTestThread((testThread) => {
      seed(testThread, 100);
      expectMatchesFull(testThread);
      for (const rows of [
        [delta("live", " continuation")],
        [message("live", "Final answer"), completed("live")],
        [
          request("creq_abcdefghij"),
          started("next"),
          accepted("creq_abcdefghij", "next"),
          delta("next", "Next"),
        ],
        [delta("next", " update")],
      ]) {
        appendRows(testThread, rows);
        const count = countSelectedEventRows(testThread, () => {
          load(testThread);
        });
        expect(count).toBeGreaterThan(0);
        expect(count).toBeLessThan(20);
        expectMatchesFull(testThread);
      }
    });
  });

  it("keeps checkpointing after an early user message lands inside another turn", () => {
    withTestThread((testThread) => {
      appendRows(testThread, [
        started("early"),
        request("creq_earryaaaaa"),
        started("overlap"),
        accepted("creq_earryaaaaa", "overlap"),
        message("overlap", "Overlap answer"),
        completed("overlap"),
        message("early", "Early answer"),
        completed("early"),
      ]);
      seed(testThread, 100);
      expectMatchesFull(testThread);
      for (const rows of [
        [delta("live", " continuation")],
        [message("live", "Final answer"), completed("live")],
        [
          started("slow"),
          request("creq_taiiaaaaaa"),
          started("fast"),
          accepted("creq_taiiaaaaaa", "fast"),
          message("fast", "Fast answer"),
        ],
        [completed("fast"), message("slow", "Slow answer"), completed("slow")],
        [
          started("seeded"),
          agentRequest("creq_seedaaaaaa"),
          message("seeded", "Seeded answer"),
          completed("seeded"),
        ],
        [started("next"), delta("next", "Next")],
        [delta("next", " update")],
      ]) {
        appendRows(testThread, rows);
        const count = countSelectedEventRows(testThread, () => {
          load(testThread);
        });
        expect(count).toBeLessThan(40);
        expectMatchesFull(testThread);
      }
    });
  });

  it("keeps checkpointing past steers rejected by a later command failure", () => {
    withTestThread((testThread) => {
      seed(testThread, 20);
      appendRows(testThread, [
        message("live", "Live done"),
        completed("live"),
        started("busy"),
        request("creq_rejectaaaa", "busy"),
        request("creq_rejectbbbb", "busy"),
        accepted("creq_rejectaaaa", "busy"),
        {
          type: "system/error",
          turnId: "busy",
          data: { code: "thread_command_failed", message: "Steer failed" },
        },
        message("busy", "Busy answer"),
        completed("busy"),
      ]);
      expectMatchesFull(testThread);
      appendRows(
        testThread,
        Array.from({ length: 60 }, (_, i) => [
          started(`later-${i}`),
          message(`later-${i}`, `Later ${i}`),
          completed(`later-${i}`),
        ]).flat(),
      );
      appendRows(testThread, [started("tail"), delta("tail", "Tail")]);
      expectMatchesFull(testThread);
      for (const rows of [
        [delta("tail", " continuation")],
        [message("tail", "Final answer"), completed("tail")],
        [
          request("creq_abcdefghij"),
          started("next"),
          accepted("creq_abcdefghij", "next"),
          delta("next", "Next"),
        ],
        [delta("next", " update")],
      ]) {
        appendRows(testThread, rows);
        const count = countSelectedEventRows(testThread, () => {
          load(testThread);
        });
        expect(count).toBeLessThan(20);
        expectMatchesFull(testThread);
      }
    });
  });

  it.each([
    "rewind",
    "clear",
    "external rewrite",
    "metadata",
    "display",
    "late event",
    "late steer",
    "thread error",
    "nested",
  ] as const)(
    "matches a full rebuild after %s invalidates the frozen prefix",
    (change) =>
      withTestThread((testThread) => {
        seed(testThread);
        expectMatchesFull(testThread);
        switch (change) {
          case "rewind": {
            const maxSeq = getLatestThreadSequence(testThread.db, {
              threadId: testThread.thread.id,
            });
            testThread.db.transaction((tx) =>
              deleteThreadEventSuffixInTransaction(tx, {
                threadId: testThread.thread.id,
                cutoffSequence: 4,
                oldMaxSequence: maxSeq,
              }),
            );
            appendRows(testThread, [
              message("turn-0", "Replacement"),
              completed("turn-0"),
              started("replacement"),
              delta("replacement", "New"),
            ]);
            while (
              getLatestThreadSequence(testThread.db, {
                threadId: testThread.thread.id,
              }) < maxSeq
            ) {
              appendRows(testThread, [
                {
                  type: "system/manager/user_message",
                  data: { text: "Refilled history" },
                },
              ]);
            }
            break;
          }
          case "clear":
            appendRows(testThread, [
              {
                type: "system/operation",
                data: {
                  operation: THREAD_CONTEXT_CLEAR_OPERATION,
                  operationId: "clear",
                  status: "completed",
                  message: "Context cleared",
                },
              },
              started("fresh"),
              delta("fresh", "Fresh"),
            ]);
            break;
          case "external rewrite":
            testThread.coldDb.$client
              .prepare(
                "UPDATE events SET data = json_set(data, '$.item.text', 'Rewritten') WHERE thread_id = ? AND sequence = 4",
              )
              .run(testThread.thread.id);
            break;
          case "display":
            break;
          case "metadata":
            testThread.thread = {
              ...testThread.thread,
              title: "Renamed",
              status: "error",
            };
            break;
          case "late event":
            appendRows(testThread, [message("turn-0", "Late replacement")]);
            break;
          case "late steer":
            appendRows(testThread, [
              request("creq_abcdefghij", "turn-0"),
              accepted("creq_abcdefghij", "turn-0"),
            ]);
            break;
          case "thread error":
            appendRows(testThread, [
              { type: "system/error", data: { message: "Failed" } },
            ]);
            break;
          case "nested":
            appendRows(testThread, [
              {
                ...started("child"),
                parentToolCallId: "parent",
                data: { parentToolCallId: "parent" },
              },
              {
                ...message("child", "Child answer"),
                parentToolCallId: "parent",
                data: {
                  item: {
                    id: "message-child",
                    type: "agentMessage",
                    text: "Child answer",
                    parentToolCallId: "parent",
                  },
                },
              },
            ]);
            break;
        }
        expectMatchesFull(
          testThread,
          change === "display" ? "flat" : "collapse",
        );
      }),
  );

  it("evicts old checkpoints without changing their rebuilt outlines", () => {
    withTestThread((testThread) => {
      seed(testThread, 100);
      expectMatchesFull(testThread);
      const firstThread = testThread.thread;
      for (let index = 0; index < 16; index += 1) {
        testThread.thread = createThread(testThread.db, noopNotifier, {
          projectId: testThread.projectId,
          providerId: "codex",
          status: "active",
        });
        seed(testThread);
        load(testThread);
      }
      testThread.thread = firstThread;
      const count = countSelectedEventRows(testThread, () => {
        load(testThread);
      });
      expect(count).toBeGreaterThan(500);
      expectMatchesFull(testThread);
      appendRows(testThread, [delta("live", " after eviction")]);
      expect(
        countSelectedEventRows(testThread, () => {
          load(testThread);
        }),
      ).toBeLessThan(20);
      expectMatchesFull(testThread);
    });
  });

  it("preserves overlapping turns and pending steers as they complete", () => {
    withTestThread((testThread) => {
      seed(testThread);
      expectMatchesFull(testThread);
      for (const rows of [
        [
          request("creq_abcdefghij", "live"),
          started("overlapping"),
          delta("overlapping", "Other"),
        ],
        [message("live", "Finished"), completed("live")],
        [
          accepted("creq_abcdefghij", "overlapping"),
          message("overlapping", "Other finished"),
          completed("overlapping"),
        ],
        [started("last"), delta("last", "Last")],
        [started("another"), delta("another", "Another")],
        [message("another", "Another done"), completed("another")],
        [message("last", "Last done"), completed("last")],
        [started("final"), delta("final", "Final")],
      ]) {
        appendRows(testThread, rows);
        expectMatchesFull(testThread);
      }
    });
  });

  describe("in the off-thread read worker configuration", () => {
    function withWorkerConfiguration(
      run: () => void,
      auditRate: number = 1,
    ): void {
      configureConversationOutlineCache({
        maxEntries: 32,
        maxChars: 512_000_000,
        requireUnchangedDataVersion: false,
        auditRate,
        auditBurst: 0,
        auditReporter: {
          mismatch: (details) => {
            throw new Error(`outline audit mismatch ${JSON.stringify(details)}`);
          },
          summary: () => {},
        },
      });
      try {
        run();
      } finally {
        configureConversationOutlineCache({
          maxEntries: 16,
          maxChars: 8_000_000,
          requireUnchangedDataVersion: true,
          auditRate: 0,
          auditBurst: 0,
          auditReporter: null,
        });
      }
    }

    function backgroundWork(turnId: string, id: string, complete: boolean): RowSpec {
      return {
        type: complete ? "item/completed" : "item/started",
        turnId,
        itemId: id,
        itemKind: "backgroundTask",
        data: {
          item: {
            id,
            type: "backgroundTask",
            taskType: "local_bash",
            description: "Fixture work",
            status: complete ? "completed" : "pending",
            taskStatus: complete ? "completed" : "running",
            skipTranscript: false,
          },
        },
      };
    }

    function history(testThread: TestThread, turns: number): void {
      appendRows(
        testThread,
        Array.from({ length: turns }, (_, index) => [
          started(`old-${index}`),
          backgroundWork(`old-${index}`, `old-work-${index}`, false),
          message(`old-${index}`, `Old answer ${index}`),
          completed(`old-${index}`),
        ]).flat(),
      );
    }

    it("keeps reusing its checkpoint while other connections write", () =>
      withWorkerConfiguration(() =>
        withTestThread((testThread) => {
          history(testThread, 80);
          appendRows(testThread, [started("live"), delta("live", "Live")]);
          expectMatchesFull(testThread);
          testThread.coldDb.$client
            .prepare("UPDATE threads SET updated_at = updated_at WHERE id = ?")
            .run(testThread.thread.id);
          appendRows(testThread, [
            backgroundWork("live", "live-work", false),
            delta("live", " more"),
          ]);
          const rows = countSelectedEventRows(testThread, () =>
            expectMatchesFull(testThread),
          );
          expect(rows).toBeLessThan(20);
        }),
      0,
      ));

    it("rebuilds when a frozen event is deleted without a rewrite", () =>
      withWorkerConfiguration(() =>
        withTestThread((testThread) => {
          history(testThread, 20);
          appendRows(testThread, [started("live"), delta("live", "Live")]);
          expectMatchesFull(testThread);
          testThread.coldDb.$client
            .prepare(
              "DELETE FROM events WHERE thread_id = ? AND item_id = 'message-old-3'",
            )
            .run(testThread.thread.id);
          const rows = countSelectedEventRows(testThread, () =>
            expectMatchesFull(testThread),
          );
          expect(rows).toBeGreaterThan(40);
        }),
      ));

    it("rebuilds when late work updates a frozen background task", () =>
      withWorkerConfiguration(() =>
        withTestThread((testThread) => {
          history(testThread, 20);
          appendRows(testThread, [started("live"), delta("live", "Live")]);
          expectMatchesFull(testThread);
          appendRows(testThread, [backgroundWork("live", "old-work-2", true)]);
          expectMatchesFull(testThread);
        }),
      ));

    it("keeps its checkpoint across thread status changes", () =>
      withWorkerConfiguration(() =>
        withTestThread((testThread) => {
          history(testThread, 40);
          appendRows(testThread, [started("live"), delta("live", "Live")]);
          expectMatchesFull(testThread);
          for (const status of ["idle", "active", "idle"] as const) {
            testThread.thread = { ...testThread.thread, status };
            appendRows(testThread, [delta("live", ` ${status}`)]);
            const rows = countSelectedEventRows(testThread, () =>
              expectMatchesFull(testThread),
            );
            expect(rows).toBeLessThan(20);
          }
        }),
      0,
      ));
  });
});
