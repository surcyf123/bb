import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createConnection,
  createProject,
  createThread,
  deleteThreadEventSuffixInTransaction,
  getLatestStoredConversationOutlineSequence,
  getLatestThreadSequence,
  getThread,
  getThreadEventRewriteGeneration,
  getThreadEventStructuralGeneration,
  insertEvents,
  migrate,
  noopNotifier,
  upsertHost,
} from "@bb/db";
import { threadScope } from "@bb/domain";
import {
  buildThreadTimelineOffThread,
  closeOffThreadReaders,
  readConversationOutlineOffThread,
} from "../../../src/services/threads/off-thread-reads.js";
import {
  buildThreadTimelineWithProfile,
  loadThreadConversationOutline,
} from "../../../src/services/threads/timeline.js";
import {
  DEFAULT_MAX_INLINE_OUTPUT_CHARS,
  truncateTimelineResponseOutputs,
} from "../../../src/services/threads/timeline-output-truncation.js";
import { previewTimelineResponseOutputs } from "../../../src/services/threads/timeline-output-preview.js";

const workerUrl = new URL(
  "../../../src/services/threads/off-thread-read-worker.ts",
  import.meta.url,
);
const workerExecArgv = ["--import", "tsx", "--conditions=source"];
const roots: string[] = [];

afterEach(async () => {
  await closeOffThreadReaders();
  await Promise.all(
    roots.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function fixture(status?: "idle") {
  const root = await mkdtemp(join(tmpdir(), "bb-off-thread-"));
  roots.push(root);
  const databasePath = join(root, "bb.db");
  const db = createConnection(databasePath);
  migrate(db);
  const host = upsertHost(db, noopNotifier, { name: "off-thread-host" });
  const { project } = createProject(db, noopNotifier, {
    name: "off-thread",
    source: { type: "local_path", hostId: host.id, path: root },
  });
  const thread = createThread(db, noopNotifier, {
    projectId: project.id,
    providerId: "codex",
    ...(status === undefined ? {} : { status }),
  });
  const add = (sequence: number, text: string) =>
    insertEvents(db, noopNotifier, [
      {
        threadId: thread.id,
        sequence,
        scope: threadScope(),
        type: "system/manager/user_message",
        itemKind: null,
        itemId: null,
        parentToolCallId: null,
        data: JSON.stringify({ text }),
      },
    ]);
  const warn = vi.fn();
  const target = { databasePath, logger: { warn }, workerUrl, workerExecArgv };
  const readOutline = () =>
    readConversationOutlineOffThread(target, {
      threadId: thread.id,
      rewriteGeneration: getThreadEventRewriteGeneration(thread.id),
      structuralGeneration: getThreadEventStructuralGeneration(thread.id),
      completedTurnDisplay: "collapse",
    });
  const loadInProcess = () => {
    const current = getThread(db, thread.id);
    if (current === null) throw new Error("missing thread");
    return loadThreadConversationOutline(db, current, {
      completedTurnDisplay: "collapse",
      maxSeq: getLatestThreadSequence(db, { threadId: thread.id }),
      outlineSequence: getLatestStoredConversationOutlineSequence(db, {
        threadId: thread.id,
      }),
      materialize: false,
    });
  };
  return { db, thread, add, warn, target, readOutline, loadInProcess };
}

function previews(body: string | null): string[] {
  if (body === null) throw new Error("expected an off-thread outline");
  const parsed: { items: { preview: string }[] } = JSON.parse(body);
  return parsed.items.map((item) => item.preview);
}

describe("off-thread conversation outline reads", { timeout: 60_000 }, () => {
  it("returns the in-process outline and sees newly appended events", async () => {
    const { db, add, readOutline, loadInProcess } = await fixture();
    try {
      add(1, "First message");
      const body = await readOutline();
      expect(body).toBe(JSON.stringify(loadInProcess()));
      add(2, "Second message");
      expect(previews(await readOutline())).toEqual([
        "First message",
        "Second message",
      ]);
      expect(await readOutline()).toBe(JSON.stringify(loadInProcess()));
    } finally {
      db.$client.close();
    }
  });

  it("does not reuse an outline from before a history rewrite", async () => {
    const { db, thread, add, readOutline, loadInProcess } = await fixture("idle");
    try {
      add(1, "First message");
      add(2, "Original message");
      expect(previews(await readOutline())).toEqual([
        "First message",
        "Original message",
      ]);
      db.transaction((tx) =>
        deleteThreadEventSuffixInTransaction(tx, {
          threadId: thread.id,
          cutoffSequence: 2,
          oldMaxSequence: 2,
        }),
      );
      add(2, "Rewritten message");
      const body = await readOutline();
      expect(previews(body)).toEqual(["First message", "Rewritten message"]);
      expect(body).toBe(JSON.stringify(loadInProcess()));
    } finally {
      db.$client.close();
    }
  });

  it("keeps the main event loop responsive while building a large outline", async () => {
    const { db, add, readOutline } = await fixture();
    try {
      db.transaction(() => {
        for (let index = 1; index <= 1_500; index += 1) {
          add(index, `Message ${index} ${"x".repeat(250)}`);
        }
      });
      let ticks = 0;
      const timer = setInterval(() => {
        ticks += 1;
      }, 5);
      try {
        expect(previews(await readOutline())).toHaveLength(1_500);
        expect(ticks).toBeGreaterThan(1);
      } finally {
        clearInterval(timer);
      }
    } finally {
      db.$client.close();
    }
  });

  it("falls back and pauses after a broken worker fails repeatedly", async () => {
    const { db, thread, warn, target } = await fixture();
    try {
      const broken = {
        ...target,
        workerUrl: new URL("data:text/javascript,throw new Error('broken worker')"),
        workerExecArgv: [],
      };
      const read = () =>
        readConversationOutlineOffThread(broken, {
          threadId: thread.id,
          rewriteGeneration: 0,
          structuralGeneration: 0,
          completedTurnDisplay: "collapse",
        });
      for (let attempt = 0; attempt < 3; attempt += 1) {
        expect(await read()).toBeNull();
      }
      expect(warn).toHaveBeenCalledTimes(3);
      expect(await read()).toBeNull();
      expect(warn).toHaveBeenCalledTimes(3);
    } finally {
      db.$client.close();
    }
  });
});

describe("off-thread timeline builds", { timeout: 60_000 }, () => {
  it.each([
    { includeNestedRows: false, summaryOnly: false },
    { includeNestedRows: true, summaryOnly: false },
    { includeNestedRows: false, summaryOnly: true },
  ])("returns the in-process timeline for %o", async (view) => {
    const { db, thread, add, target } = await fixture();
    try {
      for (let index = 1; index <= 30; index += 1) add(index, `Message ${index}`);
      const current = getThread(db, thread.id);
      if (current === null) throw new Error("missing thread");
      const options = {
        completedTurnDisplay: "collapse" as const,
        eventBudget: 1_500,
        includeDiagnosticOperations: false,
        maxSeq: getLatestThreadSequence(db, { threadId: thread.id }),
        page: { kind: "latest" as const, segmentLimit: 20 },
        planCommand: null,
        ...view,
      };
      const built = await buildThreadTimelineOffThread(
        target,
        {
          threadId: thread.id,
          rewriteGeneration: getThreadEventRewriteGeneration(thread.id),
          structuralGeneration: getThreadEventStructuralGeneration(thread.id),
          thread: current,
          options,
        },
        JSON.stringify(view),
      );
      const { response } = buildThreadTimelineWithProfile(db, current, {
        ...options,
        maxInlineOutputChars: DEFAULT_MAX_INLINE_OUTPUT_CHARS,
      });
      const truncated = truncateTimelineResponseOutputs(
        response,
        DEFAULT_MAX_INLINE_OUTPUT_CHARS,
      );
      const expected = view.includeNestedRows
        ? truncated
        : previewTimelineResponseOutputs(truncated);
      expect(JSON.stringify(built?.full)).toBe(JSON.stringify(expected));
      expect(built?.profile).toBeDefined();
    } finally {
      db.$client.close();
    }
  });
});
