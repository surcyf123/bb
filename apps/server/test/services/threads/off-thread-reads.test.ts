import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  createConnection,
  createProject,
  createThread,
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
import { threadScope, type Thread } from "@bb/domain";
import {
  closeOffThreadReaders,
  loadConversationOutlineOffThread,
  offThreadHistoryState,
} from "../../../src/services/threads/off-thread-reads.js";
import {
  createThreadTimelineWindows,
  type ThreadTimelineWindowRequest,
} from "../../../src/services/threads/thread-timeline-windows.js";
import { loadThreadConversationOutline } from "../../../src/services/threads/timeline.js";
import {
  createTestAppHarness,
  type TestAppHarness,
} from "../../helpers/test-app.js";

const workerUrl = new URL(
  "../../../src/services/threads/off-thread-read-worker.ts",
  import.meta.url,
);
const workerExecArgv = ["--import", "tsx", "--conditions=source"];
const request: ThreadTimelineWindowRequest = {
  deferContent: true,
  includeNestedRows: false,
  page: { kind: "latest", segmentLimit: 20 },
};
const roots: string[] = [];
let harness: TestAppHarness;

beforeAll(async () => {
  harness = await createTestAppHarness();
});

afterAll(async () => {
  await harness.cleanup();
});

afterEach(async () => {
  await closeOffThreadReaders();
  await Promise.all(
    roots.splice(0).map((path) => rm(path, { recursive: true, force: true })),
  );
});

async function fixture() {
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
  const created = createThread(db, noopNotifier, {
    projectId: project.id,
    providerId: "codex",
  });
  const add = (sequence: number, text: string) =>
    insertEvents(db, noopNotifier, [
      {
        threadId: created.id,
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
  const thread = () => getThread(db, created.id) as Thread;
  return { add, db, target, thread, warn };
}

describe("off-thread reads", { timeout: 60_000 }, () => {
  it("builds the same timeline window as the in-process path and follows new events", async () => {
    const { add, db, target, thread, warn } = await fixture();
    try {
      add(1, "First message");
      const deps = { ...harness.deps, db };
      const offThread = createThreadTimelineWindows(deps, target);
      const inProcess = createThreadTimelineWindows(deps);
      expect(
        JSON.stringify(
          await offThread.readOffThread(thread(), request, undefined),
        ),
      ).toBe(JSON.stringify(inProcess.read(thread(), request, undefined)));
      add(2, "Second message");
      const second = await offThread.readOffThread(
        thread(),
        request,
        undefined,
      );
      expect(JSON.stringify(second)).toBe(
        JSON.stringify(inProcess.read(thread(), request, undefined)),
      );
      expect(JSON.stringify(second)).toContain("Second message");
      expect(warn).not.toHaveBeenCalled();
    } finally {
      db.$client.close();
    }
  });

  it("loads the same conversation outline as the in-process path", async () => {
    const { add, db, target, thread, warn } = await fixture();
    try {
      add(1, "First message");
      add(2, "Second message");
      const options = {
        completedTurnDisplay: "collapse" as const,
        includeClearedContextHistory: false,
        maxSeq: getLatestThreadSequence(db, { threadId: thread().id }),
        outlineSequence: getLatestStoredConversationOutlineSequence(db, {
          threadId: thread().id,
        }),
      };
      const items = await loadConversationOutlineOffThread(
        target,
        {
          history: offThreadHistoryState(
            thread().id,
            getThreadEventRewriteGeneration(thread().id),
            getThreadEventStructuralGeneration(thread().id),
          ),
          options,
          thread: thread(),
        },
        "outline",
      );
      expect(items).toEqual(
        loadThreadConversationOutline(db, thread(), options).items,
      );
      expect(items).toHaveLength(2);
      expect(warn).not.toHaveBeenCalled();
    } finally {
      db.$client.close();
    }
  });

  it("falls back to the in-process build when the worker cannot start", async () => {
    const { add, db, target, thread, warn } = await fixture();
    try {
      add(1, "First message");
      const deps = { ...harness.deps, db };
      const broken = createThreadTimelineWindows(deps, {
        ...target,
        workerUrl: new URL("./missing-worker.js", import.meta.url),
      });
      const response = await broken.readOffThread(thread(), request, undefined);
      expect(JSON.stringify(response)).toContain("First message");
      expect(warn).toHaveBeenCalled();
    } finally {
      db.$client.close();
    }
  });
});
