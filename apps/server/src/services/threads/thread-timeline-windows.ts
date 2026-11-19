import { performance } from "node:perf_hooks";
import {
  getAppSettings,
  getLatestThreadSequence,
  getThread,
  getThreadEventRewriteGeneration,
} from "@bb/db";
import {
  DEFAULT_COMPLETED_TURN_DISPLAY,
  type AppSettings,
  type CompletedTurnDisplay,
  type Thread,
  type ThreadTimelineSubscriptionTarget,
} from "@bb/domain";
import {
  computeThreadTimelineDelta,
  isThreadTimelineDelta,
  type ThreadTimelineReadResponse,
  type ThreadTimelineResponse,
  type ThreadTimelineSignal,
} from "@bb/server-contract";
import type { AppDeps } from "../../types.js";
import type { ThreadTimelineSubscriber } from "../../ws/hub.js";
import { observeTimelinePushTrace } from "../system/turn-trace.js";
import {
  buildThreadTimelineWithProfile,
  resolveThreadTimelineSurface,
  THREAD_TIMELINE_DEFAULT_SEGMENT_LIMIT,
  THREAD_TIMELINE_SEGMENT_LIMIT_MAX,
} from "./timeline.js";
import { createSlowThreadTimelineBuildLogger } from "./timeline-build-log.js";
import {
  buildThreadTimelineCacheKey,
  buildThreadTimelineCacheRevision,
  createThreadTimelineWindowCache,
} from "./timeline-cache.js";
import { forgetTimelineGroupingContexts } from "./timeline-context-order.js";
import { forgetTimelineProjectionFolds } from "./timeline-projection-fold.js";
import { forgetLatestTimelineSelections } from "./timeline-selection-memo.js";
import { createTimelineServedWindowsCache } from "./timeline-served-windows-cache.js";
import { timelineRowHash, timelineWindowTag } from "./timeline-window-tag.js";
import { resolveThreadHeadStateOptions } from "./thread-head-state.js";
import { reuseUnchangedTimelineWindowParts } from "./timeline-window-reuse.js";
import {
  DEFAULT_MAX_INLINE_OUTPUT_CHARS,
  shortenTimelineResponseOutputs,
} from "./timeline-output-shortening.js";
import type { ThreadTimelinePageRequest } from "./timeline-pagination.js";
import {
  buildThreadTimelineOffThread,
  forgetOffThreadHistory,
  noteOffThreadHistoryChange,
  offThreadHistoryState,
  type OffThreadReadTarget,
  type OffThreadTimelineResult,
} from "./off-thread-reads.js";

type ThreadTimelineWindowDeps = Pick<
  AppDeps,
  "config" | "db" | "hub" | "logger" | "providerRegistry"
>;

export interface ThreadTimelineWindowRequest {
  deferContent: boolean;
  includeNestedRows: boolean;
  page: ThreadTimelinePageRequest;
}

const MAX_PUSH_PAUSE_MS = 1_000;

interface TimelinePush {
  signal: ThreadTimelineSignal;
  subscribers: ThreadTimelineSubscriber[];
}

interface TargetPushes {
  pushes: TimelinePush[];
  response: ThreadTimelineResponse;
  windowKey: string;
}

interface ThreadPushSchedule {
  nextAllowedAt: number;
  rerun: boolean;
  running: boolean;
  timer: ReturnType<typeof setTimeout> | null;
}

export interface ThreadTimelineWindows {
  read(
    thread: Thread,
    request: ThreadTimelineWindowRequest,
    afterTag: string | undefined,
  ): ThreadTimelineReadResponse;
  readOffThread(
    thread: Thread,
    request: ThreadTimelineWindowRequest,
    afterTag: string | undefined,
  ): Promise<ThreadTimelineReadResponse>;
}

export function resolveThreadProviderDisplayName(
  deps: Pick<AppDeps, "providerRegistry">,
  providerId: string,
): string | undefined {
  return deps.providerRegistry.get(providerId)?.info.displayName;
}

export function resolveThreadCompletedTurnDisplay(
  deps: Pick<AppDeps, "providerRegistry">,
  settings: AppSettings,
  providerId: string,
): CompletedTurnDisplay {
  return (
    settings.providerCompletedTurnDisplay[providerId] ??
    deps.providerRegistry.get(providerId)?.info.completedTurnDisplay ??
    DEFAULT_COMPLETED_TURN_DISPLAY
  );
}

function latestWindowKey(
  request: ThreadTimelineWindowRequest,
  segmentLimit: number,
): string {
  return [segmentLimit, request.deferContent, request.includeNestedRows].join(
    "|",
  );
}

export function createThreadTimelineWindows(
  deps: ThreadTimelineWindowDeps,
  offThreadTarget: OffThreadReadTarget | null = null,
): ThreadTimelineWindows {
  const timelineCache = createThreadTimelineWindowCache();
  const servedWindows = createTimelineServedWindowsCache({
    subscriberTags: (threadId) => deps.hub.listThreadTimelineTags(threadId),
  });
  const lastPushedByTarget = new WeakMap<
    ThreadTimelineSubscriptionTarget,
    ThreadTimelineResponse
  >();
  const pushSchedulesByThreadId = new Map<string, ThreadPushSchedule>();
  const slowTimelineBuildLogger = createSlowThreadTimelineBuildLogger({
    logger: deps.logger,
  });

  function prepareWindow(thread: Thread, request: ThreadTimelineWindowRequest) {
    const providerDisplayName = resolveThreadProviderDisplayName(
      deps,
      thread.providerId,
    );
    const settings = getAppSettings(deps.db);
    const includeDiagnosticOperations = settings.showDiagnosticEvents;
    const includeClearedContextHistory = settings.keepHistoryAfterContextClear;
    const completedTurnDisplay = resolveThreadCompletedTurnDisplay(
      deps,
      settings,
      thread.providerId,
    );
    const maxSeq = getLatestThreadSequence(deps.db, { threadId: thread.id });
    const surfaceOptions = {
      ...resolveThreadHeadStateOptions(deps, thread.providerId),
      completedTurnDisplay,
      deferContent: request.deferContent,
      includeClearedContextHistory,
      includeDiagnosticOperations,
      includeNestedRows: request.includeNestedRows,
      maxInlineOutputChars: DEFAULT_MAX_INLINE_OUTPUT_CHARS,
      providerDisplayName,
      summarizeShellCommands: settings.summarizeShellCommands,
    };
    const rewriteGeneration = getThreadEventRewriteGeneration(thread.id);
    return {
      buildOptions: {
        ...surfaceOptions,
        eventBudget: deps.config.featureFlags.timelineWindowEventBudget,
        maxSeq,
        page: request.page,
      },
      revision: buildThreadTimelineCacheRevision({
        maxSeq,
        page: request.page,
        rewriteGeneration,
        status: thread.status,
        surface: resolveThreadTimelineSurface(deps.db, thread, surfaceOptions)
          .cacheKey,
      }),
      rewriteGeneration,
      slot: buildThreadTimelineCacheKey({
        threadId: thread.id,
        page: request.page,
        deferContent: request.deferContent,
        includeNestedRows: request.includeNestedRows,
      }),
    };
  }

  async function buildWindowOffThread(
    thread: Thread,
    request: ThreadTimelineWindowRequest,
  ): Promise<ThreadTimelineResponse> {
    const prepared = prepareWindow(thread, request);
    if (
      offThreadTarget === null ||
      timelineCache.has(prepared.slot, prepared.revision)
    ) {
      return buildWindow(thread, request, prepared, null);
    }
    const offThread = await buildThreadTimelineOffThread(
      offThreadTarget,
      {
        history: offThreadHistoryState(thread.id, prepared.rewriteGeneration),
        options: prepared.buildOptions,
        thread,
      },
      `${prepared.slot}\0${prepared.revision}`,
    );
    return buildWindow(thread, request, prepared, offThread);
  }

  function buildWindow(
    thread: Thread,
    request: ThreadTimelineWindowRequest,
    prepared = prepareWindow(thread, request),
    offThread: OffThreadTimelineResult | null = null,
  ): ThreadTimelineResponse {
    return timelineCache.getOrBuild({
      threadId: thread.id,
      slot: prepared.slot,
      revision: prepared.revision,
      build: (previous) => {
        let expiresAt: number | null;
        let shortened;
        if (offThread === null) {
          const built = buildThreadTimelineWithProfile(
            deps.db,
            thread,
            prepared.buildOptions,
          );
          slowTimelineBuildLogger.log({
            cost: built.cost,
            profile: built.profile,
            threadId: thread.id,
          });
          expiresAt = built.expiresAt;
          shortened = shortenTimelineResponseOutputs(built.response, {
            maxInlineOutputChars: DEFAULT_MAX_INLINE_OUTPUT_CHARS,
            preview: !request.includeNestedRows,
          });
        } else {
          expiresAt = offThread.expiresAt;
          shortened = offThread.window;
        }
        const window = reuseUnchangedTimelineWindowParts(shortened, previous);
        return {
          expiresAt,
          value: { ...window, tag: timelineWindowTag(window) },
        };
      },
    });
  }

  function pushRequest(
    target: ThreadTimelineSubscriptionTarget,
  ): ThreadTimelineWindowRequest | null {
    const segmentLimit =
      target.segmentLimit ?? THREAD_TIMELINE_DEFAULT_SEGMENT_LIMIT;
    if (segmentLimit > THREAD_TIMELINE_SEGMENT_LIMIT_MAX) {
      return null;
    }
    if (listPushSubscribers(target).length === 0) {
      return null;
    }
    return {
      deferContent: target.deferContent,
      includeNestedRows: false,
      page: { kind: "latest", segmentLimit },
    };
  }

  function listPushSubscribers(
    target: ThreadTimelineSubscriptionTarget,
  ): ThreadTimelineSubscriber[] {
    return deps.hub
      .listThreadTimelineSubscribers(target)
      .filter(({ baseline }) => baseline.kind !== "pending");
  }

  function pushTarget(
    thread: Thread,
    target: ThreadTimelineSubscriptionTarget,
    request: ThreadTimelineWindowRequest,
    response: ThreadTimelineResponse,
  ): TargetPushes | null {
    const subscribers = listPushSubscribers(target);
    if (subscribers.length === 0 || request.page.kind !== "latest") {
      return null;
    }
    const windowKey = latestWindowKey(request, request.page.segmentLimit);
    const hasLegacySubscribers = subscribers.some(
      ({ baseline }) => baseline.kind === "legacy",
    );
    const legacyBaseline = hasLegacySubscribers
      ? (lastPushedByTarget.get(target) ??
        servedWindows.latest(thread.id, windowKey))
      : undefined;
    if (hasLegacySubscribers) {
      lastPushedByTarget.set(target, response);
    }
    const groups = new Map<
      string | null,
      {
        baseline: ThreadTimelineResponse | undefined;
        subscribers: ThreadTimelineSubscriber[];
      }
    >();
    for (const subscriber of subscribers) {
      const baseline =
        subscriber.baseline.kind === "tag"
          ? servedWindows.get(thread.id, subscriber.baseline.tag)
          : legacyBaseline;
      if (baseline?.tag === response.tag) {
        continue;
      }
      const groupKey = baseline?.tag ?? null;
      const group = groups.get(groupKey) ?? { baseline, subscribers: [] };
      group.subscribers.push(subscriber);
      groups.set(groupKey, group);
    }
    if (groups.size === 0) {
      return null;
    }
    const pushes = [...groups.values()].map(({ baseline, subscribers }) => ({
      subscribers,
      signal: {
        type: "thread-timeline" as const,
        target,
        afterSequence: null,
        timeline:
          baseline === undefined
            ? response
            : computeThreadTimelineDelta(baseline, response, timelineRowHash),
      },
    }));
    return { pushes, response, windowKey };
  }

  function listPushTargets(threadId: string): {
    targets: ThreadTimelineSubscriptionTarget[];
    thread: Thread;
  } | null {
    const targets = deps.hub
      .listWatchedTargets("thread-timeline")
      .filter((target) => target.threadId === threadId);
    if (targets.length === 0) {
      return null;
    }
    const thread = getThread(deps.db, threadId);
    if (thread === null || thread.deletedAt !== null) {
      return null;
    }
    return { targets, thread };
  }

  function sendTargetPushes(
    thread: Thread,
    targetPushes: TargetPushes | null,
    startedAt: number,
  ): void {
    if (targetPushes === null) {
      return;
    }
    const builtAt = performance.now();
    for (const { signal, subscribers } of targetPushes.pushes) {
      const bytes = deps.hub.sendThreadTimelineSignal(subscribers, signal);
      observeTimelinePushTrace(deps, {
        builtAt,
        bytes,
        maxSeq: signal.timeline.maxSeq,
        sentAt: performance.now(),
        startedAt,
        threadId: thread.id,
        withDelta: isThreadTimelineDelta(signal.timeline),
      });
    }
    servedWindows.recordPushed(
      thread.id,
      targetPushes.windowKey,
      targetPushes.response,
    );
  }

  function pushThread(threadId: string): void {
    const plan = listPushTargets(threadId);
    if (plan === null) return;
    for (const target of plan.targets) {
      const startedAt = performance.now();
      const request = pushRequest(target);
      if (request === null) continue;
      sendTargetPushes(
        plan.thread,
        pushTarget(
          plan.thread,
          target,
          request,
          buildWindow(plan.thread, request),
        ),
        startedAt,
      );
    }
  }

  async function pushThreadOffThread(threadId: string): Promise<void> {
    const plan = listPushTargets(threadId);
    if (plan === null) return;
    for (const target of plan.targets) {
      const startedAt = performance.now();
      const request = pushRequest(target);
      if (request === null) continue;
      const response = await buildWindowOffThread(plan.thread, request);
      sendTargetPushes(
        plan.thread,
        pushTarget(plan.thread, target, request, response),
        startedAt,
      );
    }
  }

  function finishScheduledPush(
    threadId: string,
    schedule: ThreadPushSchedule,
    startedAt: number,
  ): void {
    const finishedAt = performance.now();
    schedule.running = false;
    schedule.nextAllowedAt =
      finishedAt + Math.min(MAX_PUSH_PAUSE_MS, finishedAt - startedAt);
    if (schedule.rerun && pushSchedulesByThreadId.get(threadId) === schedule) {
      schedule.rerun = false;
      schedulePush(threadId);
    }
  }

  function warnPushFailed(threadId: string, error: unknown): void {
    deps.logger.warn(
      {
        threadId,
        error: error instanceof Error ? error.message : String(error),
      },
      "Failed to push thread timeline",
    );
  }

  function runScheduledPush(threadId: string): void {
    const schedule = pushSchedulesByThreadId.get(threadId);
    if (schedule === undefined) {
      return;
    }
    schedule.timer = null;
    const startedAt = performance.now();
    if (offThreadTarget === null) {
      try {
        pushThread(threadId);
      } catch (error) {
        warnPushFailed(threadId, error);
      }
      finishScheduledPush(threadId, schedule, startedAt);
      return;
    }
    schedule.running = true;
    void pushThreadOffThread(threadId)
      .catch((error: unknown) => warnPushFailed(threadId, error))
      .finally(() => finishScheduledPush(threadId, schedule, startedAt));
  }

  function schedulePush(threadId: string): void {
    const schedule = pushSchedulesByThreadId.get(threadId) ?? {
      nextAllowedAt: 0,
      rerun: false,
      running: false,
      timer: null,
    };
    pushSchedulesByThreadId.set(threadId, schedule);
    if (schedule.timer !== null) {
      return;
    }
    if (schedule.running) {
      schedule.rerun = true;
      return;
    }
    schedule.timer = setTimeout(
      () => runScheduledPush(threadId),
      Math.max(0, schedule.nextAllowedAt - performance.now()),
    );
  }

  deps.hub.onThreadTimelineTag((target, tag) => {
    servedWindows.release(target.threadId, tag);
    schedulePush(target.threadId);
  });

  deps.hub.onChangedMessage((message) => {
    if (message.entity !== "thread") {
      return;
    }
    if (message.changes.includes("thread-deleted")) {
      servedWindows.invalidateThread(message.id);
      timelineCache.invalidateThread(message.id);
      const schedule = pushSchedulesByThreadId.get(message.id);
      if (schedule?.timer) {
        clearTimeout(schedule.timer);
      }
      pushSchedulesByThreadId.delete(message.id);
      forgetOffThreadHistory(message.id);
      return;
    }
    if (
      message.changes.includes("events-appended") &&
      deps.hub
        .listWatchedTargets("thread-timeline")
        .some((target) => target.threadId === message.id)
    ) {
      schedulePush(message.id);
    }
    const rewritten = message.changes.includes("history-rewritten");
    if (!rewritten && !message.changes.includes("history-compacted")) {
      return;
    }
    noteOffThreadHistoryChange(
      message.id,
      rewritten ? "rewritten" : "compacted",
    );
    if (rewritten) {
      forgetTimelineGroupingContexts(deps.db, message.id);
    }
    forgetLatestTimelineSelections(deps.db, message.id);
    forgetTimelineProjectionFolds(deps.db, message.id);
    timelineCache.invalidateThread(message.id);
  });

  function serveWindow(
    thread: Thread,
    request: ThreadTimelineWindowRequest,
    afterTag: string | undefined,
    response: ThreadTimelineResponse,
  ): ThreadTimelineReadResponse {
    const previous =
      afterTag === undefined
        ? undefined
        : servedWindows.get(thread.id, afterTag);
    if (request.page.kind === "latest") {
      servedWindows.recordRead(
        thread.id,
        latestWindowKey(request, request.page.segmentLimit),
        response,
        afterTag,
      );
    }
    return previous === undefined
      ? response
      : computeThreadTimelineDelta(previous, response, timelineRowHash);
  }

  return {
    read(thread, request, afterTag) {
      return serveWindow(
        thread,
        request,
        afterTag,
        buildWindow(thread, request),
      );
    },
    async readOffThread(thread, request, afterTag) {
      return serveWindow(
        thread,
        request,
        afterTag,
        await buildWindowOffThread(thread, request),
      );
    },
  };
}
