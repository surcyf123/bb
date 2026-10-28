import type { DbConnection } from "../connection.js";

const generationsByThreadId = new Map<string, number>();
const structuralGenerationsByThreadId = new Map<string, number>();

export function getThreadEventRewriteGeneration(threadId: string): number {
  return generationsByThreadId.get(threadId) ?? 0;
}

export function getThreadEventStructuralGeneration(threadId: string): number {
  return structuralGenerationsByThreadId.get(threadId) ?? 0;
}

export function bumpThreadEventRewriteGeneration(
  threadId: string,
  cause: "rewrite" | "prune" = "rewrite",
): void {
  generationsByThreadId.set(
    threadId,
    getThreadEventRewriteGeneration(threadId) + 1,
  );
  if (cause === "rewrite")
    structuralGenerationsByThreadId.set(
      threadId,
      getThreadEventStructuralGeneration(threadId) + 1,
    );
}

export function syncThreadEventRewriteGeneration(
  threadId: string,
  generation: number,
  structuralGeneration: number,
): void {
  generationsByThreadId.set(threadId, generation);
  structuralGenerationsByThreadId.set(threadId, structuralGeneration);
}

export function getDatabaseDataVersion(db: DbConnection): number {
  const version: unknown = db.$client.pragma("data_version", { simple: true });
  if (typeof version !== "number") {
    throw new Error("PRAGMA data_version did not return a number");
  }
  return version;
}
