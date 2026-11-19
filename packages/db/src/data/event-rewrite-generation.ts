import { eq } from "drizzle-orm";
import type { DbConnection, DbQueryConnection } from "../connection.js";
import { threadConversationOutlines } from "../schema.js";

const generationsByThreadId = new Map<string, number>();
const structuralGenerationsByThreadId = new Map<string, number>();

export function getThreadEventRewriteGeneration(threadId: string): number {
  return generationsByThreadId.get(threadId) ?? 0;
}

export function getThreadEventStructuralGeneration(threadId: string): number {
  return structuralGenerationsByThreadId.get(threadId) ?? 0;
}

export function bumpThreadEventRewriteGeneration(
  db: DbQueryConnection,
  threadId: string,
  cause: "rewrite" | "prune" = "rewrite",
): void {
  if (cause === "rewrite") {
    structuralGenerationsByThreadId.set(
      threadId,
      getThreadEventStructuralGeneration(threadId) + 1,
    );
  }
  db.delete(threadConversationOutlines)
    .where(eq(threadConversationOutlines.threadId, threadId))
    .run();
  generationsByThreadId.set(
    threadId,
    getThreadEventRewriteGeneration(threadId) + 1,
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

export function forgetThreadEventRewriteGenerations(): void {
  generationsByThreadId.clear();
  structuralGenerationsByThreadId.clear();
}

let dataVersionPinned = false;

export function pinDatabaseDataVersion(): void {
  dataVersionPinned = true;
}

export function getDatabaseDataVersion(db: DbConnection): number {
  if (dataVersionPinned) return 0;
  const version: unknown = db.$client.pragma("data_version", { simple: true });
  if (typeof version !== "number") {
    throw new Error("PRAGMA data_version did not return a number");
  }
  return version;
}
