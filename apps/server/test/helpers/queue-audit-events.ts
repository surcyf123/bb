interface StoredEventLike {
  type: string;
  data: string;
}

function queueAuditMethod(row: StoredEventLike): string | null {
  if (row.type !== "provider/unhandled") return null;
  const data: unknown = JSON.parse(row.data);
  if (
    typeof data !== "object" ||
    data === null ||
    !("providerId" in data) ||
    data.providerId !== "bb-queue" ||
    !("rawType" in data) ||
    typeof data.rawType !== "string"
  ) {
    return null;
  }
  return data.rawType;
}

export function withoutQueueAuditEvents<T extends StoredEventLike>(
  rows: readonly T[],
): T[] {
  return rows.filter((row) => queueAuditMethod(row) === null);
}

export function queueAuditMethods(rows: readonly StoredEventLike[]): string[] {
  return rows.flatMap((row) => {
    const method = queueAuditMethod(row);
    return method === null ? [] : [method];
  });
}
