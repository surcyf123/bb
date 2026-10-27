import { inArray } from "drizzle-orm";
import { appSettingsSchema, defaultAppSettings } from "@bb/domain";
import type { DbQueryConnection } from "../connection.js";
import { appSettingsValues } from "../schema.js";

const schema = appSettingsSchema.pick({
  batchReadyQueuedMessages: true,
  readyQueueSenderCap: true,
  childOutcomeBatchWindowMs: true,
});

export function getQueueDeliverySettings(db: DbQueryConnection) {
  const values: Record<string, unknown> = {
    batchReadyQueuedMessages: defaultAppSettings.batchReadyQueuedMessages,
    readyQueueSenderCap: defaultAppSettings.readyQueueSenderCap,
    childOutcomeBatchWindowMs: defaultAppSettings.childOutcomeBatchWindowMs,
  };
  for (const row of db.select().from(appSettingsValues).where(inArray(appSettingsValues.key, Object.keys(values))).all()) {
    try {
      values[row.key] = JSON.parse(row.value);
    } catch {
      continue;
    }
  }
  return schema.parse(values);
}
