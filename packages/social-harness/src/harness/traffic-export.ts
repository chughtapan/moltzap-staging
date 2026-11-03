/** @file The record of endpoint traffic: what the daemon delivered to its host and what the host sent. */

import { FileSystem } from "@effect/platform";
import { DateTime, Effect, Schema } from "effect";
import { MsgId } from "../communication/messages.js";
import { CollectiveId } from "../communication/service.js";
import { exactStruct } from "../encoding/canonical-json.js";
import { PermittedItem } from "../firewall/service.js";
import { SendRequest } from "./message-format.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their domain names with the nominal values they decode. */

const historyExportSendOutcome = Schema.Union(
  exactStruct({
    kind: Schema.Literal("sent"),
    operationId: Schema.optionalWith(CollectiveId, { exact: true }),
    msgIds: Schema.Array(MsgId),
  }),
  exactStruct({ kind: Schema.Literal("failed"), error: Schema.String }),
);

/**
 * Decodes each JSON line as a delivered item, completed send with input and
 * outcome, or export-failure marker.
 */
export const HistoryExportRecord = Schema.Union(
  exactStruct({
    kind: Schema.Literal("inbound"),
    item: PermittedItem,
    at: Schema.DateTimeUtc,
  }),
  exactStruct({
    kind: Schema.Literal("outbound"),
    input: SendRequest,
    outcome: historyExportSendOutcome,
    at: Schema.DateTimeUtc,
  }),
  exactStruct({
    kind: Schema.Literal("export-failed"),
    reason: Schema.String,
    at: Schema.DateTimeUtc,
  }),
).annotations({ identifier: "HistoryExportRecord" });
/** A validated line of the daemon's history export. */
export type HistoryExportRecord = typeof HistoryExportRecord.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules after the Schema/type pairs. */

const encodeLine = Schema.encode(Schema.parseJson(HistoryExportRecord));

/** Writes optional traffic history without failing message delivery. */
export type HistoryExport = (
  record: HistoryExportRecord,
) => Effect.Effect<void>;

/**
 * Appends complete JSON lines before exposing items or returning sends.
 * The first write creates the file. Serialized appends cannot interleave.
 * A failed append disables export for this process and attempts one
 * `export-failed` line without interrupting delivery.
 */
export function makeHistoryExport(
  path: string,
): Effect.Effect<HistoryExport, never, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fileSystem = yield* FileSystem.FileSystem;
    const gate = yield* Effect.makeSemaphore(1);
    let enabled = true;
    const append = (record: HistoryExportRecord) =>
      encodeLine(record).pipe(
        Effect.flatMap((line) =>
          fileSystem.writeFileString(path, `${line}\n`, { flag: "a" }),
        ),
      );
    const disable = (cause: { readonly message: string }) =>
      Effect.gen(function* () {
        enabled = false;
        yield* Effect.logWarning(`history export stopped: ${cause.message}`);
        const at = yield* DateTime.now;
        yield* append({
          kind: "export-failed",
          reason: cause.message,
          at,
        }).pipe(Effect.ignore);
      });
    return (record: HistoryExportRecord) =>
      gate.withPermits(1)(
        Effect.suspend(() =>
          enabled ? append(record).pipe(Effect.catchAll(disable)) : Effect.void,
        ),
      );
  }).pipe(Effect.withSpan("makeHistoryExport"));
}
