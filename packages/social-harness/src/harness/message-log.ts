/** @file The record of endpoint traffic: what the daemon delivered to its host and what the host sent. */

import { FileSystem } from "@effect/platform";
import { DateTime, Effect, Schema } from "effect";
import { CollectiveId } from "../communication/index.js";
import { MsgId } from "../communication/records/values.js";
import { exactStruct } from "../encoding/exact.js";
import { PermittedItem } from "../firewall/service.js";
import { SendRequest } from "../host-protocol/send-request.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their domain names with the nominal values they decode. */

/**
 * Accepted message identities or the send failure, with an operation id for
 * a collective question or answer.
 */
const historyExportSendOutcome = Schema.Union(
  exactStruct({
    kind: Schema.Literal("sent"),
    operationId: Schema.optionalWith(CollectiveId, { exact: true }),
    msgIds: Schema.Array(MsgId),
  }),
  exactStruct({ kind: Schema.Literal("failed"), error: Schema.String }),
);

/**
 * One line of the daemon's optional history export: an item as the daemon
 * published it, a completed `send` invocation with its input and outcome, or
 * the one line that says the export stopped. Readers decode the file line by
 * line with this schema rather than copying its shape.
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

/**
 * Sink for the daemon's optional history export. Recording never fails and
 * never blocks the daemon on its own outcome: an export that stops is the
 * sink's business, recorded in the file.
 */
export interface HistoryExportPort {
  readonly record: (record: HistoryExportRecord) => Effect.Effect<void>;
}

/** The export in place when the operator configured none: records vanish. */
export const noHistoryExport: HistoryExportPort = Object.freeze({
  record: () => Effect.void,
});

/**
 * Append complete JSON lines before exposing incoming items or returning sends.
 * The first write creates the file, and serialized appends cannot interleave.
 * The first failed append disables export for this process and attempts one
 * `export-failed` line. Export failure does not stop message delivery.
 */
export function makeHistoryExport(
  path: string,
): Effect.Effect<HistoryExportPort, never, FileSystem.FileSystem> {
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
    return {
      record: (record: HistoryExportRecord) =>
        gate.withPermits(1)(
          Effect.suspend(() =>
            enabled
              ? append(record).pipe(Effect.catchAll(disable))
              : Effect.void,
          ),
        ),
    };
  }).pipe(Effect.withSpan("makeHistoryExport"));
}
