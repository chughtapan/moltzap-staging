/** @file Streams exact private assessments through operator-controlled storage access. */

import { Effect, Option, Schema, Stream } from "effect";
import { decodeJson } from "../encoding.js";
import {
  type FirewallStore,
  makeFirewallStore,
  persistenceFailure,
} from "./store.js";

/** A single query captures committed assessment IDs; immutable rows are fetched in bounded pages. */
export function assessmentAudit(agentKey: string) {
  return Stream.unwrap(
    Effect.gen(function* () {
      const store = yield* makeFirewallStore(agentKey);
      const sequences = yield* store.auditSequences();
      const throughSequence = sequences.at(-1) ?? 0;
      let count = 0;
      const records = Stream.paginateEffect(0, (offset) =>
        readPage(store, sequences, offset),
      ).pipe(
        Stream.mapConcat((rows) => rows),
        Stream.mapEffect((row) =>
          Effect.gen(function* () {
            const candidate = yield* decodeJson(Schema.Unknown, row.candidate);
            const input = yield* decodeJson(Schema.Unknown, row.input);
            const result = yield* decodeJson(Schema.Unknown, row.result);
            count += 1;
            return {
              kind: "assessment" as const,
              sequence: row.sequence,
              checkId: row.checkId,
              direction: row.direction,
              time: row.time,
              disposition: row.disposition,
              candidate,
              input,
              result,
            };
          }).pipe(Effect.mapError(persistenceFailure)),
        ),
      );
      return Stream.make({
        kind: "begin" as const,
        throughSequence,
        expectedCount: sequences.length,
      }).pipe(
        Stream.concat(records),
        Stream.concat(
          Stream.fromEffect(
            Effect.sync(() => ({
              kind: "complete" as const,
              throughSequence,
              count,
            })),
          ),
        ),
      );
    }).pipe(Effect.withSpan("assessmentAudit")),
  );
}

function readPage(
  store: FirewallStore,
  sequences: readonly number[],
  offset: number,
) {
  return Effect.gen(function* () {
    const requested = sequences.slice(offset, offset + 100);
    if (requested.length === 0) {
      return [[], Option.none<number>()] as const;
    }
    const rows = yield* store.exportAssessments(requested);
    if (
      rows.length !== requested.length ||
      rows.some((row, index) => row.sequence !== requested[index])
    ) {
      return yield* persistenceFailure();
    }
    return [rows, Option.some(offset + requested.length)] as const;
  });
}
