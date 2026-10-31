/**
 * @file Certified history: the records the engine builds from its folds, and
 * the stored rows it reads back and verifies.
 */

import { Effect } from "effect";
import type { Ed25519PublicKey } from "../../../identity/keys.js";
import {
  encodeRuntimeValue,
  EndpointStoreError,
  readRecentConversation,
} from "../../store/index.js";
import { readStoredHistory } from "./stored.js";

/** Records and store rows built from an in-memory fold. */
export {
  anchorHashAtHead,
  makeActionCertifiedRecord,
  makeCertifiedRecord,
  protocolEvidence,
  recordAnchorHash,
  stagedRecord,
  storedCertifiedRecord,
} from "./build.js";
/** Signer order, shared with the re-anchor certificate. */
export { orderedSignatures } from "./certificates.js";
/** Stored rows read back, verified, and queried. */
export {
  anchorRouterInstanceId,
  decodeStoredAnchor,
  durablePosition,
  durableRouterInstanceId,
  observedAnchorIsResolved,
  observedHeadIsResolved,
  readStoredHistory,
  readStoredMessageIntent,
  readStoredRecord,
  recordFromStore,
  stagedSuccessor,
  type StoredRowError,
  storedRowMatchesCore,
  verifyRecoveredHistory,
  verifyStoredMembership,
  verifyStoredMemberships,
  verifyStoredOutbounds,
} from "./stored.js";

/** Host visibility can join opaque positions without decoding protocol records. */
export function readRecentMessages(input: {
  readonly registrySignerPublicKey: Ed25519PublicKey;
  readonly conversationId: string;
  readonly maximumRecords: number;
  readonly candidateRecordHash?: string;
}) {
  return Effect.gen(function* () {
    const snapshot = yield* readRecentConversation(input);
    const verified = yield* readStoredHistory(
      input.registrySignerPublicKey,
      snapshot,
      snapshot.records,
    ).pipe(
      Effect.catchTags({
        ClientRepresentationError: () =>
          Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
        ParseError: () =>
          Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
        RouterWorkerPersistenceError: () =>
          Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
      }),
    );
    return yield* Effect.forEach(
      verified,
      (entry) => historyEntry(entry, snapshot.positions),
      { concurrency: 1 },
    );
  }).pipe(Effect.withSpan("readRecentMessages"));
}

function historyEntry(
  entry: Effect.Effect.Success<ReturnType<typeof readStoredHistory>>[number],
  positions: ReadonlyArray<{
    readonly recordHash: string;
    readonly sequence: number;
  }>,
) {
  return Effect.gen(function* () {
    const position = positions.find(
      (candidate) => candidate.recordHash === entry.recordHash,
    );
    if (position === undefined) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return {
      ...entry,
      sequence: position.sequence,
      bytes: (yield* encodeRuntimeValue(entry.message)).byteLength,
    };
  }).pipe(Effect.withSpan("recentHistoryEntry"));
}
