/** @file History pages retain their certified head while cursors survive replay and restart. */

import { assert, live as it } from "@effect/vitest";
import { Array, Effect } from "effect";
import type { CertifiedRecord } from "./index.js";
import {
  bytes,
  stateDirectory,
  storedConversation,
  type TestStore,
  withDaemonStore,
} from "../../__tests__/store-schema-fixtures.js";

const { foundation, record } = storedConversation("history-cursor");
const identity = {
  agentId: "agent:alice",
  canonicalAgentCard: bytes("identity"),
};

const laterEvidence = {
  conversationId: foundation.conversationId,
  kind: "durability" as const,
  subjectId: "record:52",
  evidenceKey: "agent:carol",
  canonicalEvidence: bytes("later signed evidence"),
};

it(
  "replays the same bounded certified prefix after appends, evidence growth and restart",
  replaysHistory,
);
it("rejects an unreadable cursor instead of starting a different history read", () =>
  withDaemonStore(stateDirectory(), rejectsUnreadableCursor));
function replaysHistory() {
  return Effect.gen(function* () {
    const directory = stateDirectory();
    const continuation = yield* withDaemonStore(
      directory,
      appendAfterFirstPage,
    );
    yield* withDaemonStore(directory, (store) =>
      expectReplay(store, continuation),
    );
  });
}
function appendAfterFirstPage(store: TestStore) {
  return Effect.gen(function* () {
    yield* store.bindIdentity(identity);
    yield* store.putConversationFoundation(foundation);
    yield* Effect.forEach(
      Array.range(0, 52),
      (position) => append(store, position),
      { concurrency: 1, discard: true },
    );
    const first = yield* store.readConversation({
      conversationId: foundation.conversationId,
    });
    assert.lengthOf(first.records, 50);
    assert.strictEqual(
      first.records[0]?.recordHash,
      historyRecord(0).recordHash,
    );
    assert.strictEqual(
      first.records.at(-1)?.recordHash,
      historyRecord(49).recordHash,
    );
    assert.isNotNull(first.continuation);
    const cursor = yield* Effect.fromNullable(first.continuation).pipe(
      Effect.orDie,
    );
    yield* append(store, 53);
    yield* store.mergeEvidence(laterEvidence);
    return cursor;
  });
}
function expectReplay(store: TestStore, continuation: string) {
  return Effect.gen(function* () {
    const second = yield* store.readConversation({ continuation });
    const replay = yield* store.readConversation({ continuation });
    assert.deepStrictEqual(
      second.records.map((item) => item.recordHash),
      ["record:50", "record:51", "record:52"],
    );
    assert.deepStrictEqual(replay, second);
    assert.isNull(second.continuation);
    assert.deepStrictEqual(
      second.records.at(-1)?.canonicalRecordCore,
      bytes("original record 52"),
    );
    assert.isTrue(
      second.records
        .at(-1)
        ?.durabilityEvidence.some(
          (item) => item.evidenceKey === laterEvidence.evidenceKey,
        ) ?? false,
    );
  });
}
function rejectsUnreadableCursor(store: TestStore) {
  return Effect.gen(function* () {
    const failure = yield* store
      .readConversation({ continuation: "invalid cursor" })
      .pipe(Effect.flip);
    assert.include(failure, { reason: "invalid-continuation" });
  });
}
/** Records arrive through the same certified-record operation used during recovery. */
function append(store: TestStore, position: number) {
  return store.applyCertifiedRecord(historyRecord(position));
}

/** Each immutable record has separate action and durability evidence. */
function historyRecord(position: number): CertifiedRecord {
  const recordHash = `record:${position}`;
  const actionHash = `action:${position}`;
  return {
    ...record,
    recordHash,
    actionHash,
    msgId: `message:${position}`,
    ...(position === 0 ? {} : { previousRecordHash: `record:${position - 1}` }),
    canonicalRecordCore: bytes(`original record ${position}`),
    actionEvidence: record.actionEvidence.map((item) => ({
      ...item,
      subjectId: actionHash,
    })),
    durabilityEvidence: record.durabilityEvidence.map((item) => ({
      ...item,
      subjectId: recordHash,
    })),
  };
}
