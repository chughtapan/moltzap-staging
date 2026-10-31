/** @file Exact preflight, proposal-lock, certification, and delivery tests. */

import { live as it } from "@effect/vitest";
import { Effect } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Tests create and inspect exact real-SQLite permission fixtures around the scoped Effect resource.
import { readFileSync, statSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { describe, expect } from "vitest";
import {
  bytes,
  databasePath,
  stateDirectory,
  type TestStore,
  withDaemonStore,
} from "../../__tests__/store-schema-fixtures.js";
import { WIRE_VERSION } from "../../identity/wire-version.js";
import {
  type CertifiedRecord,
  type ConversationFoundation,
  type EmptyConversationRestart,
  type EndpointRecovery,
  EndpointStoreError,
  type MessageIntent,
  type OutboundMessageInput,
  type ProposalLock,
  type ProtocolEvidence,
  type RestartedEmptyConversation,
  type StagedReanchor,
  type StagedRecord,
  type StoreMutation,
} from "./index.js";
const DATABASE_FILE_MODE = 0o600;
const DIRECTORY_MODE = 0o700;
const EXISTING_MUTATION: StoreMutation = "existing";
const INSERTED_MUTATION: StoreMutation = "inserted";
const LOCAL_AGENT_ID = "agent:local";
function initializesEmptyDatabase() {
  const directory = stateDirectory();
  const initialize = withDaemonStore(directory, (store) =>
    store.recover().pipe(
      Effect.tap((recovery) => {
        expect(recovery.certifiedRecords).toEqual([]);
        return Effect.void;
      }),
    ),
  );
  return initialize.pipe(
    Effect.zipRight(withDaemonStore(directory, (store) => store.recover())),
    Effect.tap(() =>
      Effect.sync(() => {
        assertInitializedDatabase(directory);
      }),
    ),
  );
}
function rejectsNonemptyDatabaseWithoutInitialization() {
  const directory = stateDirectory();
  const path = databasePath(directory);
  const database = new DatabaseSync(path);
  database.exec("CREATE TABLE unexpected_state (value TEXT) STRICT");
  database.close();
  const original = readFileSync(path);
  return expectReason(
    Effect.scoped(withDaemonStore(directory, () => Effect.void)).pipe(
      Effect.catchTag("OutboxTooOldError", () =>
        Effect.dieMessage("unexpected pending outbox"),
      ),
    ),
    "incompatible",
  ).pipe(
    Effect.tap(() =>
      Effect.sync(() => {
        expect(readFileSync(path)).toEqual(original);
      }),
    ),
  );
}
function retainsFirstProposalAcrossRestart() {
  const directory = stateDirectory();
  const conversationId = "conversation:lock";
  const first = proposal(conversationId, `ach_${conversationId}:0`);
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* bindLocalIdentity(store);
      yield* store.putConversationFoundation(foundation(conversationId));
      const localSignature = localActionEvidence(
        conversationId,
        first.actionHash,
      );
      yield* expectReason(store.mergeEvidence(localSignature), "not-found");
      expect(yield* store.lockProposal(first)).toBe(INSERTED_MUTATION);
      expect(yield* store.mergeEvidence(localSignature)).toBe(
        INSERTED_MUTATION,
      );
      expect(yield* store.lockProposal(first)).toBe(EXISTING_MUTATION);
      yield* expectReason(
        store.lockProposal(proposal(conversationId, "ach_competing")),
        "conflict",
      );
    }),
  ).pipe(
    Effect.zipRight(
      withDaemonStore(directory, (store) =>
        store.recover().pipe(
          Effect.tap((recovery) => {
            expect(recovery.proposalLocks).toEqual([first]);
            return Effect.void;
          }),
        ),
      ),
    ),
  );
}
/**
 * Replaces a lock held on one action with the lock on an action whose
 * certificate the endpoint verified, together with that certificate. A
 * certificate for another action is refused and leaves the held lock in
 * place; after the replacement, a reopened store holds the new lock with its
 * certificate and none of the released action's evidence.
 * @returns Completion once the reopened store is checked.
 */
function supersedesAConflictingLockWithItsCertificate() {
  const directory = stateDirectory();
  const conversationId = "conversation:supersede";
  const held = proposal(conversationId, "ach_held");
  const certified = proposal(conversationId, "ach_certified");
  const certificate = actionCertificate(conversationId, certified.actionHash);
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* bindLocalIdentity(store);
      yield* store.putConversationFoundation(foundation(conversationId));
      yield* store.lockProposal(held);
      yield* store.mergeEvidence(
        localActionEvidence(conversationId, held.actionHash),
      );
      yield* expectReason(
        store.supersedeProposalLock(certified, [
          ...certificate.slice(1),
          localActionEvidence(conversationId, held.actionHash),
        ]),
        "invalid-input",
      );
      expect((yield* store.recover()).proposalLocks).toEqual([held]);
      expect(yield* store.supersedeProposalLock(certified, certificate)).toBe(
        INSERTED_MUTATION,
      );
    }),
  ).pipe(
    Effect.zipRight(
      withDaemonStore(directory, (store) =>
        store.recover().pipe(
          Effect.tap((recovery) => {
            expect(recovery.proposalLocks).toEqual([certified]);
            expect(recovery.evidence).toEqual(certificate);
            return Effect.void;
          }),
        ),
      ),
    ),
  );
}
function atomicallyBindsFirstIntentWithItsFoundation() {
  const directory = stateDirectory();
  const firstConversationId = "conversation:intent:first";
  const competingConversationId = "conversation:intent:competing";
  const firstIntent = messageIntent(firstConversationId, "msg_atomic");
  const competingIntent = messageIntent(competingConversationId, "msg_atomic");
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* bindLocalIdentity(store);
      yield* acceptIntent(store, firstIntent);
      expect(
        yield* store.bindMessageIntent({
          kind: "new-conversation",
          foundation: foundation(firstConversationId),
          intent: firstIntent,
        }),
      ).toBe(INSERTED_MUTATION);
      expect(
        yield* store.bindMessageIntent({
          kind: "new-conversation",
          foundation: foundation(firstConversationId),
          intent: firstIntent,
        }),
      ).toBe(EXISTING_MUTATION);
      yield* expectReason(
        store.bindMessageIntent({
          kind: "new-conversation",
          foundation: foundation(competingConversationId),
          intent: competingIntent,
        }),
        "conflict",
      );
      const recovery = yield* store.recover();
      expect(recovery.messageIntents).toEqual([firstIntent]);
      expect(recovery.memberships.map((item) => item.conversationId)).toEqual([
        firstConversationId,
      ]);
    }),
  );
}
function restoresPreparedMessagesInAcceptanceOrder() {
  const directory = stateDirectory();
  const conversationId = "conversation:ordered-messages";
  const original = foundation(conversationId);
  const first = messageIntent(conversationId, "msg_z");
  const later = messageIntent(conversationId, "msg_a");
  return Effect.gen(function* () {
    yield* withDaemonStore(directory, (store) =>
      Effect.gen(function* () {
        yield* bindLocalIdentity(store);
        yield* acceptIntent(store, first);
        yield* acceptIntent(store, later);
        yield* store.bindMessageIntent({
          kind: "new-conversation",
          foundation: original,
          intent: first,
        });
        yield* store.bindMessageIntent({
          kind: "existing-conversation",
          intent: later,
        });
      }),
    );
    yield* withDaemonStore(directory, (store) =>
      Effect.gen(function* () {
        expect((yield* store.recover()).messageIntents).toEqual([first, later]);
        const restarted = yield* store.restartEmptyConversation({
          expectedFoundation: original,
          replacementFoundation: {
            ...original,
            anchorHash: original.anchorHash + ":replacement",
            canonicalAnchor: bytes("replacement anchor"),
          },
        });
        expect(restarted.messageIntents).toEqual([first, later]);
      }),
    );
  });
}
function atomicallyLocksVerifiedFirstMessageFoundation() {
  const directory = stateDirectory();
  const conversationId = "conversation:first-message-lock";
  const retainedFoundation = foundation(conversationId);
  const first = proposal(conversationId, "ach_first-message:first");
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      expect(
        yield* store.lockFirstMessageProposal(retainedFoundation, first),
      ).toBe(INSERTED_MUTATION);
      expect(
        yield* store.lockFirstMessageProposal(retainedFoundation, first),
      ).toBe(EXISTING_MUTATION);
      yield* expectReason(
        store.lockFirstMessageProposal(
          retainedFoundation,
          proposal(conversationId, "ach_first-message:competing"),
        ),
        "conflict",
      );
      const recovery = yield* store.recover();
      expect(recovery.memberships).toHaveLength(1);
      expect(recovery.anchors).toHaveLength(1);
      expect(recovery.proposalLocks).toEqual([first]);
    }),
  );
}
function promotesRemoteRecordWithStableDelivery() {
  const directory = stateDirectory();
  const record = certifiedRecord("conversation:remote", "agent:remote");
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* bindLocalIdentity(store);
      yield* store.putConversationFoundation(foundation(record.conversationId));
      yield* store.applyCertifiedRecord(record);
      const first = yield* store.readUnconsumedRecords();
      expect(first.records.map((entry) => entry.recordHash)).toEqual([
        record.recordHash,
      ]);
      yield* store.applyCertifiedRecord(record);
      expect((yield* store.readUnconsumedRecords()).records).toHaveLength(1);
      yield* store.consumeRecord(record.recordHash);
    }),
  ).pipe(
    Effect.zipRight(
      withDaemonStore(directory, (store) =>
        Effect.gen(function* () {
          expect((yield* store.readUnconsumedRecords()).records).toEqual([]);
          const recovery = yield* store.recover();
          expect(
            recovery.certifiedRecords.map((entry) => entry.recordHash),
          ).toEqual([record.recordHash]);
        }),
      ),
    ),
  );
}

/**
 * The endpoint stages its own message for its vote and stores that vote, then
 * receives the message whole, with its own vote in the durability certificate.
 * The store applies it over the staged record and the stored vote, completes
 * the local message intent and retains no delivery to this endpoint itself.
 * Fails when applying a whole record refuses a vote of this endpoint's own
 * that it already holds, or delivers a local message to its author.
 * @returns The trace, run to completion.
 */
function completesLocalMessageWithoutSelfDelivery() {
  const directory = stateDirectory();
  const conversationId = "conversation:local";
  const record = certifiedRecord(conversationId, LOCAL_AGENT_ID);
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* lockLocalMessage(store, record);
      yield* store.stageRecordAndOutbound(
        stagedRecord(record),
        stageOutbound(stagedRecord(record)),
      );
      yield* Effect.forEach(
        record.durabilityEvidence,
        (vote) => store.mergeEvidence(vote),
        { concurrency: 1, discard: true },
      );
      yield* store.applyCertifiedRecord(record);
      const recovery = yield* store.recover();
      expect(recovery.messageIntents[0]?.completedRecordHash).toBe(
        record.recordHash,
      );
      expect((yield* store.readUnconsumedRecords()).records).toEqual([]);
    }),
  );
}
/**
 * The endpoint holds a certified head and a staged successor of it that it
 * voted durable. The store refuses a re-anchor candidate away from that head
 * under the same anchor, so this endpoint's signatures never land on both a
 * re-anchor away from the head and a durability certificate extending it,
 * whatever order recovery and ingress reach the store in. Fails when the
 * store stages that candidate.
 * @returns The trace, run to completion.
 */
function refusesAReanchorAwayFromAStagedSuccessor() {
  const directory = stateDirectory();
  const conversationId = "conversation:staged-successor";
  const head = certifiedRecord(conversationId, LOCAL_AGENT_ID);
  const successor = stagedRecord(
    certifiedRecord(conversationId, LOCAL_AGENT_ID, 1),
  );
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* lockLocalMessage(store, head);
      yield* store.applyCertifiedRecord(head);
      yield* store.stageRecordAndOutbound(successor, stageOutbound(successor));
      yield* expectReason(
        store.stageReanchor(reanchorAwayFrom(head)),
        "conflict",
      );
      expect((yield* store.recover()).stagedReanchors).toEqual([]);
    }),
  );
}
/**
 * Certificate adoption retains verified signatures after departure without
 * authorizing a new local durability vote. An identical retained signature
 * remains replayable, and repeated adoption keeps one remote delivery.
 */
function refusesALocalVoteUnderAnAnchorItReanchorsAwayFrom() {
  const directory = stateDirectory();
  const conversationId = "conversation:reanchored-vote";
  const head = certifiedRecord(conversationId, LOCAL_AGENT_ID);
  const successor = certifiedRecord(conversationId, "agent:remote", 1);
  const localVote: ProtocolEvidence = {
    conversationId,
    kind: "durability",
    subjectId: successor.recordHash,
    evidenceKey: LOCAL_AGENT_ID,
    canonicalEvidence: bytes("durability-vote:local"),
  };
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* lockLocalMessage(store, head);
      yield* store.applyCertifiedRecord(head);
      yield* store.lockProposal({
        ...proposal(conversationId, successor.actionHash),
        previousRecordHash: head.recordHash,
      });
      yield* store.stageReanchor(reanchorAwayFrom(head));
      yield* store.applyCertifiedRecord(successor);
      yield* expectReason(store.mergeEvidence(localVote), "conflict");
      expect((yield* store.recover()).evidence).not.toContainEqual(localVote);
      yield* store.applyCertifiedRecord({
        ...successor,
        durabilityEvidence: [...successor.durabilityEvidence, localVote],
      });
      expect(yield* store.mergeEvidence(localVote)).toBe(EXISTING_MUTATION);
      const recovered = yield* store.recover();
      expect(recovered.positions[0]?.headRecordHash).toBe(successor.recordHash);
      expect(recovered.evidence).toContainEqual(localVote);
      expect((yield* store.readUnconsumedRecords()).records).toHaveLength(1);
    }),
  );
}
interface OutboundLifecycleFixture {
  readonly directory: string;
  readonly message: OutboundMessageInput;
}
function persistsExactOutboundLifecycleAcrossRestart() {
  const fixture: OutboundLifecycleFixture = {
    directory: stateDirectory(),
    message: outboundMessage("conversation:outbound", "env_outbound", "outer"),
  };
  return stageInitialOutbound(fixture).pipe(
    Effect.zipRight(completeRetriedOutbound(fixture)),
    Effect.zipRight(verifyOutboundComplete(fixture.directory)),
  );
}
function stageInitialOutbound(fixture: OutboundLifecycleFixture) {
  return withDaemonStore(fixture.directory, (store) =>
    Effect.gen(function* () {
      yield* store.putConversationFoundation(
        foundation(fixture.message.conversationId),
      );
      const { staged, repeated } = yield* Effect.all(
        {
          staged: store.enqueueOutbound(fixture.message),
          repeated: store.enqueueOutbound(fixture.message),
        },
        { concurrency: 2 },
      );
      expect(repeated).toEqual(staged);
      repeated.canonicalSignedMessage.fill(0);
      const snapshot = yield* recoverOnlyOutbound(store);
      expect(snapshot).toEqual(staged);
      snapshot.canonicalSignedMessage.fill(0);
      yield* expectReason(
        store.enqueueOutbound({
          ...fixture.message,
          canonicalSignedMessage: bytes("conflicting envelope"),
        }),
        "conflict",
      );
      expect(staged.envelopeId).toBe(fixture.message.envelopeId);
      expect(yield* store.beginOutbound(staged.envelopeId)).toEqual({
        kind: "pending",
        mode: "initial",
        outbound: staged,
      });
    }),
  );
}
function completeRetriedOutbound(fixture: OutboundLifecycleFixture) {
  return withDaemonStore(fixture.directory, (store) =>
    Effect.gen(function* () {
      const replay = yield* recoverOnlyOutbound(store);
      expect(replay).toEqual({
        ...fixture.message,
      });
      expect(yield* store.beginOutbound(replay.envelopeId)).toEqual({
        kind: "pending",
        mode: "retry",
        outbound: replay,
      });
      expect(yield* store.completeOutbound(replay)).toBe(INSERTED_MUTATION);
      expect(yield* store.completeOutbound(replay)).toBe(EXISTING_MUTATION);
    }),
  );
}
function verifyOutboundComplete(directory: string) {
  return withDaemonStore(directory, (store) =>
    store.recover().pipe(
      Effect.tap((recovery) => {
        expect(recovery.outboundMessages).toEqual([]);
        return Effect.void;
      }),
    ),
  );
}
function recoverOnlyOutbound(store: TestStore) {
  return store.recover().pipe(
    Effect.flatMap((recovery) => {
      const outbound = recovery.outboundMessages[0];
      return outbound === undefined
        ? Effect.die("pending outbound was not recovered")
        : Effect.succeed(outbound);
    }),
  );
}

function retainsRecordDisseminationAcrossCrashWindows() {
  const directory = stateDirectory();
  const record = certifiedRecord("conversation:dissemination", "agent:remote");
  const staged = stagedRecord(record);
  const first = stageOutbound(staged);
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* bindLocalIdentity(store);
      yield* store.putConversationFoundation(foundation(record.conversationId));
      yield* expectReason(
        store.stageRecordAndOutbound(staged, {
          ...first,
          canonicalSignedMessage: new Uint8Array(),
        }),
        "invalid-input",
      );
      expect((yield* store.recover()).stagedRecords).toEqual([]);
      const retained = yield* store.stageRecordAndOutbound(staged, first);
      expect(retained).toEqual(first);
      expect(
        yield* store.stageRecordAndOutbound(staged, {
          ...first,
          envelopeId: "env_unused",
        }),
      ).toEqual(retained);
    }),
  ).pipe(
    Effect.zipRight(
      withDaemonStore(directory, (store) =>
        Effect.gen(function* () {
          expect(
            (yield* store.readRecordOutbound(
              record.conversationId,
              record.recordHash,
            ))?.outbound,
          ).toEqual(first);
          yield* store.discardOutbound([first]);
          const replacement = { ...first, envelopeId: "env_reanchored" };
          expect(
            yield* store.stageRecordAndOutbound(staged, replacement),
          ).toEqual(replacement);
        }),
      ),
    ),
  );
}

interface EmptyRestartFixture {
  readonly directory: string;
  readonly conversationId: string;
  readonly newFoundation: ConversationFoundation;
  readonly intent: MessageIntent;
  readonly staged: StagedRecord;
  readonly lock: ProposalLock;
  readonly restart: EmptyConversationRestart;
}
function restartsOnlyAnEmptyConversationAtomically() {
  const directory = stateDirectory();
  const conversationId = "conversation:empty-restart";
  const oldFoundation = foundation(conversationId);
  const newFoundation: ConversationFoundation = {
    ...oldFoundation,
    anchorHash: `anc_${conversationId}:1`,
    canonicalAnchor: bytes(`anchor:${conversationId}:1`),
  };
  const intent = messageIntent(conversationId, "msg_restart");
  const staged = stagedRecord(certifiedRecord(conversationId, LOCAL_AGENT_ID));
  const fixture: EmptyRestartFixture = {
    directory,
    conversationId,
    newFoundation,
    intent,
    staged,
    lock: proposal(conversationId, staged.actionHash),
    restart: {
      expectedFoundation: oldFoundation,
      replacementFoundation: newFoundation,
    },
  };
  return restartEmptyConversation(fixture);
}
function restartEmptyConversation(fixture: EmptyRestartFixture) {
  return withDaemonStore(fixture.directory, (store) =>
    prepareEmptyRestartState(store, fixture).pipe(
      Effect.zipRight(store.restartEmptyConversation(fixture.restart)),
      Effect.tap((restarted) =>
        Effect.sync(() => {
          assertRestartedEmptyConversation(restarted, fixture);
        }),
      ),
      Effect.zipRight(store.recover()),
      Effect.tap((recovery) =>
        Effect.sync(() => {
          assertEmptyRestartRecovery(recovery, fixture);
        }),
      ),
    ),
  );
}
function prepareEmptyRestartState(
  store: TestStore,
  fixture: EmptyRestartFixture,
) {
  return Effect.gen(function* () {
    yield* bindLocalIdentity(store);
    yield* acceptIntent(store, fixture.intent);
    yield* store.bindMessageIntent({
      kind: "new-conversation",
      foundation: fixture.restart.expectedFoundation,
      intent: fixture.intent,
    });
    yield* store.lockProposal(fixture.lock);
    yield* store.stageRecordAndOutbound(
      fixture.staged,
      stageOutbound(fixture.staged),
    );
    yield* store.mergeEvidence({
      conversationId: fixture.conversationId,
      kind: "action",
      subjectId: fixture.staged.actionHash,
      evidenceKey: LOCAL_AGENT_ID,
      canonicalEvidence: bytes("action:stale"),
    });
    yield* store.mergeEvidence({
      conversationId: fixture.conversationId,
      kind: "durability",
      subjectId: fixture.staged.recordHash,
      evidenceKey: LOCAL_AGENT_ID,
      canonicalEvidence: bytes("durability:stale"),
    });
    yield* store.enqueueOutbound(
      outboundMessage(fixture.conversationId, "env_stale", "outer:stale"),
    );
  });
}
function assertRestartedEmptyConversation(
  restarted: RestartedEmptyConversation,
  fixture: EmptyRestartFixture,
): void {
  expect(restarted).toEqual({
    foundation: fixture.newFoundation,
    messageIntents: [fixture.intent],
  });
}
function assertEmptyRestartRecovery(
  recovery: EndpointRecovery,
  fixture: EmptyRestartFixture,
): void {
  expect(recovery.anchors).toEqual([
    {
      conversationId: fixture.conversationId,
      anchorHash: fixture.newFoundation.anchorHash,
      canonicalAnchor: fixture.newFoundation.canonicalAnchor,
    },
  ]);
  expect(recovery.positions).toEqual([
    {
      conversationId: fixture.conversationId,
      membershipHash: fixture.newFoundation.membershipHash,
      currentAnchorHash: fixture.newFoundation.anchorHash,
    },
  ]);
  expect(recovery.messageIntents).toEqual([fixture.intent]);
  expect(recovery.proposalLocks).toEqual([]);
  expect(recovery.stagedRecords).toEqual([]);
  expect(recovery.evidence).toEqual([]);
  expect(recovery.outboundMessages).toEqual([]);
}
function refusesEmptyRestartAfterCertification() {
  const directory = stateDirectory();
  const conversationId = "conversation:certified-restart";
  const oldFoundation = foundation(conversationId);
  const record = certifiedRecord(conversationId, LOCAL_AGENT_ID);
  const replacement: ConversationFoundation = {
    ...oldFoundation,
    anchorHash: `anc_${conversationId}:1`,
    canonicalAnchor: bytes(`anchor:${conversationId}:1`),
  };
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* lockLocalMessage(store, record);
      yield* store.applyCertifiedRecord(record);
      yield* expectReason(
        store.restartEmptyConversation({
          expectedFoundation: oldFoundation,
          replacementFoundation: replacement,
        }),
        "conflict",
      );
      const recovery = yield* store.recover();
      expect(recovery.positions[0]?.headRecordHash).toBe(record.recordHash);
      expect(recovery.positions[0]?.currentAnchorHash).toBe(
        oldFoundation.anchorHash,
      );
    }),
  );
}
function discardsOnlyAnExactCurrentOutboundSet() {
  const directory = stateDirectory();
  const conversationId = "conversation:discard-outbound";
  const first = outboundMessage(conversationId, "env_discard_a", "outer:a");
  const second = outboundMessage(conversationId, "env_discard_b", "outer:b");
  return withDaemonStore(directory, (store) =>
    Effect.gen(function* () {
      yield* store.putConversationFoundation(foundation(conversationId));
      const retainedFirst = yield* store.enqueueOutbound(first);
      const retainedSecond = yield* store.enqueueOutbound(second);
      yield* expectReason(
        store.discardOutbound([
          retainedFirst,
          {
            ...retainedSecond,
            wireVersion: WIRE_VERSION,
            canonicalSignedMessage: bytes("outer:changed"),
          },
        ]),
        "conflict",
      );
      expect((yield* store.recover()).outboundMessages).toEqual([
        retainedFirst,
        retainedSecond,
      ]);
      expect(
        yield* store.discardOutbound([retainedFirst, retainedSecond]),
      ).toBe(INSERTED_MUTATION);
      expect(
        yield* store.discardOutbound([retainedFirst, retainedSecond]),
      ).toBe(EXISTING_MUTATION);
      expect((yield* store.recover()).outboundMessages).toEqual([]);
    }),
  );
}
function assertInitializedDatabase(directory: string): void {
  const path = databasePath(directory);
  expect(statSync(directory).mode & 0o777).toBe(DIRECTORY_MODE);
  expect(statSync(path).mode & 0o777).toBe(DATABASE_FILE_MODE);
}

/**
 * A certified record under a conversation's first anchor, with one action
 * signature and one durability vote, both its author's.
 * @param conversationId Conversation under whose first foundation it is.
 * @param authorAgentId Agent that authored it and signed both certificates.
 * @param ordinal Its position in the conversation, extending the record one
 *     position before it.
 * @returns A record whose hashes and bytes follow from its position.
 */
function certifiedRecord(
  conversationId: string,
  authorAgentId: string,
  ordinal = 0,
): CertifiedRecord {
  const actionHash = `ach_${conversationId}:${ordinal}`;
  const recordHash = `rch_${conversationId}:${ordinal}`;
  const conversationFoundation = foundation(conversationId);
  return {
    conversationId,
    recordHash,
    ...(ordinal === 0
      ? {}
      : { previousRecordHash: `rch_${conversationId}:${ordinal - 1}` }),
    membershipHash: conversationFoundation.membershipHash,
    anchorHash: conversationFoundation.anchorHash,
    actionHash,
    authorAgentId,
    msgId: `msg_${authorAgentId}:${ordinal}`,
    canonicalRecordCore: bytes(`record:${conversationId}:${ordinal}`),
    actionEvidence: [
      {
        conversationId,
        kind: "action",
        subjectId: actionHash,
        evidenceKey: authorAgentId,
        canonicalEvidence: bytes(`action-signature:${authorAgentId}`),
      },
    ],
    durabilityEvidence: [
      {
        conversationId,
        kind: "durability",
        subjectId: recordHash,
        evidenceKey: authorAgentId,
        canonicalEvidence: bytes(`durability-vote:${authorAgentId}`),
      },
    ],
  };
}
/**
 * Bind this endpoint's identity and its message intent for `record`, with the
 * conversation's first foundation, and lock `record`'s action: the state in
 * which this endpoint proposes its own message.
 * @param store Store that takes the identity, the intent and the lock.
 * @param record Record whose message intent is bound and whose action is locked.
 * @returns Completion once all three are durable.
 */
function lockLocalMessage(store: TestStore, record: StagedRecord) {
  return Effect.gen(function* () {
    yield* bindLocalIdentity(store);
    yield* acceptIntent(
      store,
      messageIntent(record.conversationId, record.msgId),
    );
    yield* store.bindMessageIntent({
      kind: "new-conversation",
      foundation: foundation(record.conversationId),
      intent: messageIntent(record.conversationId, record.msgId),
    });
    yield* store.lockProposal(
      proposal(record.conversationId, record.actionHash),
    );
  });
}
function reanchorAwayFrom(head: CertifiedRecord): StagedReanchor {
  return {
    conversationId: head.conversationId,
    anchorHash: `anc_${head.conversationId}:1`,
    previousAnchorHash: head.anchorHash,
    routerInstanceId: `rti_${head.conversationId}`,
    selectedRecordHash: head.recordHash,
    canonicalBody: bytes(`reanchor:${head.conversationId}:1`),
  };
}
function stagedRecord(record: CertifiedRecord): StagedRecord {
  return {
    conversationId: record.conversationId,
    recordHash: record.recordHash,
    ...(record.previousRecordHash === undefined
      ? {}
      : { previousRecordHash: record.previousRecordHash }),
    membershipHash: record.membershipHash,
    anchorHash: record.anchorHash,
    actionHash: record.actionHash,
    authorAgentId: record.authorAgentId,
    msgId: record.msgId,
    canonicalRecordCore: record.canonicalRecordCore,
  };
}
function messageIntent(conversationId: string, msgId: string): MessageIntent {
  return {
    conversationId,
    membershipHash: foundation(conversationId).membershipHash,
    authorAgentId: LOCAL_AGENT_ID,
    msgId,
    canonicalIntent: bytes(`intent:${conversationId}:${msgId}`),
    canonicalMessage: bytes("captured:" + msgId),
  };
}
function foundation(conversationId: string): ConversationFoundation {
  return {
    conversationId,
    membershipHash: `mbr_${conversationId}`,
    canonicalMembership: bytes(`membership:${conversationId}`),
    anchorHash: `anc_${conversationId}:0`,
    canonicalAnchor: bytes(`anchor:${conversationId}:0`),
  };
}
function proposal(conversationId: string, actionHash: string): ProposalLock {
  return {
    conversationId,
    actionHash,
    canonicalActionCore: bytes(`action:${actionHash}`),
  };
}

function stageOutbound(record: StagedRecord): OutboundMessageInput {
  return {
    ...outboundMessage(
      record.conversationId,
      "env_" + record.recordHash,
      "outer staged",
    ),
    recordHash: record.recordHash,
  };
}

function outboundMessage(
  conversationId: string,
  envelopeId: string,
  canonical: string,
): OutboundMessageInput {
  return {
    conversationId,
    envelopeId,
    wireVersion: WIRE_VERSION,
    canonicalSignedMessage: bytes(canonical),
  };
}
function localActionEvidence(
  conversationId: string,
  actionHash: string,
): ProtocolEvidence {
  return {
    conversationId,
    kind: "action",
    subjectId: actionHash,
    evidenceKey: LOCAL_AGENT_ID,
    canonicalEvidence: bytes("action-signature:local"),
  };
}
function actionCertificate(
  conversationId: string,
  actionHash: string,
): readonly ProtocolEvidence[] {
  return ["agent:a", "agent:b", "agent:c"].map(
    (signer): ProtocolEvidence => ({
      conversationId,
      kind: "action",
      subjectId: actionHash,
      evidenceKey: signer,
      canonicalEvidence: bytes(`action-signature:${signer}`),
    }),
  );
}
function bindLocalIdentity(store: TestStore) {
  return store.bindIdentity({
    agentId: LOCAL_AGENT_ID,
    canonicalAgentCard: bytes("card:local"),
  });
}
function expectReason<Value, Requirements>(
  effect: Effect.Effect<Value, EndpointStoreError, Requirements>,
  reason: EndpointStoreError["reason"],
) {
  return Effect.flip(effect).pipe(
    Effect.orDie,
    Effect.tap((failure) => {
      expect(failure).toBeInstanceOf(EndpointStoreError);
      expect(failure.reason).toBe(reason);
      expect(Object.hasOwn(failure, "cause")).toBe(false);
      return Effect.void;
    }),
    Effect.asVoid,
  );
}
describe("endpoint SQLite preflight", () => {
  it("initializes an empty database and reopens it", initializesEmptyDatabase);
  it(
    "rejects an unversioned populated database without changing its bytes",
    rejectsNonemptyDatabaseWithoutInitialization,
  );
});
// @agent-code-guard/regression-only: these cases pin the proposal-lock store contract.
describe("endpoint proposal locking", () => {
  it(
    "atomically binds the first message intent with its foundation",
    atomicallyBindsFirstIntentWithItsFoundation,
  );
  it(
    "atomically locks a verified first message with its foundation",
    atomicallyLocksVerifiedFirstMessageFoundation,
  );
  it(
    "retains the first proposal lock across conflicts and restart",
    retainsFirstProposalAcrossRestart,
  );
  it(
    "replaces a conflicting lock with a certified one and its certificate",
    supersedesAConflictingLockWithItsCertificate,
  );
});
describe("endpoint record certification and delivery", () => {
  it(
    "atomically applies a remote record received whole and replays one stable delivery",
    promotesRemoteRecordWithStableDelivery,
  );
  it(
    "completes a local message intent without creating self-delivery",
    completesLocalMessageWithoutSelfDelivery,
  );
});
describe("endpoint re-anchor candidates", () => {
  it(
    "refuses a candidate away from a head it holds a staged successor of",
    refusesAReanchorAwayFromAStagedSuccessor,
  );
  it(
    "adopts verified certificates after departure while refusing new local votes",
    refusesALocalVoteUnderAnAnchorItReanchorsAwayFrom,
  );
});
describe("endpoint durable Router outbox", () => {
  it(
    "replays, retries, and completes exact envelopes",
    persistsExactOutboundLifecycleAcrossRestart,
  );
  it(
    "invalidates only an exact current envelope set atomically",
    discardsOnlyAnExactCurrentOutboundSet,
  );
  it(
    "recovers record dissemination before and after outbox attachment",
    retainsRecordDisseminationAcrossCrashWindows,
  );
});
describe("endpoint empty-history Router restart", () => {
  it(
    "restores prepared messages in acceptance order",
    restoresPreparedMessagesInAcceptanceOrder,
  );
  it(
    "retains intents while replacing only incomplete state",
    restartsOnlyAnEmptyConversationAtomically,
  );
  it(
    "refuses to replace a foundation after certification",
    refusesEmptyRestartAfterCertification,
  );
});
function acceptIntent(store: TestStore, intent: MessageIntent) {
  return store.acceptMessage({
    ...intent,
    canonicalMessage: bytes("captured:" + intent.msgId),
  });
}
