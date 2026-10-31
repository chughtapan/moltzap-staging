/**
 * @file Router-ordered proposal selection, addressed record certification,
 * and the resume of durable dissemination obligations.
 */

import type * as SqlClient from "@effect/sql/SqlClient";
import { absurd, Effect } from "effect";
import type { SignedMessage } from "../../../identity/envelope/signed-message.js";
import {
  ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  equalCanonical,
} from "../../../encoding/canonical-json.js";
import {
  type ActionCertifiedRecord,
  type ActionCore,
  ActionCore as ActionCoreSchema,
  type ActionHash,
  type ActionProposal,
  type AnchorHash,
  type CertifiedRecord,
  type ConversationId,
  type DecodedOuterBody,
  EvidenceStatement,
  type EvidenceStatement as EvidenceStatementValue,
  hashAnchor,
  InitialAnchorBody,
  MembershipDescriptor,
  quorumThreshold,
  signEvidenceMessage,
  type VerifiedMembership,
  verifyActionCertifiedRecord,
  verifyActionProposal,
  verifyCertifiedRecord,
  verifyDeliveredEvidence,
  verifyMembershipDescriptor,
  verifyOuterMessage,
  verifyStableEvidence,
} from "../../records/index.js";
import {
  type RouterIngressDisposition,
  type RouterWorkerIngress,
  RouterWorkerPersistenceError,
} from "../../router/index.js";
import * as Store from "../../store/index.js";
import {
  type ConversationFoundation,
  type EndpointStoreError,
  isSemanticStoreRejection,
  type ProposalLock,
} from "../../store/index.js";
import {
  type EngineActionFold,
  type EngineConversation,
  type EngineRuntime,
  evidenceMatchesFold,
  type EvidenceRoute,
  evidenceTarget,
  makeActionFold,
} from "../engine-context.js";
import {
  makeActionCertifiedRecord,
  makeCertifiedRecord,
  protocolEvidence,
  recordAnchorHash,
  stagedRecord,
  storedCertifiedRecord,
} from "../history/index.js";
import { completePromotion, rebasePendingIntents } from "./head.js";

const persistenceFailure = () => new RouterWorkerPersistenceError();

const localRepresentationFailure = (): RouterWorkerPersistenceError =>
  persistenceFailure();

const currentAnchorHash = (
  conversation: EngineConversation,
): Effect.Effect<
  AnchorHash,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  conversation.currentAnchor.kind === "initial_anchor_body"
    ? hashAnchor(conversation.currentAnchor).pipe(
        Effect.mapError(localRepresentationFailure),
      )
    : Effect.succeed(conversation.currentAnchor.anchorHash);

const gapFree = (
  conversation: EngineConversation,
  action: ActionCore,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    if (action.kind === "FIRST_MESSAGE") {
      return (
        conversation.head === undefined &&
        conversation.currentAnchor.kind === "initial_anchor_body" &&
        (yield* equalCanonical(
          InitialAnchorBody,
          conversation.currentAnchor,
          action.anchor,
        ).pipe(Effect.mapError(localRepresentationFailure)))
      );
    }
    return (
      conversation.head?.recordHash === action.previousRecordHash &&
      (yield* currentAnchorHash(conversation)) === action.anchorHash
    );
  });

/**
 * The membership an action's conversation has here: the one this endpoint
 * holds, or, for the FIRST_MESSAGE of a conversation it does not hold yet, the
 * descriptor FIRST_MESSAGE carries, verified against the Registry key. A MESSAGE names
 * its membership only by hash, so a MESSAGE for a conversation this endpoint
 * does not hold does not verify.
 * @param runtime Engine whose held conversations are searched.
 * @param action The action whose conversation is resolved.
 * @returns The verified membership of the action's conversation.
 */
const ingressMembership = (
  runtime: EngineRuntime,
  action: ActionCore,
): Effect.Effect<
  VerifiedMembership,
  ClientRepresentationError,
  SqlClient.SqlClient
> => {
  const retained = runtime.conversations.get(action.conversationId);
  if (retained !== undefined) {
    return Effect.succeed(retained.membership);
  }
  return action.kind === "FIRST_MESSAGE"
    ? verifyMembershipDescriptor(
        action.membership,
        runtime.input.registrySignerPublicKey,
      )
    : Effect.fail(new ClientRepresentationError());
};

const conversationForAction = (
  runtime: EngineRuntime,
  action: ActionCore,
  routerInstanceId: RouterWorkerIngress<DecodedOuterBody>["routerInstanceId"],
): Effect.Effect<
  EngineConversation | undefined,
  ClientRepresentationError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const retained = runtime.conversations.get(action.conversationId);
    if (retained !== undefined || action.kind === "MESSAGE") {
      return retained;
    }
    const membership = yield* verifyMembershipDescriptor(
      action.membership,
      runtime.input.registrySignerPublicKey,
    );
    if (action.anchor.routerInstanceId !== routerInstanceId) {
      return undefined;
    }
    return {
      conversationId: action.conversationId,
      membership,
      currentAnchor: action.anchor,
    };
  });

const firstMessageFoundation = (
  conversation: EngineConversation,
  action: Extract<ActionCore, { readonly kind: "FIRST_MESSAGE" }>,
): Effect.Effect<
  ConversationFoundation,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    return {
      conversationId: action.conversationId,
      membershipHash: conversation.membership.hash,
      canonicalMembership: yield* encodeCanonical(
        MembershipDescriptor,
        conversation.membership.descriptor,
      ).pipe(Effect.mapError(localRepresentationFailure)),
      anchorHash: yield* hashAnchor(action.anchor).pipe(
        Effect.mapError(localRepresentationFailure),
      ),
      canonicalAnchor: yield* encodeCanonical(
        InitialAnchorBody,
        action.anchor,
      ).pipe(Effect.mapError(localRepresentationFailure)),
    };
  });

const proposalLock = (
  conversation: EngineConversation,
  action: ActionCore,
  actionHash: ActionHash,
): Effect.Effect<
  ProposalLock,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  encodeCanonical(ActionCoreSchema, action).pipe(
    Effect.mapError(localRepresentationFailure),
    Effect.map((canonicalActionCore) => ({
      conversationId: conversation.conversationId,
      ...(action.previousRecordHash === null
        ? {}
        : { previousRecordHash: action.previousRecordHash }),
      actionHash,
      canonicalActionCore,
    })),
  );

const lockAction = (
  runtime: EngineRuntime,
  conversation: EngineConversation,
  action: ActionCore,
  actionHash: ActionHash,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const lock = yield* proposalLock(conversation, action, actionHash);
    if (
      action.kind === "FIRST_MESSAGE" &&
      !runtime.conversations.has(action.conversationId)
    ) {
      const foundation = yield* firstMessageFoundation(conversation, action);
      yield* Store.lockFirstMessageProposal(foundation, lock);
      yield* Effect.sync(() => {
        runtime.conversations.set(action.conversationId, conversation);
      });
      return;
    }
    yield* Store.lockProposal(lock);
  });

const foldFor = (
  runtime: EngineRuntime,
  conversation: EngineConversation,
  action: ActionCore,
  actionHash: ActionHash,
): EngineActionFold => {
  const retained = runtime.actionFolds.get(actionHash);
  if (retained !== undefined) {
    return retained;
  }
  const fold = makeActionFold(
    conversation,
    action,
    actionHash,
    action.kind === "FIRST_MESSAGE"
      ? action.anchor
      : conversation.currentAnchor,
  );
  runtime.actionFolds.set(actionHash, fold);
  return fold;
};

type LocalEvidenceRole = "action" | "durability";

const mergeEvidence = (
  fold: EngineActionFold,
  role: LocalEvidenceRole,
  message: SignedMessage,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const subjectId = role === "action" ? fold.actionHash : fold.recordHash;
    if (subjectId === undefined) {
      return;
    }
    const row = yield* protocolEvidence(
      fold.conversation.conversationId,
      role,
      subjectId,
      message,
    ).pipe(Effect.mapError(localRepresentationFailure));
    yield* Store.mergeEvidence(row);
    const evidence =
      role === "action" ? fold.actionEvidence : fold.durabilityEvidence;
    yield* Effect.sync(() => {
      evidence.set(message.senderAgentId, message);
    });
  });

/**
 * Merge unless the evidence is already retained, then queue and flag it.
 * Queueing and its flag share an interruption boundary after evidence is durable.
 * @param runtime Engine whose store and outbox retain the evidence.
 * @param fold Fold that owes the local evidence.
 * @param role The evidence obligation being completed.
 * @param evidence Local signature or vote selected by the caller.
 * @returns Completion once the evidence is durable and queued.
 */
const completeLocalEvidence = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
  role: LocalEvidenceRole,
  evidence: SignedMessage,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const retained =
      role === "action" ? fold.actionEvidence : fold.durabilityEvidence;
    if (!retained.has(runtime.input.localAgentCard.agentId)) {
      yield* mergeEvidence(fold, role, evidence);
    }
    yield* Effect.uninterruptible(
      runtime.outbox.queueEvidence(fold.conversation, evidence).pipe(
        Effect.mapError(() => persistenceFailure()),
        Effect.zipRight(
          Effect.sync(() => {
            switch (role) {
              case "action":
                fold.localActionEvidenceQueued = true;
                return;
              case "durability":
                fold.localDurabilityEvidenceQueued = true;
                return;
              default:
                return absurd(role);
            }
          }),
        ),
      ),
    );
  });

const localActionEvidence = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    if (fold.localActionEvidenceQueued) {
      return;
    }
    const evidence = yield* selectLocalActionEvidence(runtime, fold);
    if (evidence === undefined) {
      return;
    }
    yield* completeLocalEvidence(runtime, fold, "action", evidence);
  });

/**
 * Select this endpoint's action signature for a fold: the one it holds, or a
 * new one. The fold's action passed verification when it arrived, so the
 * endpoint signs it without consulting anything outside the transport. A fold that already holds its action
 * certificate gets no new signature: the action needs none, and this endpoint
 * may have taken that certificate over a lock it held on another action at
 * the same predecessor, which it signed.
 * @param runtime Engine whose identity signs.
 * @param fold Fold the signature names.
 * @returns The signature to send, or nothing when none is due.
 */
function selectLocalActionEvidence(
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  SignedMessage | undefined,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  const retained = fold.actionEvidence.get(
    runtime.input.localAgentCard.agentId,
  );
  if (retained !== undefined) {
    return Effect.succeed(retained);
  }
  if (hasActionThreshold(fold)) {
    return Effect.succeed(undefined);
  }
  return signLocalActionEvidence(runtime, fold);
}

function signLocalActionEvidence(
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  SignedMessage,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> {
  return signEvidenceMessage({
    statement: {
      sharnessVersion: runtime.wireVersion,
      kind: "action_signature",
      signerAgentId: runtime.input.localAgentCard.agentId,
      actionHash: fold.actionHash,
    },
    agentCard: runtime.input.localAgentCard,
    signingAuthority: runtime.input.signingAuthority,
  }).pipe(Effect.mapError(localRepresentationFailure));
}

function hasActionThreshold(fold: EngineActionFold): boolean {
  const memberCount = fold.conversation.membership.members.length;
  const count = fold.actionEvidence.size;
  const thresholdReached =
    fold.action.kind === "FIRST_MESSAGE"
      ? count === memberCount
      : count >= quorumThreshold(memberCount);
  return (
    thresholdReached &&
    fold.actionEvidence.has(fold.action.messageIntent.authorAgentId)
  );
}

const hasDurabilityThreshold = (fold: EngineActionFold): boolean =>
  fold.durabilityEvidence.size >=
  quorumThreshold(fold.conversation.membership.members.length);

const actionAnchorHash = (
  fold: EngineActionFold,
): Effect.Effect<
  AnchorHash,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> => recordAnchorHash(fold).pipe(Effect.mapError(localRepresentationFailure));

/**
 * Queue this endpoint's durability vote for a fold's staged record: the vote
 * it holds, or a new one. An uncertified fold's record is one this endpoint
 * staged for its own vote, since a record received whole is staged in the
 * transaction that certifies it. A certified fold gets no new vote: a vote is
 * stored before it is sent, so a certified fold without one names a record
 * this endpoint never voted for, and the record needs no vote.
 * @param runtime Engine whose identity signs and whose outbox sends.
 * @param fold Fold whose staged record the vote names.
 * @returns Completion once the vote is queued, or at once when none is due.
 */
const localDurabilityEvidence = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const recordHash = fold.recordHash;
    const localAgentId = runtime.input.localAgentCard.agentId;
    if (recordHash === undefined || fold.localDurabilityEvidenceQueued) {
      return;
    }
    const retained = fold.durabilityEvidence.get(localAgentId);
    if (retained === undefined && fold.certified) {
      return;
    }
    const evidence =
      retained ??
      (yield* signEvidenceMessage({
        statement: {
          sharnessVersion: runtime.wireVersion,
          kind: "durability_vote",
          signerAgentId: localAgentId,
          conversationId: fold.conversation.conversationId,
          membershipHash: fold.conversation.membership.hash,
          recordHash,
        },
        agentCard: runtime.input.localAgentCard,
        signingAuthority: runtime.input.signingAuthority,
      }).pipe(Effect.mapError(localRepresentationFailure)));
    yield* completeLocalEvidence(runtime, fold, "durability", evidence);
  });

/** Keeps the verified record and its two certificates in one atomic store input. */
const certifiedRecordRows = (
  record: CertifiedRecord,
  actionEvidence: Iterable<SignedMessage>,
  votes: Iterable<SignedMessage>,
) =>
  storedCertifiedRecord(record, actionEvidence, votes).pipe(
    Effect.mapError(localRepresentationFailure),
  );

/**
 * Store a fold's complete record as certified and install it as its
 * conversation's head in one uninterruptible step. An interruption between
 * the two would leave the store holding the record certified while the fold,
 * the conversation's head and the local message's completion still lag it.
 * @param runtime Engine whose store and conversation take the record.
 * @param fold Fold of the record's action, holding its votes.
 * @param record The complete certified record.
 * @returns Completion once the record is installed and pending messages rebased.
 */
const promote = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
  record: CertifiedRecord,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const stored = yield* certifiedRecordRows(
      record,
      fold.actionEvidence.values(),
      fold.durabilityEvidence.values(),
    );
    yield* Effect.uninterruptible(
      Store.promoteRecord(stored).pipe(
        Effect.zipRight(completePromotion(runtime, fold, record)),
      ),
    );
    yield* rebasePendingIntents(runtime, fold.conversation.conversationId);
    yield* runtime.phases.headAdvanced(
      runtime,
      fold.conversation.conversationId,
    );
  });

const maybePromote = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
  actionCertifiedRecord: ActionCertifiedRecord,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    if (!hasDurabilityThreshold(fold) || fold.certified) {
      return;
    }
    const record = yield* makeCertifiedRecord(actionCertifiedRecord, fold).pipe(
      Effect.mapError(localRepresentationFailure),
    );
    yield* verifyCertifiedRecord({
      record,
      membership: fold.conversation.membership,
    }).pipe(Effect.mapError(localRepresentationFailure));
    yield* promote(runtime, fold, record);
  });

/**
 * Stage a record from its action certificate, then vote for it and promote it
 * once the votes reach `q(n)`. The first time this endpoint stages the record,
 * whether it assembled the certificate or received a copy, it sends its own
 * copy before its vote. A faulty member can seal a body that some members
 * cannot open, so a member that staged from such a copy still gives every
 * member a copy it can open ahead of its vote.
 * @param runtime Engine that stages the record and owns its fold.
 * @param fold Fold of the record's action.
 * @param record Verified action certificate for the fold's action.
 * @returns Completion once the record is staged and its local evidence queued.
 */
const stageActionCertificate = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
  record: ActionCertifiedRecord,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    if (fold.recordHash === undefined) {
      const stored = yield* stagedRecord(record).pipe(
        Effect.mapError(localRepresentationFailure),
      );
      yield* Effect.uninterruptible(
        runtime.outbox.stageRecord(fold.conversation, record, stored).pipe(
          Effect.catchTag("ClientRepresentationError", () =>
            Effect.fail(persistenceFailure()),
          ),
          Effect.zipRight(
            Effect.sync(() => {
              fold.recordHash = record.recordHash;
              runtime.recordFolds.set(record.recordHash, fold);
            }),
          ),
        ),
      );
    }
    yield* localDurabilityEvidence(runtime, fold);
    yield* maybePromote(runtime, fold, record);
  });

const maybeCertifyAction = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    if (!hasActionThreshold(fold) || fold.recordHash !== undefined) {
      return;
    }
    const record = yield* makeActionCertifiedRecord(
      fold,
      yield* actionAnchorHash(fold),
    ).pipe(Effect.mapError(localRepresentationFailure));
    yield* stageActionCertificate(runtime, fold, record);
  });

type ProtocolAcceptanceError =
  | ClientRepresentationError
  | EndpointStoreError
  | RouterWorkerPersistenceError;

const prepareProposalFold = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  proposal: ActionProposal,
): Effect.Effect<
  EngineActionFold | undefined,
  ProtocolAcceptanceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const conversation = yield* conversationForAction(
      runtime,
      proposal.action,
      ingress.routerInstanceId,
    );
    if (
      conversation === undefined ||
      !(yield* gapFree(conversation, proposal.action))
    ) {
      return undefined;
    }
    yield* verifyOuterMessage({
      message: ingress.message,
      membership: conversation.membership,
    });
    const verified = yield* verifyActionProposal({
      proposal,
      membership: conversation.membership,
      outerSenderAgentId: ingress.message.senderAgentId,
    });
    yield* lockAction(
      runtime,
      conversation,
      proposal.action,
      verified.actionHash,
    );
    return foldFor(runtime, conversation, proposal.action, verified.actionHash);
  });

const acceptProposal = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  proposal: ActionProposal,
): Effect.Effect<
  RouterIngressDisposition,
  ProtocolAcceptanceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const fold = yield* prepareProposalFold(runtime, ingress, proposal);
    if (fold === undefined) {
      return "ignored";
    }
    yield* localActionEvidence(runtime, fold);
    yield* maybeCertifyAction(runtime, fold);
    return "accepted";
  });

const acceptEvidence = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  message: SignedMessage,
  statement: EvidenceStatementValue,
): Effect.Effect<
  RouterIngressDisposition,
  ProtocolAcceptanceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const { route } = evidenceTarget(runtime, statement);
    if (route === undefined) {
      return "ignored";
    }
    const evidence = yield* verifyDeliveredEvidence({
      outer: ingress.message,
      evidence: message,
      statement,
      membership: route.fold.conversation.membership,
    });
    if (!evidenceMatchesFold(route, evidence.statement)) {
      return "ignored";
    }
    yield* mergeEvidence(route.fold, route.kind, evidence.message);
    yield* advanceEvidenceFold(runtime, route);
    return "accepted";
  });

function advanceEvidenceFold(
  runtime: EngineRuntime,
  route: EvidenceRoute,
): Effect.Effect<void, ProtocolAcceptanceError, SqlClient.SqlClient> {
  switch (route.kind) {
    case "action":
      return maybeCertifyAction(runtime, route.fold);
    case "durability":
      return advanceDurabilityFold(runtime, route.fold);
    default: {
      const exhaustive: never = route;
      return exhaustive;
    }
  }
}

function advanceDurabilityFold(
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<void, ProtocolAcceptanceError, SqlClient.SqlClient> {
  const recordHash = fold.recordHash;
  if (recordHash === undefined) {
    return Effect.void;
  }
  const record = runtime.recordFolds.get(recordHash);
  if (record === undefined || !hasDurabilityThreshold(record)) {
    return Effect.void;
  }
  return Effect.gen(function* () {
    const actionCertifiedRecord = yield* makeActionCertifiedRecord(
      record,
      yield* actionAnchorHash(record),
    ).pipe(Effect.mapError(localRepresentationFailure));
    yield* maybePromote(runtime, record, actionCertifiedRecord);
  });
}

const membershipForRecord = (
  runtime: EngineRuntime,
  record: ActionCertifiedRecord,
): Effect.Effect<
  VerifiedMembership,
  ClientRepresentationError,
  SqlClient.SqlClient
> =>
  ingressMembership(runtime, record.recordCore.action).pipe(
    Effect.tap((membership) =>
      verifyActionCertifiedRecord({ record, membership }),
    ),
  );

const ensureConversation = (
  runtime: EngineRuntime,
  membership: VerifiedMembership,
  record: ActionCertifiedRecord,
): EngineConversation | undefined => {
  const action = record.recordCore.action;
  const retained = runtime.conversations.get(action.conversationId);
  if (retained !== undefined) {
    return retained;
  }
  if (
    action.kind !== "FIRST_MESSAGE" ||
    record.routerAnchor.kind !== "initial_anchor_body"
  ) {
    return undefined;
  }
  return {
    conversationId: action.conversationId,
    membership,
    currentAnchor: record.routerAnchor,
  };
};

const mergeActionCertificate = (
  fold: EngineActionFold,
  signatures: readonly unknown[],
): Effect.Effect<void, ProtocolAcceptanceError, SqlClient.SqlClient> =>
  Effect.forEach(
    signatures,
    (representation) =>
      verifyStableEvidence({
        representation,
        membership: fold.conversation.membership,
      }).pipe(
        Effect.filterOrFail(
          ({ statement }) =>
            evidenceMatchesFold({ fold, kind: "action" }, statement),
          () => new ClientRepresentationError(),
        ),
        Effect.flatMap(({ message }) => mergeEvidence(fold, "action", message)),
      ),
    { concurrency: 1, discard: true },
  );

/**
 * Lock the action a verified record certifies over the lock this endpoint
 * holds on another MESSAGE at the same predecessor. The record's `q(n)` action
 * certificate means that other action can never be certified, because any
 * two `q(n)` quorums share an honest member, who signs one action at a
 * predecessor under one anchor. The store replaces the lock and keeps the
 * certificate's signatures with it. In the same uninterruptible step the other
 * action's fold is dropped, since the store now refuses its evidence, and the
 * certified action's fold takes the whole certificate, so this endpoint, which
 * signed the other action, never signs this one even when the rest of the
 * record's acceptance is interrupted. The store refuses the replacement when
 * this endpoint already staged the other action, and the record is ignored.
 * @param runtime Engine whose store and folds change.
 * @param conversation Conversation the record extends.
 * @param record The verified action-certified record.
 * @returns Completion once the record's action is locked here.
 */
const supersedeLock = (
  runtime: EngineRuntime,
  conversation: EngineConversation,
  record: ActionCertifiedRecord,
): Effect.Effect<void, ProtocolAcceptanceError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const { action, actionHash } = record.recordCore;
    const lock = yield* proposalLock(conversation, action, actionHash);
    const signatures = yield* Effect.forEach(
      record.actionCertificate.signatures,
      (representation) =>
        verifyStableEvidence({
          representation,
          membership: conversation.membership,
        }).pipe(Effect.map(({ message }) => message)),
      { concurrency: 1 },
    );
    const certificate = yield* Effect.forEach(
      signatures,
      (message) =>
        protocolEvidence(
          conversation.conversationId,
          "action",
          actionHash,
          message,
        ),
      { concurrency: 1 },
    );
    yield* Store.supersedeProposalLock(lock, certificate).pipe(
      Effect.zipRight(
        Effect.sync(() => {
          dropFoldsAtPredecessor(runtime, conversation, action, actionHash);
          const adopted = foldFor(runtime, conversation, action, actionHash);
          for (const message of signatures) {
            adopted.actionEvidence.set(message.senderAgentId, message);
          }
        }),
      ),
      Effect.uninterruptible,
    );
  });

function dropFoldsAtPredecessor(
  runtime: EngineRuntime,
  conversation: EngineConversation,
  action: ActionCore,
  kept: ActionHash,
): void {
  for (const [actionHash, fold] of runtime.actionFolds) {
    if (
      actionHash !== kept &&
      fold.conversation.conversationId === conversation.conversationId &&
      fold.action.previousRecordHash === action.previousRecordHash
    ) {
      runtime.actionFolds.delete(actionHash);
      if (fold.recordHash !== undefined) {
        runtime.recordFolds.delete(fold.recordHash);
      }
    }
  }
}

const prepareRecordFold = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  membership: VerifiedMembership,
  record: ActionCertifiedRecord,
): Effect.Effect<
  EngineActionFold | undefined,
  ProtocolAcceptanceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    yield* verifyOuterMessage({
      message: ingress.message,
      membership,
    });
    const conversation = ensureConversation(runtime, membership, record);
    const action = record.recordCore.action;
    if (conversation === undefined || !(yield* gapFree(conversation, action))) {
      return undefined;
    }
    yield* lockAction(
      runtime,
      conversation,
      action,
      record.recordCore.actionHash,
    ).pipe(
      Effect.catchTag("EndpointStoreError", (error) =>
        action.kind === "MESSAGE" && error.reason === "conflict"
          ? supersedeLock(runtime, conversation, record)
          : Effect.fail(error),
      ),
    );
    const fold = foldFor(
      runtime,
      conversation,
      action,
      record.recordCore.actionHash,
    );
    yield* mergeActionCertificate(fold, record.actionCertificate.signatures);
    return fold;
  });

const acceptActionCertifiedRecord = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  record: ActionCertifiedRecord,
): Effect.Effect<
  RouterIngressDisposition,
  ProtocolAcceptanceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const membership = yield* membershipForRecord(runtime, record);
    const fold = yield* prepareRecordFold(runtime, ingress, membership, record);
    if (fold === undefined) {
      return "ignored";
    }
    yield* stageActionCertificate(runtime, fold, record);
    return "accepted";
  });

function retainCertifiedFold(
  runtime: EngineRuntime,
  fold: EngineActionFold,
  record: ActionCertifiedRecord,
  votes: readonly SignedMessage[],
) {
  return Effect.sync(() => {
    fold.recordHash = record.recordHash;
    runtime.recordFolds.set(record.recordHash, fold);
    for (const vote of votes) {
      fold.durabilityEvidence.set(vote.senderAgentId, vote);
    }
  });
}

/**
 * Accept a record that arrives whole, with its durability certificate. The
 * store stages, certifies and promotes it in one transaction, and the fold
 * takes the record, its votes and its promotion in the same uninterruptible
 * step. Neither a crash nor an interruption therefore leaves the record
 * staged but not certified, where it would draw a durability vote from this
 * endpoint, which never voted for it.
 * @param runtime Engine that accepts the record.
 * @param ingress Authenticated delivery carrying the record.
 * @param record The record with its durability certificate.
 * @returns Whether the record was accepted or ignored.
 */
const acceptCertifiedRecord = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  record: CertifiedRecord,
): Effect.Effect<
  RouterIngressDisposition,
  ProtocolAcceptanceError,
  SqlClient.SqlClient
> =>
  Effect.gen(function* () {
    const membership = yield* ingressMembership(
      runtime,
      record.actionCertifiedRecord.recordCore.action,
    );
    const votes = (yield* verifyCertifiedRecord({ record, membership })).map(
      ({ message }) => message,
    );
    const actionRecord = record.actionCertifiedRecord;
    const fold = yield* prepareRecordFold(
      runtime,
      ingress,
      membership,
      actionRecord,
    );
    if (fold === undefined) {
      return "ignored";
    }
    const stored = yield* certifiedRecordRows(
      record,
      fold.actionEvidence.values(),
      votes,
    );
    yield* Effect.uninterruptible(
      Store.applyCertifiedRecord(stored).pipe(
        Effect.zipRight(
          retainCertifiedFold(runtime, fold, actionRecord, votes),
        ),
        Effect.zipRight(completePromotion(runtime, fold, record)),
      ),
    );
    yield* rebasePendingIntents(runtime, fold.conversation.conversationId);
    yield* runtime.phases.headAdvanced(
      runtime,
      fold.conversation.conversationId,
    );
    return "accepted";
  });

function acceptPacket(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  packet: Extract<DecodedOuterBody, { readonly kind: "direct" }>["packet"],
): Effect.Effect<
  RouterIngressDisposition,
  ProtocolAcceptanceError,
  SqlClient.SqlClient
> {
  switch (packet.kind) {
    case "action_proposal":
      return acceptProposal(runtime, ingress, packet);
    case "action_certified_record":
      return acceptActionCertifiedRecord(runtime, ingress, packet);
    case "certified_record":
      return acceptCertifiedRecord(runtime, ingress, packet);
    case "catch_up_request":
    case "catch_up_page":
    case "catch_up_incomplete":
    case "completed_reanchor":
      return Effect.succeed("ignored");
    default: {
      const exhaustive: never = packet;
      return exhaustive;
    }
  }
}

const ignoredDisposition: RouterIngressDisposition = "ignored";

/**
 * Apply one semantically verified Router-ordered protocol value.
 * @param runtime Current engine state and durable protocol dependencies.
 * @param ingress Authenticated Router-ordered protocol input.
 * @param statement Evidence decoded from this ingress's inner message, when available.
 * @returns Whether the input changed durable state or was irrelevant.
 */
export const acceptEngineIngress = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  statement?: EvidenceStatementValue,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  runtime.gate
    .withPermits(1)(
      Effect.gen(function* () {
        if (ingress.payload.kind === "direct") {
          return yield* acceptPacket(runtime, ingress, ingress.payload.packet);
        }
        const decoded =
          statement ??
          (yield* decodeCanonical(
            EvidenceStatement,
            ingress.payload.message.body,
          ));
        return yield* acceptEvidence(
          runtime,
          ingress,
          ingress.payload.message,
          decoded,
        );
      }),
    )
    .pipe(
      Effect.catchTag("ClientRepresentationError", () =>
        Effect.succeed(ignoredDisposition),
      ),
      Effect.catchTag("EndpointStoreError", (error) =>
        isSemanticStoreRejection(error)
          ? Effect.succeed(ignoredDisposition)
          : Effect.fail(persistenceFailure()),
      ),
      Effect.withSpan("acceptEngineIngress"),
    );

/**
 * Recovery accepts complete certified records through the same durable path.
 * @param runtime Current engine state and durable protocol dependencies.
 * @param ingress Authenticated recovery input from one fixed member.
 * @returns Whether recovery advanced durable state or ignored the input.
 */
export const acceptEngineRecoveryIngress = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient.SqlClient
> =>
  runtime.gate
    .withPermits(1)(
      ingress.payload.kind === "direct" &&
        ingress.payload.packet.kind === "certified_record"
        ? acceptCertifiedRecord(runtime, ingress, ingress.payload.packet)
        : Effect.succeed(ignoredDisposition),
    )
    .pipe(
      Effect.catchTag("ClientRepresentationError", () =>
        Effect.succeed(ignoredDisposition),
      ),
      Effect.catchTag("EndpointStoreError", (error) =>
        isSemanticStoreRejection(error)
          ? Effect.succeed(ignoredDisposition)
          : Effect.fail(persistenceFailure()),
      ),
      Effect.withSpan("acceptEngineRecoveryIngress"),
    );

/**
 * Resume the evidence one fold owes from durable state. A fold whose input the
 * store refuses, such as a record under an anchor this endpoint has staged a
 * re-anchor candidate away from, resumes nothing past the refusal, as live
 * ingress ignores the same refusal, so only that fold's conversation waits.
 * @param runtime Recovered engine state and durable protocol dependencies.
 * @param fold Fold whose evidence resumes.
 * @returns Completion once the fold's due evidence is queued or refused.
 */
const resumeFold = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    const retainedRecord =
      fold.recordHash === undefined
        ? undefined
        : yield* makeActionCertifiedRecord(
            fold,
            yield* actionAnchorHash(fold),
          ).pipe(Effect.mapError(localRepresentationFailure));
    if (
      retainedRecord !== undefined &&
      (yield* Store.readRecordOutbound(
        fold.conversation.conversationId,
        retainedRecord.recordHash,
      )) !== undefined
    ) {
      const stored = yield* stagedRecord(retainedRecord).pipe(
        Effect.mapError(localRepresentationFailure),
      );
      yield* runtime.outbox
        .stageRecord(fold.conversation, retainedRecord, stored)
        .pipe(
          Effect.catchTag("ClientRepresentationError", () =>
            Effect.fail(persistenceFailure()),
          ),
        );
    }
    yield* localActionEvidence(runtime, fold);
    yield* maybeCertifyAction(runtime, fold);
    if (fold.recordHash !== undefined) {
      yield* localDurabilityEvidence(runtime, fold);
      const record =
        retainedRecord ??
        (yield* makeActionCertifiedRecord(
          fold,
          yield* actionAnchorHash(fold),
        ).pipe(Effect.mapError(localRepresentationFailure)));
      yield* maybePromote(runtime, fold, record);
    }
  }).pipe(
    Effect.catchTag("EndpointStoreError", (error) =>
      isSemanticStoreRejection(error)
        ? Effect.logWarning(
            "Fold resume stopped: the endpoint store refused it; its conversation waits",
          ).pipe(
            Effect.annotateLogs({
              conversationId: fold.conversation.conversationId,
              actionHash: fold.actionHash,
              reason: error.reason,
            }),
          )
        : Effect.fail(persistenceFailure()),
    ),
  );

/**
 * Resume only the evidence obligations already selected in durable state.
 * @param runtime Recovered engine state and durable protocol dependencies.
 * @param conversationId The one conversation to resume; every conversation
 *     when omitted.
 * @returns Completion after all resumable evidence work has been queued.
 */
export const resumeEngineFolds = (
  runtime: EngineRuntime,
  conversationId?: ConversationId,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient> =>
  runtime.gate
    .withPermits(1)(
      Effect.forEach(
        [...runtime.actionFolds.values()].filter(
          (fold) =>
            conversationId === undefined ||
            fold.conversation.conversationId === conversationId,
        ),
        (fold) => resumeFold(runtime, fold),
        { concurrency: 1, discard: true },
      ),
    )
    .pipe(Effect.withSpan("resumeEngineFolds"));
