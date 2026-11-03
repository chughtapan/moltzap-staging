/**
 * @file Selects ordered proposals, certifies records and resumes pending delivery.
 */

import type { SqlClient } from "@effect/sql/SqlClient";
import { absurd, Effect } from "effect";
import type { SignedMessage } from "../../identity/message-security.js";
import type { SendError } from "../errors.js";
import type {
  EngineActionFold,
  EngineConversation,
  EngineMessageIntent,
  EngineRuntime,
} from "./conversation.js";
import {
  ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  equalCanonical,
} from "../../encoding/canonical-json.js";
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
} from "../records.js";
import {
  type RouterIngressDisposition,
  type RouterWorkerIngress,
  RouterWorkerPersistenceError,
} from "../router/connection.js";
import {
  applyCertifiedRecord,
  type ConversationFoundation,
  type EndpointStoreError,
  isSemanticStoreRejection,
  lockFirstMessageProposal,
  lockProposal,
  promoteRecord,
  type ProposalLock,
  readRecordOutbound,
  mergeEvidence as storedMergeEvidence,
  supersedeProposalLock,
} from "../store.js";
import {
  evidenceMatchesFold,
  type EvidenceRoute,
  evidenceTarget,
  makeActionCertifiedRecord,
  makeActionFold,
  makeCertifiedRecord,
  protocolEvidence,
  recordAnchorHash,
  stagedRecord,
  storedCertifiedRecord,
} from "./history.js";

// safer-arch-ignore no-module-cycle: Conversation state imports are type-only; runtime dependencies point into the algorithms.
// safer-arch-ignore no-fat-orchestrator: One private operation owns the complete certification lifecycle inside Communication.

const persistenceFailure = () => new RouterWorkerPersistenceError();

const localRepresentationFailure = (): RouterWorkerPersistenceError =>
  persistenceFailure();

const currentAnchorHash = (
  conversation: EngineConversation,
): Effect.Effect<AnchorHash, RouterWorkerPersistenceError, SqlClient> =>
  conversation.currentAnchor.kind === "initial_anchor_body"
    ? hashAnchor(conversation.currentAnchor).pipe(
        Effect.mapError(localRepresentationFailure),
      )
    : Effect.succeed(conversation.currentAnchor.anchorHash);

const gapFree = (
  conversation: EngineConversation,
  action: ActionCore,
): Effect.Effect<boolean, RouterWorkerPersistenceError, SqlClient> =>
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
 * FIRST_MESSAGE can supply verified membership for an unknown conversation.
 * MESSAGE carries only its hash and requires membership already held here.
 */
const ingressMembership = (
  runtime: EngineRuntime,
  action: ActionCore,
): Effect.Effect<VerifiedMembership, ClientRepresentationError, SqlClient> => {
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
  SqlClient
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
  SqlClient
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
): Effect.Effect<ProposalLock, RouterWorkerPersistenceError, SqlClient> =>
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
  SqlClient
> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
    const lock = yield* proposalLock(conversation, action, actionHash);
    if (
      action.kind === "FIRST_MESSAGE" &&
      !runtime.conversations.has(action.conversationId)
    ) {
      const foundation = yield* firstMessageFoundation(conversation, action);
      yield* lockFirstMessageProposal(agentKey, foundation, lock);
      yield* Effect.sync(() => {
        runtime.conversations.set(action.conversationId, conversation);
      });
      return;
    }
    yield* lockProposal(agentKey, lock);
  });
};

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
  agentKey: string,
  fold: EngineActionFold,
  role: LocalEvidenceRole,
  message: SignedMessage,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient
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
    yield* storedMergeEvidence(agentKey, row);
    const evidence =
      role === "action" ? fold.actionEvidence : fold.durabilityEvidence;
    yield* Effect.sync(() => {
      evidence.set(message.senderAgentId, message);
    });
  });

/**
 * Persists evidence before queueing it. Queueing and the fold's queued flag
 * share an interruption boundary.
 */
const completeLocalEvidence = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
  role: LocalEvidenceRole,
  evidence: SignedMessage,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient
> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
    const retained =
      role === "action" ? fold.actionEvidence : fold.durabilityEvidence;
    if (!retained.has(runtime.input.localAgentCard.agentId)) {
      yield* mergeEvidence(agentKey, fold, role, evidence);
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
};

const localActionEvidence = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient
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
 * Reuses a retained action signature or creates one. Never signs after an
 * action certificate arrives: this endpoint may have signed the competing
 * action whose lock that certificate superseded.
 */
function selectLocalActionEvidence(
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  SignedMessage | undefined,
  RouterWorkerPersistenceError,
  SqlClient
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
): Effect.Effect<SignedMessage, RouterWorkerPersistenceError, SqlClient> {
  return signEvidenceMessage({
    statement: {
      sharnessVersion: runtime.wireVersion,
      kind: "action_signature",
      signerAgentId: runtime.input.localAgentCard.agentId,
      actionHash: fold.actionHash,
    },
    identityCard: runtime.input.localAgentCard,
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
): Effect.Effect<AnchorHash, RouterWorkerPersistenceError, SqlClient> =>
  recordAnchorHash(fold).pipe(Effect.mapError(localRepresentationFailure));

/**
 * Votes only for locally staged, uncertified records. Records received whole
 * are staged and certified atomically, so they never draw a new local vote.
 */
const localDurabilityEvidence = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient
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
        identityCard: runtime.input.localAgentCard,
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

/** Updates the certified head and local completion index after durable promotion. */
function completePromotion(
  runtime: EngineRuntime,
  fold: EngineActionFold,
  record: CertifiedRecord,
): Effect.Effect<void> {
  return Effect.sync(() => {
    fold.certified = true;
    fold.conversation.head = {
      recordHash: record.actionCertifiedRecord.recordHash,
      record,
    };
    if (
      fold.action.messageIntent.authorAgentId !==
      runtime.input.localAgentCard.agentId
    ) {
      return;
    }
    runtime.intents.delete(fold.action.messageIntent.msgId);
    runtime.completedMessages.add(fold.action.messageIntent.msgId);
  });
}

type ReproposalDisposition = "fail" | "ignore";

const reproposalDispositionByReason = {
  "outcome-unknown": "fail",
  "certification-unavailable": "ignore",
  "content-invalid": "fail",
  "message-conflict": "fail",
  "invalid-address": "fail",
  "membership-invalid": "fail",
  "network-unavailable": "ignore",
  "delivery-pending": "ignore",
  "not-registered": "fail",
  "persistence-failed": "fail",
  "unknown-agent": "fail",
  "version-mismatch": "fail",
} as const satisfies Readonly<
  Record<SendError["reason"], ReproposalDisposition>
>;

const reproposePendingIntent = (
  runtime: EngineRuntime,
  pending: EngineMessageIntent,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> =>
  runtime.phases.proposeIntent(runtime, pending).pipe(
    Effect.asVoid,
    Effect.catchTag("SendError", (error) =>
      reproposalDispositionByReason[error.reason] === "ignore"
        ? Effect.void
        : Effect.fail(new RouterWorkerPersistenceError()),
    ),
  );

/**
 * Recovery proposes pending messages at its settled head. Proposing while
 * fenced would lose the author's Router echo and leave peers signing alone.
 */
const rebasePendingIntents = (
  runtime: EngineRuntime,
  conversationId: EngineConversation["conversationId"],
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> =>
  runtime.phases.isRecovering(runtime, conversationId)
    ? Effect.void
    : Effect.forEach(
        runtime.intents.values(),
        (pending) =>
          pending.intent.conversationId === conversationId
            ? reproposePendingIntent(runtime, pending)
            : Effect.void,
        { concurrency: 1, discard: true },
      );

/**
 * Promotes durable state and adopts the head in one interruption boundary so
 * folds and local completion cannot lag a committed record.
 */
const promote = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
  record: CertifiedRecord,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient
> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
    const stored = yield* certifiedRecordRows(
      record,
      fold.actionEvidence.values(),
      fold.durabilityEvidence.values(),
    );
    yield* Effect.uninterruptible(
      promoteRecord(agentKey, stored).pipe(
        Effect.zipRight(completePromotion(runtime, fold, record)),
      ),
    );
    yield* rebasePendingIntents(runtime, fold.conversation.conversationId);
    yield* runtime.phases.headAdvanced(
      runtime,
      fold.conversation.conversationId,
    );
  });
};

const maybePromote = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
  actionCertifiedRecord: ActionCertifiedRecord,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient
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
 * Stages before voting and promotes at `q(n)` votes. Disseminates a local copy
 * ahead of the vote because a faulty sender may have encrypted its copy so
 * some members cannot open it.
 */
const stageActionCertificate = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
  record: ActionCertifiedRecord,
): Effect.Effect<
  void,
  EndpointStoreError | RouterWorkerPersistenceError,
  SqlClient
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
  SqlClient
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
  SqlClient
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
  SqlClient
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
  SqlClient
> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
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
    yield* mergeEvidence(agentKey, route.fold, route.kind, evidence.message);
    yield* advanceEvidenceFold(runtime, route);
    return "accepted";
  });
};

function advanceEvidenceFold(
  runtime: EngineRuntime,
  route: EvidenceRoute,
): Effect.Effect<void, ProtocolAcceptanceError, SqlClient> {
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
): Effect.Effect<void, ProtocolAcceptanceError, SqlClient> {
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
): Effect.Effect<VerifiedMembership, ClientRepresentationError, SqlClient> =>
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
  agentKey: string,
  fold: EngineActionFold,
  signatures: readonly unknown[],
): Effect.Effect<void, ProtocolAcceptanceError, SqlClient> =>
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
        Effect.flatMap(({ message }) =>
          mergeEvidence(agentKey, fold, "action", message),
        ),
      ),
    { concurrency: 1, discard: true },
  );

/**
 * Replaces a competing proposal lock with the verified action certificate and
 * drops its fold atomically. Retaining the certificate prevents this endpoint
 * from signing again if interrupted. A staged competitor refuses replacement:
 * it already required a conflicting quorum certificate.
 */
const supersedeLock = (
  runtime: EngineRuntime,
  conversation: EngineConversation,
  record: ActionCertifiedRecord,
): Effect.Effect<void, ProtocolAcceptanceError, SqlClient> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
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
    yield* supersedeProposalLock(agentKey, lock, certificate).pipe(
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
};

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
  SqlClient
> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
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
    yield* mergeActionCertificate(
      agentKey,
      fold,
      record.actionCertificate.signatures,
    );
    return fold;
  });
};

const acceptActionCertifiedRecord = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  record: ActionCertifiedRecord,
): Effect.Effect<
  RouterIngressDisposition,
  ProtocolAcceptanceError,
  SqlClient
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
 * Stages and promotes a complete certified record atomically with its fold.
 * A crash must not leave it staged alone, where recovery would cast a local
 * vote for a record this endpoint received already certified.
 */
const acceptCertifiedRecord = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  record: CertifiedRecord,
): Effect.Effect<
  RouterIngressDisposition,
  ProtocolAcceptanceError,
  SqlClient
> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
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
      applyCertifiedRecord(agentKey, stored).pipe(
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
};

function acceptPacket(
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  packet: Extract<DecodedOuterBody, { readonly kind: "direct" }>["packet"],
): Effect.Effect<RouterIngressDisposition, ProtocolAcceptanceError, SqlClient> {
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
 * Applies verified Router-ordered input. Returns whether it changed durable
 * state or was irrelevant.
 */
export const acceptEngineIngress = (
  runtime: EngineRuntime,
  ingress: RouterWorkerIngress<DecodedOuterBody>,
  statement?: EvidenceStatementValue,
): Effect.Effect<
  RouterIngressDisposition,
  RouterWorkerPersistenceError,
  SqlClient
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
  SqlClient
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
 * Resumes retained evidence obligations. A store refusal stops only that fold,
 * including one under an anchor a staged re-anchor is leaving.
 */
const resumeFold = (
  runtime: EngineRuntime,
  fold: EngineActionFold,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> => {
  const agentKey = runtime.input.agentKey;
  return Effect.gen(function* () {
    const retainedRecord =
      fold.recordHash === undefined
        ? undefined
        : yield* makeActionCertifiedRecord(
            fold,
            yield* actionAnchorHash(fold),
          ).pipe(Effect.mapError(localRepresentationFailure));
    if (
      retainedRecord !== undefined &&
      (yield* readRecordOutbound(
        agentKey,
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
};

/**
 * Resumes durable evidence obligations for one conversation, or all when
 * `conversationId` is omitted.
 */
export const resumeEngineFolds = (
  runtime: EngineRuntime,
  conversationId?: ConversationId,
): Effect.Effect<void, RouterWorkerPersistenceError, SqlClient> =>
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
