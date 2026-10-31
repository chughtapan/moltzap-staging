/**
 * @file The engine kernel: the dependencies one endpoint engine is built
 * from and the state every phase reads. It holds no phase logic, so every
 * phase can depend on it without depending on another phase.
 */

import type * as SqlClient from "@effect/sql/SqlClient";
import type { Context, Deferred, Effect, Scope } from "effect";
import type { ClientRepresentationError } from "../../encoding/canonical-json.js";
import type { VerifiedAgentCard } from "../../identity/cards.js";
import type { SignedMessage } from "../../identity/envelope/signed-message.js";
import type { AgentId } from "../../identity/identifiers.js";
import type {
  AgentSigningAuthority,
  Ed25519PublicKey,
} from "../../identity/keys.js";
import type { Registry } from "../../identity/registry/index.js";
import type { Message } from "../message.js";
import type {
  ActionCertifiedRecord,
  ActionCore,
  ActionHash,
  CertifiedRecord,
  ConversationId,
  DecodedOuterBody,
  DirectPacket,
  EvidenceStatement,
  MessageIntent,
  RecordHash,
  RouterAnchor,
  VerifiedEvidence,
  VerifiedMembership,
} from "../records/index.js";
import type {
  RouterIngressDisposition,
  RouterWorker,
  RouterWorkerIngress,
  RouterWorkerPersistenceError,
  RouterWorkerSendError,
} from "../router/index.js";
import type { EndpointStoreError, StagedRecord } from "../store/index.js";
import type { SendError } from "./errors.js";

/** RouterWorker operations consumed by the engine's outbound queue. */
type EngineRouterPort = Pick<
  RouterWorker,
  "currentAnchor" | "awaitAnchor" | "send"
>;

/** Stable private dependencies for one endpoint protocol engine. */
export interface EndpointEngineInput {
  readonly localAgentCard: VerifiedAgentCard;
  readonly signingAuthority: AgentSigningAuthority;
  readonly registrySignerPublicKey: Ed25519PublicKey;
  readonly registry: Pick<Context.Tag.Service<typeof Registry>, "lookup">;
  readonly routerWorker: EngineRouterPort;
  /**
   * Report a store failure that may leave a message's intent bound but not
   * proposed: an uncertain persistence failure while binding it, or any store failure
   * while queueing its proposal. The host must stop, so recovery after the
   * restart proposes the intent; a host that kept running would leave it
   * waiting for a head move that a new conversation never gets.
   */
  readonly reportStorageFault: (
    error: EndpointStoreError,
  ) => Effect.Effect<void>;
}

/** Preparation uses the exact payload already retained by Communication. */
export interface AcceptedMessage {
  readonly message: Message;
  readonly canonicalMessage: Uint8Array;
}

/** One locally authored immutable message intent awaiting certification. */
export interface EngineMessageIntent {
  readonly intent: MessageIntent;
  readonly canonicalIntent: Uint8Array;
  readonly completion: Deferred.Deferred<RecordHash, SendError>;
  proposedActionHash?: ActionHash;
}

/** One locally complete history head. */
export interface EngineCertifiedHead {
  readonly recordHash: RecordHash;
  readonly record: CertifiedRecord;
}

/** Immutable membership with the endpoint's current durable position. */
export interface EngineConversation {
  readonly conversationId: ConversationId;
  readonly membership: VerifiedMembership;
  currentAnchor: RouterAnchor;
  head?: EngineCertifiedHead;
}

/** Volatile evidence fold for the selected action at one predecessor. */
export interface EngineActionFold {
  readonly conversation: EngineConversation;
  readonly action: ActionCore;
  readonly actionHash: ActionHash;
  readonly routerAnchor: RouterAnchor;
  readonly actionEvidence: Map<AgentId, SignedMessage>;
  readonly durabilityEvidence: Map<AgentId, SignedMessage>;
  localActionEvidenceQueued: boolean;
  localDurabilityEvidenceQueued: boolean;
  recordHash?: RecordHash;
  certified: boolean;
}

/**
 * Open an empty fold for one action: no evidence, and no local signature
 * queued yet, so certification decides what to sign and send.
 * @param conversation Conversation whose next position the action proposes.
 * @param action Selected action the fold collects evidence for.
 * @param actionHash Hash that action and durability evidence name.
 * @param routerAnchor Anchor the action's record certifies against.
 * @returns A fold the caller registers in `EngineRuntime.actionFolds`.
 */
export function makeActionFold(
  conversation: EngineConversation,
  action: ActionCore,
  actionHash: ActionHash,
  routerAnchor: RouterAnchor,
): EngineActionFold {
  return {
    conversation,
    action,
    actionHash,
    routerAnchor,
    actionEvidence: new Map(),
    durabilityEvidence: new Map(),
    localActionEvidenceQueued: false,
    localDurabilityEvidenceQueued: false,
    certified: false,
  };
}

/** Shared acquired state used by addressed send and protocol ingress. */
export interface EngineRuntime {
  readonly wireVersion: string;
  readonly input: EndpointEngineInput;
  readonly conversations: Map<ConversationId, EngineConversation>;
  readonly intents: Map<string, EngineMessageIntent>;
  /** Locally complete message ids and the hash of each one's certified record. */
  readonly completedMessages: Map<string, RecordHash>;
  readonly actionFolds: Map<ActionHash, EngineActionFold>;
  readonly recordFolds: Map<RecordHash, EngineActionFold>;
  readonly gate: Effect.Semaphore;
  readonly outbox: EngineOutbox;
  readonly phases: EnginePhases;
  /**
   * The engine's lifetime. Recovery work that outlives one Router callback,
   * such as a conversation's catch-up retries, runs in a child of it, so it
   * ends with the engine.
   */
  readonly scope: Scope.Scope;
}

/**
 * Why the outbox could not stage an envelope: signing or canonical encoding
 * failed, or the store refused the row. Each phase maps it to its own error.
 */
export type EngineOutboxError = ClientRepresentationError | EndpointStoreError;

/**
 * The engine's outbox: the only caller of the outer-envelope signers, so the
 * engine builds and signs every outer body here; the Router worker sends and
 * resends staged bytes without signing. Its queue operations always stage the
 * signed envelope durably and queue its outbox identity for the Router
 * worker; `sign` serves a caller that routes the envelope itself. Phases
 * reach it only through `EngineRuntime.outbox`.
 */
export interface EngineOutbox {
  /** Sign an envelope for a caller that routes it, as recovery does. */
  readonly sign: (
    membership: VerifiedMembership,
    body: DecodedOuterBody,
  ) => Effect.Effect<SignedMessage, ClientRepresentationError>;
  readonly queuePacket: (
    conversation: EngineConversation,
    packet: DirectPacket,
  ) => Effect.Effect<void, EngineOutboxError, SqlClient.SqlClient>;
  /** Relay stable inner evidence; its signer attribution is unchanged. */
  readonly queueEvidence: (
    conversation: EngineConversation,
    evidence: SignedMessage,
  ) => Effect.Effect<void, EngineOutboxError, SqlClient.SqlClient>;
  /**
   * Stage a record together with its signed envelope before any durability vote.
   */
  readonly stageRecord: (
    conversation: EngineConversation,
    packet: ActionCertifiedRecord,
    record: StagedRecord,
  ) => Effect.Effect<void, EngineOutboxError, SqlClient.SqlClient>;
  /**
   * Stage an envelope that `sign` returned; any other `SignedMessage` would
   * skip the outbox's signing.
   */
  readonly enqueueSigned: (
    conversationId: ConversationId,
    message: SignedMessage,
  ) => Effect.Effect<void, EngineOutboxError, SqlClient.SqlClient>;
  readonly resume: (envelopeIds: readonly string[]) => Effect.Effect<void>;
  /** Forget every queued identity; the store keeps the envelopes. */
  readonly clear: () => void;
  /** Run `effect` while no drain reads or removes the queue head. */
  readonly serialized: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
  readonly drain: Effect.Effect<
    void,
    RouterWorkerSendError,
    SqlClient.SqlClient
  >;
  /** Drain on every wake; ends only with a fatal worker failure. */
  readonly run: Effect.Effect<
    never,
    RouterWorkerSendError,
    SqlClient.SqlClient
  >;
}

/**
 * The operations one engine phase starts in another. The engine assembly
 * supplies them, so send, certification, dissemination and recovery depend
 * on this contract instead of on each other.
 */
export interface EnginePhases {
  readonly proposeIntent: (
    runtime: EngineRuntime,
    intent: EngineMessageIntent,
  ) => Effect.Effect<ActionHash, SendError, SqlClient.SqlClient>;
  readonly acceptIngress: (
    runtime: EngineRuntime,
    ingress: RouterWorkerIngress<DecodedOuterBody>,
    statement?: EvidenceStatement,
  ) => Effect.Effect<
    RouterIngressDisposition,
    RouterWorkerPersistenceError,
    SqlClient.SqlClient
  >;
  readonly acceptRecoveryIngress: (
    runtime: EngineRuntime,
    ingress: RouterWorkerIngress<DecodedOuterBody>,
  ) => Effect.Effect<
    RouterIngressDisposition,
    RouterWorkerPersistenceError,
    SqlClient.SqlClient
  >;
  /** Resume the evidence work of one recovered conversation's folds. */
  readonly resumeFolds: (
    runtime: EngineRuntime,
    conversationId: ConversationId,
  ) => Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient>;
  /** Arm catch-up again for a conversation whose retries ran out. */
  readonly rearmCatchUp: (
    runtime: EngineRuntime,
    conversationId: ConversationId,
  ) => Effect.Effect<void, never, SqlClient.SqlClient>;
  /** Restart recovery after either form of certification promotes a fenced head. */
  readonly headAdvanced: (
    runtime: EngineRuntime,
    conversationId: ConversationId,
  ) => Effect.Effect<void, RouterWorkerPersistenceError, SqlClient.SqlClient>;
  /** Whether a conversation is still fenced while it recovers. */
  readonly isRecovering: (
    runtime: EngineRuntime,
    conversationId: ConversationId,
  ) => boolean;
}

/** A protocol fold selected by an evidence statement's signed subject. */
export type EvidenceRoute =
  | Readonly<{ fold: EngineActionFold; kind: "action" }>
  | Readonly<{ fold: EngineActionFold; kind: "durability" }>;

/**
 * Resolve evidence once for both recovery fencing and active certification.
 * @param runtime Engine holding the active folds.
 * @param statement Decoded statement whose signature has not yet been checked.
 * @returns Its named conversation and active fold, when known.
 */
export function evidenceTarget(
  runtime: EngineRuntime,
  statement: EvidenceStatement,
): {
  readonly conversationId?: ConversationId;
  readonly route?: EvidenceRoute;
} {
  switch (statement.kind) {
    case "action_signature": {
      const fold = runtime.actionFolds.get(statement.actionHash);
      return fold === undefined
        ? {}
        : {
            conversationId: fold.conversation.conversationId,
            route: { fold, kind: "action" },
          };
    }
    case "durability_vote": {
      const fold = runtime.recordFolds.get(statement.recordHash);
      return {
        conversationId: statement.conversationId,
        ...(fold === undefined
          ? {}
          : { route: { fold, kind: "durability" as const } }),
      };
    }
    case "reanchor_vote":
      return { conversationId: statement.reanchor.conversationId };
    case "catch_up_attestation":
      return { conversationId: statement.request.conversationId };
    default: {
      const exhaustive: never = statement;
      return exhaustive;
    }
  }
}

/**
 * Bind verified evidence to the active fold selected by its signed subject.
 * @param route Active fold and the certificate role the statement targets.
 * @param statement Cryptographically verified evidence statement.
 * @returns Whether all fold-specific bindings match.
 */
export function evidenceMatchesFold(
  route: EvidenceRoute,
  statement: VerifiedEvidence["statement"],
): boolean {
  const { fold } = route;
  switch (route.kind) {
    case "action":
      return (
        statement.kind === "action_signature" &&
        statement.actionHash === fold.actionHash
      );
    case "durability":
      if (
        statement.kind !== "durability_vote" ||
        fold.recordHash === undefined
      ) {
        return false;
      }
      return (
        statement.recordHash === fold.recordHash &&
        statement.conversationId === fold.conversation.conversationId &&
        statement.membershipHash === fold.conversation.membership.hash
      );
    default: {
      const exhaustive: never = route;
      return exhaustive;
    }
  }
}
