/**
 * @file Signed two-member history, the store rows its endpoint
 * keeps, and the corrupt reads of those rows stored-history tests start from.
 */

import type { SqlClient } from "@effect/sql";
import { Effect, Schema } from "effect";
import type {
  ProtocolEvidence,
  CertifiedRecord as StoredCertifiedRecord,
} from "../communication/store/index.js";
import type { AgentSigningAuthority } from "../identity/keys.js";
import type { TestStore } from "./store-schema-fixtures.js";
import {
  protocolEvidence,
  stagedRecord,
} from "../communication/conversations/history/index.js";
import { Message, messageFromIntent } from "../communication/message.js";
import {
  ActionCore,
  type ActionHash,
  type CertifiedRecord,
  type EvidenceStatement,
  type FirstMessageActionCore,
  hashAction,
  hashAnchor,
  hashMessageIntent,
  hashRecord,
  type InitialAnchorBody,
  InitialAnchorBody as InitialAnchorBodySchema,
  MembershipDescriptor,
  type MessageActionCore,
  MessageIntent,
  mintMsgId,
  type RecordCore,
  type RecordHash,
  signEvidenceMessage,
  type VerifiedMembership,
} from "../communication/records/index.js";
import { encodeCanonical } from "../encoding/canonical-json.js";
import {
  AgentCard,
  agentViewOf,
  type VerifiedAgentCard,
} from "../identity/cards.js";
import { SignedMessage } from "../identity/envelope/signed-message.js";
import { WIRE_VERSION } from "../identity/wire-version.js";

/** One member's verified card and the authority that signs as it. */
export interface SigningIdentity {
  readonly card: VerifiedAgentCard;
  readonly authority: AgentSigningAuthority;
}

/** A FIRST_MESSAGE record core with its anchor and both certificates' signers. */
interface CertifiedRecordParts {
  readonly recordCore: RecordCore;
  readonly recordHash: RecordHash;
  readonly anchor: InitialAnchorBody;
  readonly signatures: readonly [unknown, unknown];
  readonly votes: readonly [unknown, unknown];
}

/**
 * A FIRST_MESSAGE record authored by `remote` and certified by both members, on a
 * initial anchor at `routerInstanceId`. Each certificate lists `local`'s
 * signature first, whatever the members' AgentId order.
 * @param local One member of the two-member conversation.
 * @param remote The other member, who authors the message.
 * @param membership The verified two-member membership.
 * @param routerInstanceId The Router instance the initial anchor binds.
 * @returns The complete certified record.
 */
export const buildCertifiedFirstMessage = (
  local: SigningIdentity,
  remote: SigningIdentity,
  membership: VerifiedMembership,
  routerInstanceId: InitialAnchorBody["routerInstanceId"],
): Effect.Effect<CertifiedRecord> =>
  Effect.gen(function* () {
    const action = yield* firstMessageAction(
      remote,
      membership,
      routerInstanceId,
    );
    const actionHash = yield* hashAction(action);
    const recordCore: RecordCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "record_core",
      membership: membership.descriptor,
      anchorHash: yield* hashAnchor(action.anchor),
      action,
      actionHash,
    };
    const recordHash = yield* hashRecord(recordCore);
    return certifiedRecord({
      recordCore,
      recordHash,
      anchor: action.anchor,
      signatures: yield* Effect.all([
        signAction(local, actionHash),
        signAction(remote, actionHash),
      ]),
      votes: yield* Effect.all([
        voteDurable(local, membership, recordHash),
        voteDurable(remote, membership, recordHash),
      ]),
    });
  }).pipe(Effect.orDie, Effect.withSpan("buildCertifiedFirstMessage"));

/**
 * Keep a certified FIRST_MESSAGE record in `local`'s store as its endpoint does:
 * bind the local identity, lock the FIRST_MESSAGE proposal with the conversation's
 * membership and initial anchor, then keep the record row, with one evidence
 * row per signer. A remote author's message also becomes a local delivery.
 * @param store The endpoint store to write.
 * @param local The card of the agent whose store this is.
 * @param membership The record's verified membership.
 * @param record A record from `buildCertifiedFirstMessage`.
 * @returns Completion once every write is durable.
 */
export const storeCertifiedFirstMessage = (
  store: TestStore,
  local: VerifiedAgentCard,
  membership: VerifiedMembership,
  record: CertifiedRecord,
): Effect.Effect<void, never, SqlClient.SqlClient> =>
  Effect.gen(function* () {
    yield* store.bindIdentity({
      agentId: local.agentId,
      canonicalAgentCard: yield* encodeCanonical(AgentCard, local),
    });
    yield* lockFirstMessage(store, membership, record);
    yield* bindLocalIntent(store, local, membership, record);
    yield* store.applyCertifiedRecord(yield* storedRecordRow(record));
  }).pipe(Effect.orDie, Effect.withSpan("storeCertifiedFirstMessage"));

/** The real endpoint store and the two members that certify each history extension. */
export interface HistoryEndpoint {
  readonly store: TestStore;
  readonly local: SigningIdentity;
  readonly remote: SigningIdentity;
  readonly membership: VerifiedMembership;
}

/**
 * Extend a two-member certified history with a signed remote or local message
 * carrying `text` as its one part, or exactly the given content.
 */
export function appendCertifiedMessage(
  endpoint: HistoryEndpoint,
  previous: CertifiedRecord,
  text: string | MessageIntent["content"],
  authoredLocally = false,
): Effect.Effect<CertifiedRecord, never, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const messageIntent: MessageIntent = {
      ...previous.actionCertifiedRecord.recordCore.action.messageIntent,
      authorAgentId: (authoredLocally ? endpoint.local : endpoint.remote).card
        .agentId,
      msgId: yield* mintMsgId(),
      content: typeof text === "string" ? [{ type: "text", text }] : text,
    };
    return yield* appendCertifiedIntent(endpoint, previous, messageIntent);
  }).pipe(Effect.orDie, Effect.withSpan("appendCertifiedMessage"));
}

/**
 * Sign one evidence statement as `identity` and encode it as the JWS
 * representation a certificate carries.
 * @param identity The member who signs.
 * @param statement The evidence statement it signs.
 * @returns The encoded signer message.
 */
export function signEvidence(
  identity: SigningIdentity,
  statement: EvidenceStatement,
) {
  return signEvidenceMessage({
    statement,
    agentCard: identity.card,
    signingAuthority: identity.authority,
  }).pipe(Effect.flatMap((message) => Schema.encode(SignedMessage)(message)));
}

/** Certify the exact retained intent so recovery tests never replace an uncertain message. */
function appendCertifiedIntent(
  endpoint: HistoryEndpoint,
  previous: CertifiedRecord,
  intent: MessageIntent,
): Effect.Effect<CertifiedRecord, never, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const record = yield* buildCertifiedMessage(endpoint, previous, intent);
    yield* storeCertifiedMessage(
      endpoint,
      previous.actionCertifiedRecord,
      record,
    );
    return record;
  }).pipe(Effect.orDie, Effect.withSpan("appendCertifiedIntent"));
}

/** Build a genuine certificate without advancing the receiving engine's store. */
function buildCertifiedMessage(
  endpoint: Pick<HistoryEndpoint, "local" | "remote" | "membership">,
  previous: CertifiedRecord,
  intent: MessageIntent,
): Effect.Effect<CertifiedRecord> {
  const { local, remote, membership } = endpoint;
  return Effect.gen(function* () {
    const predecessor = previous.actionCertifiedRecord;
    if (predecessor.routerAnchor.kind !== "initial_anchor_body") {
      return yield* Effect.dieMessage("expected a initially anchored history");
    }
    const recordCore = yield* messageRecordCore(
      membership,
      predecessor,
      intent,
    );
    const actionHash = recordCore.actionHash;
    const recordHash = yield* hashRecord(recordCore);
    const record = certifiedRecord({
      recordCore,
      recordHash,
      anchor: predecessor.routerAnchor,
      signatures: yield* Effect.all([
        signAction(local, actionHash),
        signAction(remote, actionHash),
      ]),
      votes: yield* Effect.all([
        voteDurable(local, membership, recordHash),
        voteDurable(remote, membership, recordHash),
      ]),
    });
    return record;
  }).pipe(Effect.orDie, Effect.withSpan("buildCertifiedMessage"));
}

function storeCertifiedMessage(
  endpoint: HistoryEndpoint,
  predecessor: CertifiedRecord["actionCertifiedRecord"],
  record: CertifiedRecord,
) {
  const { store, local, membership } = endpoint;
  const core = record.actionCertifiedRecord.recordCore;
  return Effect.gen(function* () {
    yield* bindLocalIntent(store, local.card, membership, record);
    yield* store.lockProposal({
      conversationId: membership.descriptor.conversationId,
      previousRecordHash: predecessor.recordHash,
      actionHash: core.actionHash,
      canonicalActionCore: yield* encodeCanonical(ActionCore, core.action),
    });
    yield* store.applyCertifiedRecord(yield* storedRecordRow(record));
  });
}

function messageRecordCore(
  membership: VerifiedMembership,
  predecessor: CertifiedRecord["actionCertifiedRecord"],
  messageIntent: MessageIntent,
) {
  return Effect.gen(function* () {
    const action: MessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "MESSAGE",
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      anchorHash: predecessor.recordCore.anchorHash,
      previousRecordHash: predecessor.recordHash,
      messageIntent,
      messageIntentHash: yield* hashMessageIntent(messageIntent),
    };
    const recordCore: RecordCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "record_core",
      membershipHash: membership.hash,
      anchorHash: predecessor.recordCore.anchorHash,
      action,
      actionHash: yield* hashAction(action),
    };
    return recordCore;
  }).pipe(Effect.orDie);
}

function firstMessageAction(
  author: SigningIdentity,
  membership: VerifiedMembership,
  routerInstanceId: InitialAnchorBody["routerInstanceId"],
) {
  return Effect.gen(function* () {
    const messageIntent: MessageIntent = {
      sharnessVersion: WIRE_VERSION,
      kind: "message_intent",
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      authorAgentId: author.card.agentId,
      msgId: yield* mintMsgId(),
      content: [{ type: "text", text: "certified before restart" }],
    };
    const action: FirstMessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "FIRST_MESSAGE",
      conversationId: membership.descriptor.conversationId,
      membership: membership.descriptor,
      anchor: {
        sharnessVersion: WIRE_VERSION,
        kind: "initial_anchor_body",
        conversationId: membership.descriptor.conversationId,
        membershipHash: membership.hash,
        routerInstanceId,
      },
      previousRecordHash: null,
      messageIntent,
      messageIntentHash: yield* hashMessageIntent(messageIntent),
    };
    return action;
  });
}

function signAction(identity: SigningIdentity, actionHash: ActionHash) {
  return signEvidence(identity, {
    sharnessVersion: WIRE_VERSION,
    kind: "action_signature",
    signerAgentId: identity.card.agentId,
    actionHash,
  });
}

function voteDurable(
  identity: SigningIdentity,
  membership: VerifiedMembership,
  recordHash: RecordHash,
) {
  return signEvidence(identity, {
    sharnessVersion: WIRE_VERSION,
    kind: "durability_vote",
    signerAgentId: identity.card.agentId,
    conversationId: membership.descriptor.conversationId,
    membershipHash: membership.hash,
    recordHash,
  });
}

function certifiedRecord(parts: CertifiedRecordParts): CertifiedRecord {
  return {
    sharnessVersion: WIRE_VERSION,
    kind: "certified_record",
    actionCertifiedRecord: {
      sharnessVersion: WIRE_VERSION,
      kind: "action_certified_record",
      recordHash: parts.recordHash,
      recordCore: parts.recordCore,
      routerAnchor: parts.anchor,
      actionCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_certificate",
        actionHash: parts.recordCore.actionHash,
        signatures: parts.signatures,
      },
    },
    durabilityCertificate: {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_certificate",
      recordHash: parts.recordHash,
      votes: parts.votes,
    },
  };
}

function lockFirstMessage(
  store: TestStore,
  membership: VerifiedMembership,
  record: CertifiedRecord,
) {
  return Effect.gen(function* () {
    const certified = record.actionCertifiedRecord;
    const conversationId = membership.descriptor.conversationId;
    if (certified.routerAnchor.kind !== "initial_anchor_body") {
      return yield* Effect.dieMessage("expected a initially anchored record");
    }
    return yield* store.lockFirstMessageProposal(
      {
        conversationId,
        membershipHash: membership.hash,
        canonicalMembership: yield* encodeCanonical(
          MembershipDescriptor,
          membership.descriptor,
        ),
        anchorHash: certified.recordCore.anchorHash,
        canonicalAnchor: yield* encodeCanonical(
          InitialAnchorBodySchema,
          certified.routerAnchor,
        ),
      },
      {
        conversationId,
        actionHash: certified.recordCore.actionHash,
        canonicalActionCore: yield* encodeCanonical(
          ActionCore,
          certified.recordCore.action,
        ),
      },
    );
  });
}

function storedRecordRow(record: CertifiedRecord) {
  return Effect.gen(function* () {
    const certified = record.actionCertifiedRecord;
    const staged = yield* stagedRecord(certified);
    const row: StoredCertifiedRecord = {
      ...staged,
      actionEvidence: yield* evidenceRows(
        staged.conversationId,
        "action",
        staged.actionHash,
        certified.actionCertificate.signatures,
      ),
      durabilityEvidence: yield* evidenceRows(
        staged.conversationId,
        "durability",
        staged.recordHash,
        record.durabilityCertificate.votes,
      ),
    };
    return row;
  });
}

function evidenceRows(
  conversationId: string,
  kind: ProtocolEvidence["kind"],
  subjectId: string,
  representations: readonly unknown[],
) {
  return Effect.forEach(
    representations,
    (representation) =>
      Schema.decodeUnknown(SignedMessage)(representation).pipe(
        Effect.flatMap((message) =>
          protocolEvidence(conversationId, kind, subjectId, message),
        ),
      ),
    { concurrency: 1 },
  );
}

/** Locally certified messages must first have an immutable intent, as the engine requires. */
function bindLocalIntent(
  store: TestStore,
  local: VerifiedAgentCard,
  membership: VerifiedMembership,
  record: CertifiedRecord,
) {
  return Effect.gen(function* () {
    const intent = record.actionCertifiedRecord.recordCore.action.messageIntent;
    if (intent.authorAgentId !== local.agentId) {
      return;
    }
    const canonicalMessage = yield* encodeCanonical(
      Message,
      yield* messageFromIntent(membership.members.map(agentViewOf), intent),
    );
    yield* store.acceptMessage({
      authorAgentId: intent.authorAgentId,
      msgId: intent.msgId,
      conversationId: intent.conversationId,
      canonicalMessage,
    });
    yield* store.bindMessageIntent({
      kind: "existing-conversation",
      intent: {
        conversationId: membership.descriptor.conversationId,
        membershipHash: membership.hash,
        authorAgentId: intent.authorAgentId,
        msgId: intent.msgId,
        canonicalIntent: yield* encodeCanonical(MessageIntent, intent),
        canonicalMessage,
      },
    });
  });
}
