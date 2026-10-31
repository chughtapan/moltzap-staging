/** @file Client protocol hashes, deterministic evidence, and Router envelopes. */

import { Effect, Encoding, Schema } from "effect";
import { createHash, randomBytes } from "node:crypto";
import type { VerifiedAgentCard } from "../../identity/cards.js";
import type { AgentId } from "../../identity/identifiers.js";
import type { AgentSigningAuthority } from "../../identity/keys.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  representationFailure,
} from "../../encoding/canonical-json.js";
import {
  decodeCanonicalBase64Url,
  isStrictlyAscending,
} from "../../encoding/identifier.js";
import { SealedBody } from "../../identity/envelope/sealed-body.js";
import {
  EnvelopeId,
  type EnvelopeId as EnvelopeIdValue,
  type LiveSignedMessage,
  SignedMessage,
  type SignedMessage as SignedMessageValue,
  type VerifiedSignedMessage,
} from "../../identity/envelope/signed-message.js";
import { WIRE_VERSION } from "../../identity/wire-version.js";
import {
  ActionCore,
  ActionHash,
  type ActionHash as ActionHashValue,
  AnchorBody,
  AnchorHash,
  type AnchorHash as AnchorHashValue,
  ConversationId,
  ConversationIdentityInput,
  type ConversationId as ConversationIdValue,
  DirectPacket,
  type DirectPacket as DirectPacketValue,
  EvidenceStatement,
  type EvidenceStatement as EvidenceStatementValue,
  type InitialAnchorBody,
  MembershipDescriptor,
  type MembershipDescriptor as MembershipDescriptorValue,
  MembershipHash,
  type MembershipHash as MembershipHashValue,
  MessageIntent,
  MessageIntentHash,
  type MessageIntentHash as MessageIntentHashValue,
  type ReanchorBody,
  RecordCore,
  RecordHash,
  type RecordHash as RecordHashValue,
} from "./schemas.js";
import {
  compareAgentIds,
  MsgId,
  type MsgId as MsgIdValue,
  wireProfiles,
} from "./values.js";

/* eslint-disable jsdoc/require-jsdoc -- The package-private representation facade documents this closed protocol vocabulary. */

const HASH_BYTE_LENGTH = 32;
const AGENT_ID_BYTE_LENGTH = 16;
const ENVELOPE_ID_BYTE_LENGTH = 16;

const CLIENT_DOMAIN = wireProfiles[WIRE_VERSION].clientHashDomain;
const EVIDENCE_ENVELOPE_ID_DOMAIN = `${CLIENT_DOMAIN}evidence-envelope-id\0`;
const utf8Encoder = new TextEncoder();

const domainHash = <A, I, R, Hash>(input: {
  readonly artifact: string;
  readonly prefix: string;
  readonly schema: Schema.Schema<A, I, R>;
  readonly value: A;
  readonly output: Schema.Schema<Hash, string>;
}): Effect.Effect<Hash, ClientRepresentationError, R> =>
  encodeCanonical(input.schema, input.value).pipe(
    Effect.map((bytes) =>
      createHash("sha256")
        .update(utf8Encoder.encode(`${CLIENT_DOMAIN}${input.artifact}\0`))
        .update(bytes)
        .digest(),
    ),
    Effect.map(
      (digest) => `${input.prefix}${Encoding.encodeBase64Url(digest)}`,
    ),
    Effect.flatMap(Schema.decodeUnknown(input.output)),
    Effect.mapError(representationFailure),
  );

const decodeAgentIdBytes = (
  agentId: AgentId,
): Effect.Effect<Uint8Array, ClientRepresentationError> => {
  const bytes = decodeCanonicalBase64Url(agentId.slice(4));
  return bytes?.byteLength === AGENT_ID_BYTE_LENGTH
    ? Effect.succeed(bytes)
    : Effect.fail(representationFailure());
};

export const deriveConversationId = (
  memberAgentIds: ConversationIdentityInput["memberAgentIds"],
): Effect.Effect<ConversationIdValue, ClientRepresentationError> => {
  if (!isStrictlyAscending(memberAgentIds, compareAgentIds)) {
    return Effect.fail(representationFailure());
  }
  const input: ConversationIdentityInput = {
    kind: "conversation_identity",
    memberAgentIds,
  };
  return domainHash({
    artifact: "conversation",
    prefix: "cnv_",
    schema: ConversationIdentityInput,
    value: input,
    output: ConversationId,
  });
};

export const hashMembershipDescriptor = (
  membership: MembershipDescriptorValue,
): Effect.Effect<MembershipHashValue, ClientRepresentationError> =>
  domainHash({
    artifact: "membership",
    prefix: "mbr_",
    schema: MembershipDescriptor,
    value: membership,
    output: MembershipHash,
  });

export const mintMsgId = (): Effect.Effect<
  MsgIdValue,
  ClientRepresentationError
> =>
  Effect.try({
    try: () => `msg_${randomBytes(HASH_BYTE_LENGTH).toString("base64url")}`,
    catch: representationFailure,
  }).pipe(
    Effect.flatMap(Schema.decodeUnknown(MsgId)),
    Effect.mapError(representationFailure),
  );

export const hashMessageIntent = (
  intent: MessageIntent,
): Effect.Effect<MessageIntentHashValue, ClientRepresentationError> =>
  domainHash({
    artifact: "message-intent",
    prefix: "mit_",
    schema: MessageIntent,
    value: intent,
    output: MessageIntentHash,
  });

export const hashAnchor = (
  anchor: InitialAnchorBody | ReanchorBody,
): Effect.Effect<AnchorHashValue, ClientRepresentationError> =>
  domainHash({
    artifact: "anchor",
    prefix: "anc_",
    schema: AnchorBody,
    value: anchor,
    output: AnchorHash,
  });

export const hashAction = (
  action: ActionCore,
): Effect.Effect<ActionHashValue, ClientRepresentationError> =>
  domainHash({
    artifact: "action",
    prefix: "ach_",
    schema: ActionCore,
    value: action,
    output: ActionHash,
  });

export const hashRecord = (
  record: RecordCore,
): Effect.Effect<RecordHashValue, ClientRepresentationError> =>
  domainHash({
    artifact: "record",
    prefix: "rch_",
    schema: RecordCore,
    value: record,
    output: RecordHash,
  });

export const deriveEvidenceEnvelopeId = (
  statement: EvidenceStatementValue,
): Effect.Effect<EnvelopeIdValue, ClientRepresentationError> =>
  Effect.gen(function* () {
    const signerBytes = yield* decodeAgentIdBytes(statement.signerAgentId);
    const statementBytes = yield* encodeCanonical(EvidenceStatement, statement);
    const digest = createHash("sha256")
      .update(utf8Encoder.encode(EVIDENCE_ENVELOPE_ID_DOMAIN))
      .update(signerBytes)
      .update(statementBytes)
      .digest()
      .subarray(0, ENVELOPE_ID_BYTE_LENGTH);
    return yield* Schema.decodeUnknown(EnvelopeId)(
      `env_${Encoding.encodeBase64Url(digest)}`,
    ).pipe(Effect.mapError(representationFailure));
  }).pipe(Effect.withSpan("deriveEvidenceEnvelopeId"));

export type DecodedOuterBody =
  | Readonly<{ kind: "direct"; packet: DirectPacketValue }>
  | Readonly<{ kind: "evidence"; message: SignedMessageValue }>;

/** The local agent an outer body opens for, and the authority holding its opening key. */
export interface OuterBodyReader {
  readonly agentCard: VerifiedAgentCard;
  readonly signingAuthority: AgentSigningAuthority;
}

const decodePlaintext = (
  bytes: Uint8Array,
): Effect.Effect<DecodedOuterBody, ClientRepresentationError> =>
  decodeCanonical(DirectPacket, bytes).pipe(
    Effect.map((packet): DecodedOuterBody => ({ kind: "direct", packet })),
    Effect.catchTag("ClientRepresentationError", () =>
      decodeCanonical(SignedMessage, bytes).pipe(
        Effect.map(
          (message): DecodedOuterBody => ({ kind: "evidence", message }),
        ),
      ),
    ),
  );

/**
 * Opens a verified outer message's sealed body for the reader and decodes the
 * Client value inside. Every outer body is sealed, so a plaintext body fails
 * here like one sealed to other recipients, under another sender, or under
 * another EnvelopeId: each refuses the sender's message and says nothing about
 * local state.
 */
export const decodeOuterBody = (
  input: OuterBodyReader & {
    readonly message: LiveSignedMessage & VerifiedSignedMessage;
  },
): Effect.Effect<DecodedOuterBody, ClientRepresentationError> =>
  SealedBody.open({
    agentCard: input.agentCard,
    signingAuthority: input.signingAuthority,
    signedMessage: input.message,
  }).pipe(
    Effect.mapError(representationFailure),
    Effect.flatMap(decodePlaintext),
  );

export const signEvidenceMessage = (input: {
  readonly statement: EvidenceStatementValue;
  readonly agentCard: VerifiedAgentCard;
  readonly signingAuthority: AgentSigningAuthority;
}): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  ClientRepresentationError
> =>
  Effect.gen(function* () {
    if (input.statement.signerAgentId !== input.agentCard.agentId) {
      return yield* representationFailure();
    }
    const envelopeId = yield* deriveEvidenceEnvelopeId(input.statement);
    const body = yield* encodeCanonical(EvidenceStatement, input.statement);
    return yield* SignedMessage.sign({
      agentCard: input.agentCard,
      signingAuthority: input.signingAuthority,
      recipientAgentIds: new Set([input.agentCard.agentId]),
      envelopeId,
      body,
    }).pipe(Effect.mapError(representationFailure));
  }).pipe(Effect.withSpan("signEvidenceMessage"));

export interface OuterMembership {
  readonly members: readonly [
    VerifiedAgentCard,
    VerifiedAgentCard,
    ...VerifiedAgentCard[],
  ];
}

/**
 * Seals a canonical Client value to every member's AgentCard key, bound to the
 * sender and a fresh outer EnvelopeId, then signs the sealed bytes under that
 * EnvelopeId to the same members. The sender is a member, so it can open its
 * own stored envelope.
 */
const signOuterBody = (input: {
  readonly body: Uint8Array;
  readonly membership: OuterMembership;
  readonly agentCard: VerifiedAgentCard;
  readonly signingAuthority: AgentSigningAuthority;
}): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  ClientRepresentationError
> =>
  Effect.gen(function* () {
    const memberAgentIds = input.membership.members.map(
      (member) => member.agentId,
    );
    if (
      !isStrictlyAscending(memberAgentIds, compareAgentIds) ||
      !memberAgentIds.includes(input.agentCard.agentId)
    ) {
      return yield* representationFailure();
    }
    const randomId = yield* Effect.try({
      try: () => randomBytes(ENVELOPE_ID_BYTE_LENGTH),
      catch: representationFailure,
    });
    const envelopeId = yield* Schema.decodeUnknown(EnvelopeId)(
      `env_${Encoding.encodeBase64Url(randomId)}`,
    ).pipe(Effect.mapError(representationFailure));
    const sealed = yield* SealedBody.seal({
      senderAgentId: input.agentCard.agentId,
      recipientAgentCards: input.membership.members,
      envelopeId,
      plaintext: input.body,
    }).pipe(Effect.mapError(representationFailure));
    return yield* SignedMessage.sign({
      agentCard: input.agentCard,
      signingAuthority: input.signingAuthority,
      recipientAgentIds: new Set(memberAgentIds),
      envelopeId,
      body: sealed,
    }).pipe(Effect.mapError(representationFailure));
  }).pipe(Effect.withSpan("signOuterBody"));

export const signOuterPacket = (input: {
  readonly packet: DirectPacketValue;
  readonly membership: OuterMembership;
  readonly agentCard: VerifiedAgentCard;
  readonly signingAuthority: AgentSigningAuthority;
}): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  ClientRepresentationError
> =>
  encodeCanonical(DirectPacket, input.packet).pipe(
    Effect.flatMap((body) => signOuterBody({ ...input, body })),
  );

export const signOuterEvidence = (input: {
  readonly evidence: SignedMessageValue;
  readonly membership: OuterMembership;
  readonly agentCard: VerifiedAgentCard;
  readonly signingAuthority: AgentSigningAuthority;
}): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  ClientRepresentationError
> =>
  encodeCanonical(SignedMessage, input.evidence).pipe(
    Effect.flatMap((body) => signOuterBody({ ...input, body })),
  );

/* eslint-enable jsdoc/require-jsdoc -- Restore package documentation rules. */
