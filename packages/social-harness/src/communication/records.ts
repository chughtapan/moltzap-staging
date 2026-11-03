/** @file Conversation records own their signed encoding, membership and certificate verification. */

import { Effect, Encoding, Schema } from "effect";
import { createHash, randomBytes } from "node:crypto";
import type {
  AgentPublicKey,
  AgentSigningAuthority,
} from "../identity/keys.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  equalCanonical,
  exactParseOptions,
  exactStruct,
  representationFailure,
  sameBytes,
} from "../encoding/canonical-json.js";
import {
  decodeCanonicalBase64Url,
  isCanonicalIdentifier,
  isStrictlyAscending,
} from "../encoding/identifier.js";
import {
  AgentId,
  AgentIdentityCard,
  type VerifiedAgentIdentityCard,
} from "../identity/agent-cards.js";
import {
  EnvelopeId,
  type EnvelopeId as EnvelopeIdValue,
  type LiveSignedMessage,
  SealedBody,
  SignedMessage,
  type SignedMessage as SignedMessageValue,
  type VerifiedSignedMessage,
} from "../identity/message-security.js";
import { wireVersions } from "../version.js";
import { compareAgentIds, Content, maximumMembers, MsgId } from "./messages.js";
import { RouterInstanceId } from "./router/client.js";

/* eslint-disable jsdoc/require-jsdoc -- These package-private Schema names are the exact closed protocol vocabulary. */

const versionAndKind = <const Kind extends string>(kind: Kind) => ({
  sharnessVersion: wireVersions.history,
  kind: Schema.Literal(kind),
});

const canonicalIdentifier = <const Name extends string>(
  name: Name,
  prefix: string,
) =>
  Schema.String.pipe(
    Schema.filter((value) => isCanonicalIdentifier(prefix, value), {
      identifier: name,
      description: `${name} canonical representation`,
    }),
    Schema.brand(name),
    Schema.annotations({
      identifier: name,
      description: `${name} canonical representation`,
    }),
  );

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Private Effect Schemas share their domain names with the values they decode. */

export const ConversationId = canonicalIdentifier("ConversationId", "cnv_");
export type ConversationId = typeof ConversationId.Type;
export const MembershipHash = canonicalIdentifier("MembershipHash", "mbr_");
export type MembershipHash = typeof MembershipHash.Type;
export const MessageIntentHash = canonicalIdentifier(
  "MessageIntentHash",
  "mit_",
);
export type MessageIntentHash = typeof MessageIntentHash.Type;
export const AnchorHash = canonicalIdentifier("AnchorHash", "anc_");
export type AnchorHash = typeof AnchorHash.Type;
export const ActionHash = canonicalIdentifier("ActionHash", "ach_");
export type ActionHash = typeof ActionHash.Type;
export const RecordHash = canonicalIdentifier("RecordHash", "rch_");
export type RecordHash = typeof RecordHash.Type;

const encodedAgentCard = Schema.encodedSchema(AgentIdentityCard);
const encodedSignedMessage = Schema.encodedSchema(SignedMessage);
const MemberAgentIds = Schema.Tuple([AgentId, AgentId], AgentId).pipe(
  Schema.maxItems(maximumMembers),
);
const MembershipMembers = Schema.Tuple(
  [encodedAgentCard, encodedAgentCard],
  encodedAgentCard,
).pipe(Schema.maxItems(maximumMembers));
const EvidenceMessages = Schema.NonEmptyArray(encodedSignedMessage).pipe(
  Schema.maxItems(maximumMembers),
);

export const ConversationIdentityInput = exactStruct({
  kind: Schema.Literal("conversation_identity"),
  memberAgentIds: MemberAgentIds,
});
export type ConversationIdentityInput = typeof ConversationIdentityInput.Type;

export const MembershipDescriptor = exactStruct({
  ...versionAndKind("membership_descriptor"),
  conversationId: ConversationId,
  members: MembershipMembers,
});
export type MembershipDescriptor = typeof MembershipDescriptor.Type;

export const MessageIntent = exactStruct({
  ...versionAndKind("message_intent"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  authorAgentId: AgentId,
  msgId: MsgId,
  content: Content,
});
export type MessageIntent = typeof MessageIntent.Type;

export const InitialAnchorBody = exactStruct({
  ...versionAndKind("initial_anchor_body"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  routerInstanceId: RouterInstanceId,
});
export type InitialAnchorBody = typeof InitialAnchorBody.Type;

export const ReanchorBody = exactStruct({
  ...versionAndKind("reanchor_body"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  previousAnchorHash: AnchorHash,
  selectedRecordHash: RecordHash,
  routerInstanceId: RouterInstanceId,
});
export type ReanchorBody = typeof ReanchorBody.Type;

const AnchorBody = Schema.Union(InitialAnchorBody, ReanchorBody);
type AnchorBody = typeof AnchorBody.Type;

export const FirstMessageActionCore = exactStruct({
  ...versionAndKind("FIRST_MESSAGE"),
  conversationId: ConversationId,
  membership: MembershipDescriptor,
  anchor: InitialAnchorBody,
  previousRecordHash: Schema.Null,
  messageIntent: MessageIntent,
  messageIntentHash: MessageIntentHash,
});
export type FirstMessageActionCore = typeof FirstMessageActionCore.Type;

export const MessageActionCore = exactStruct({
  ...versionAndKind("MESSAGE"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  anchorHash: AnchorHash,
  previousRecordHash: RecordHash,
  messageIntent: MessageIntent,
  messageIntentHash: MessageIntentHash,
});
export type MessageActionCore = typeof MessageActionCore.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private protocol code composes the complete closed action union.
export const ActionCore = Schema.Union(
  FirstMessageActionCore,
  MessageActionCore,
);
export type ActionCore = typeof ActionCore.Type;

const FirstMessageRecordCore = exactStruct({
  ...versionAndKind("record_core"),
  membership: MembershipDescriptor,
  anchorHash: AnchorHash,
  action: FirstMessageActionCore,
  actionHash: ActionHash,
});
const MessageRecordCore = exactStruct({
  ...versionAndKind("record_core"),
  membershipHash: MembershipHash,
  anchorHash: AnchorHash,
  action: MessageActionCore,
  actionHash: ActionHash,
});
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private record decoding and hashing compose the complete closed record-core union.
export const RecordCore = Schema.Union(
  FirstMessageRecordCore,
  MessageRecordCore,
);
export type RecordCore = typeof RecordCore.Type;

export const ActionSignatureStatement = exactStruct({
  ...versionAndKind("action_signature"),
  signerAgentId: AgentId,
  actionHash: ActionHash,
});
export type ActionSignatureStatement = typeof ActionSignatureStatement.Type;

const DurabilityVoteStatement = exactStruct({
  ...versionAndKind("durability_vote"),
  signerAgentId: AgentId,
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  recordHash: RecordHash,
});

export const ReanchorVoteStatement = exactStruct({
  ...versionAndKind("reanchor_vote"),
  signerAgentId: AgentId,
  anchorHash: AnchorHash,
  reanchor: ReanchorBody,
});
export type ReanchorVoteStatement = typeof ReanchorVoteStatement.Type;

const ActionCertificate = exactStruct({
  ...versionAndKind("action_certificate"),
  actionHash: ActionHash,
  signatures: EvidenceMessages,
});

const DurabilityCertificate = exactStruct({
  ...versionAndKind("durability_certificate"),
  recordHash: RecordHash,
  votes: EvidenceMessages,
});

const ReanchorCertificate = exactStruct({
  ...versionAndKind("reanchor_certificate"),
  anchorHash: AnchorHash,
  votes: EvidenceMessages,
});

export const CompletedReanchor = exactStruct({
  ...versionAndKind("completed_reanchor"),
  anchorHash: AnchorHash,
  reanchor: ReanchorBody,
  certificate: ReanchorCertificate,
});
export type CompletedReanchor = typeof CompletedReanchor.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private record verification composes the complete closed anchor union.
export const RouterAnchor = Schema.Union(InitialAnchorBody, CompletedReanchor);
export type RouterAnchor = typeof RouterAnchor.Type;

export const ActionCertifiedRecord = exactStruct({
  ...versionAndKind("action_certified_record"),
  recordHash: RecordHash,
  recordCore: RecordCore,
  routerAnchor: RouterAnchor,
  actionCertificate: ActionCertificate,
});
export type ActionCertifiedRecord = typeof ActionCertifiedRecord.Type;

export const CertifiedRecord = exactStruct({
  ...versionAndKind("certified_record"),
  actionCertifiedRecord: ActionCertifiedRecord,
  durabilityCertificate: DurabilityCertificate,
});
export type CertifiedRecord = typeof CertifiedRecord.Type;

export const CatchUpRequest = exactStruct({
  ...versionAndKind("catch_up_request"),
  conversationId: ConversationId,
  membershipHash: MembershipHash,
  requesterAgentId: AgentId,
  knownRecordHash: Schema.NullOr(RecordHash),
  knownAnchorHash: Schema.NullOr(AnchorHash),
}).pipe(
  Schema.filter(
    (request) =>
      (request.knownRecordHash === null) === (request.knownAnchorHash === null),
  ),
);
export type CatchUpRequest = typeof CatchUpRequest.Type;

const CatchUpItem = Schema.Union(CertifiedRecord, CompletedReanchor);

const CatchUpAttestationStatement = exactStruct({
  ...versionAndKind("catch_up_attestation"),
  signerAgentId: AgentId,
  request: CatchUpRequest,
  itemKind: Schema.Literal(
    "certified_record",
    "completed_reanchor",
    "incomplete",
  ),
  itemHash: Schema.NullOr(Schema.Union(RecordHash, AnchorHash)),
  hasMore: Schema.Boolean,
});
type CatchUpAttestationStatement = typeof CatchUpAttestationStatement.Type;

export const CatchUpPage = exactStruct({
  ...versionAndKind("catch_up_page"),
  request: CatchUpRequest,
  item: CatchUpItem,
  hasMore: Schema.Boolean,
  attestation: encodedSignedMessage,
});
export type CatchUpPage = typeof CatchUpPage.Type;

export const CatchUpIncomplete = exactStruct({
  ...versionAndKind("catch_up_incomplete"),
  request: CatchUpRequest,
  attestation: encodedSignedMessage,
});
export type CatchUpIncomplete = typeof CatchUpIncomplete.Type;

export const ActionProposal = exactStruct({
  ...versionAndKind("action_proposal"),
  action: ActionCore,
});
export type ActionProposal = typeof ActionProposal.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private signing and verification compose the complete closed evidence union.
export const EvidenceStatement = Schema.Union(
  ActionSignatureStatement,
  DurabilityVoteStatement,
  ReanchorVoteStatement,
  CatchUpAttestationStatement,
);
export type EvidenceStatement = typeof EvidenceStatement.Type;

// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private Router-envelope codecs compose the complete closed packet union.
export const DirectPacket = Schema.Union(
  ActionProposal,
  ActionCertifiedRecord,
  CertifiedRecord,
  CompletedReanchor,
  CatchUpRequest,
  CatchUpPage,
  CatchUpIncomplete,
);
export type DirectPacket = typeof DirectPacket.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules. */

/* eslint-enable jsdoc/require-jsdoc -- Restore package documentation rules. */

/* eslint-disable jsdoc/require-jsdoc -- The package-private representation facade documents this closed protocol vocabulary. */

const AGENT_ID_BYTE_LENGTH = 16;
const ENVELOPE_ID_BYTE_LENGTH = 16;

const CLIENT_DOMAIN = "org.social-harness/client/v2/";
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
): Effect.Effect<ConversationId, ClientRepresentationError> => {
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
  membership: MembershipDescriptor,
): Effect.Effect<MembershipHash, ClientRepresentationError> =>
  domainHash({
    artifact: "membership",
    prefix: "mbr_",
    schema: MembershipDescriptor,
    value: membership,
    output: MembershipHash,
  });

export const hashMessageIntent = (
  intent: MessageIntent,
): Effect.Effect<MessageIntentHash, ClientRepresentationError> =>
  domainHash({
    artifact: "message-intent",
    prefix: "mit_",
    schema: MessageIntent,
    value: intent,
    output: MessageIntentHash,
  });

export const hashAnchor = (
  anchor: InitialAnchorBody | ReanchorBody,
): Effect.Effect<AnchorHash, ClientRepresentationError> =>
  domainHash({
    artifact: "anchor",
    prefix: "anc_",
    schema: AnchorBody,
    value: anchor,
    output: AnchorHash,
  });

export const hashAction = (
  action: ActionCore,
): Effect.Effect<ActionHash, ClientRepresentationError> =>
  domainHash({
    artifact: "action",
    prefix: "ach_",
    schema: ActionCore,
    value: action,
    output: ActionHash,
  });

export const hashRecord = (
  record: RecordCore,
): Effect.Effect<RecordHash, ClientRepresentationError> =>
  domainHash({
    artifact: "record",
    prefix: "rch_",
    schema: RecordCore,
    value: record,
    output: RecordHash,
  });

export const deriveEvidenceEnvelopeId = (
  statement: EvidenceStatement,
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
  | Readonly<{ kind: "direct"; packet: DirectPacket }>
  | Readonly<{ kind: "evidence"; message: SignedMessageValue }>;

/** The local agent an outer body opens for, and the authority holding its opening key. */
export interface OuterBodyReader {
  readonly identityCard: VerifiedAgentIdentityCard;
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
 * Opens and decodes a verified outer message. Invalid or plaintext bodies
 * cause refusal of peer input without indicating broken local state.
 */
export const decodeOuterBody = (
  input: OuterBodyReader & {
    readonly message: LiveSignedMessage & VerifiedSignedMessage;
  },
): Effect.Effect<DecodedOuterBody, ClientRepresentationError> =>
  SealedBody.open({
    identityCard: input.identityCard,
    signingAuthority: input.signingAuthority,
    signedMessage: input.message,
  }).pipe(
    Effect.mapError(representationFailure),
    Effect.flatMap(decodePlaintext),
  );

export const signEvidenceMessage = (input: {
  readonly statement: EvidenceStatement;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly signingAuthority: AgentSigningAuthority;
}): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  ClientRepresentationError
> =>
  Effect.gen(function* () {
    if (input.statement.signerAgentId !== input.identityCard.agentId) {
      return yield* representationFailure();
    }
    const envelopeId = yield* deriveEvidenceEnvelopeId(input.statement);
    const body = yield* encodeCanonical(EvidenceStatement, input.statement);
    return yield* SignedMessage.sign({
      identityCard: input.identityCard,
      signingAuthority: input.signingAuthority,
      recipientAgentIds: new Set([input.identityCard.agentId]),
      envelopeId,
      body,
    }).pipe(Effect.mapError(representationFailure));
  }).pipe(Effect.withSpan("signEvidenceMessage"));

export interface OuterMembership {
  readonly members: readonly [
    VerifiedAgentIdentityCard,
    VerifiedAgentIdentityCard,
    ...VerifiedAgentIdentityCard[],
  ];
}

/**
 * Seals to all members, including the sender, before signing with the same
 * sender, recipients and fresh EnvelopeId.
 */
const signOuterBody = (input: {
  readonly body: Uint8Array;
  readonly membership: OuterMembership;
  readonly identityCard: VerifiedAgentIdentityCard;
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
      !memberAgentIds.includes(input.identityCard.agentId)
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
      senderAgentId: input.identityCard.agentId,
      recipientIdentityCards: input.membership.members,
      envelopeId,
      plaintext: input.body,
    }).pipe(Effect.mapError(representationFailure));
    return yield* SignedMessage.sign({
      identityCard: input.identityCard,
      signingAuthority: input.signingAuthority,
      recipientAgentIds: new Set(memberAgentIds),
      envelopeId,
      body: sealed,
    }).pipe(Effect.mapError(representationFailure));
  }).pipe(Effect.withSpan("signOuterBody"));

export const signOuterPacket = (input: {
  readonly packet: DirectPacket;
  readonly membership: OuterMembership;
  readonly identityCard: VerifiedAgentIdentityCard;
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
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly signingAuthority: AgentSigningAuthority;
}): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  ClientRepresentationError
> =>
  encodeCanonical(SignedMessage, input.evidence).pipe(
    Effect.flatMap((body) => signOuterBody({ ...input, body })),
  );

/* eslint-enable jsdoc/require-jsdoc -- Restore package documentation rules. */

/* eslint-disable jsdoc/require-jsdoc -- The package-private representation facade documents these closed verification operations. */

export interface VerifiedMembership extends OuterMembership {
  readonly descriptor: MembershipDescriptor;
  readonly hash: MembershipHash;
}

export const verifyMembershipDescriptor = (
  descriptor: MembershipDescriptor,
  registrySignerPublicKey: AgentPublicKey,
): Effect.Effect<VerifiedMembership, ClientRepresentationError> =>
  Effect.gen(function* () {
    const verified = yield* Effect.forEach(
      descriptor.members,
      (representation) =>
        Schema.decodeUnknown(AgentIdentityCard)(
          representation,
          exactParseOptions,
        ).pipe(
          Effect.flatMap((agentCard) =>
            AgentIdentityCard.verify({
              identityCard: agentCard,
              registrySignerPublicKey,
            }),
          ),
          Effect.mapError(representationFailure),
        ),
      { concurrency: "inherit" },
    );
    const agentIds = verified.map((card) => card.agentId);
    const agentNames = new Set(verified.map((card) => card.agentName));
    const first = verified[0];
    const second = verified[1];
    if (
      first === undefined ||
      second === undefined ||
      !isStrictlyAscending(agentIds, compareAgentIds) ||
      agentNames.size !== verified.length
    ) {
      return yield* representationFailure();
    }
    const memberAgentIds: readonly [AgentId, AgentId, ...AgentId[]] = [
      first.agentId,
      second.agentId,
      ...verified.slice(2).map((card) => card.agentId),
    ];
    const conversationId = yield* deriveConversationId(memberAgentIds);
    if (conversationId !== descriptor.conversationId) {
      return yield* representationFailure();
    }
    const members: OuterMembership["members"] = [
      first,
      second,
      ...verified.slice(2),
    ];
    return {
      descriptor,
      hash: yield* hashMembershipDescriptor(descriptor),
      members,
    };
  }).pipe(Effect.withSpan("verifyMembershipDescriptor"));

export const memberCard = (
  membership: VerifiedMembership,
  agentId: AgentId,
): VerifiedAgentIdentityCard | undefined =>
  membership.members.find((card) => card.agentId === agentId);

export const verifyOuterMessage = (input: {
  readonly message: LiveSignedMessage;
  readonly membership: VerifiedMembership;
}): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  ClientRepresentationError
> =>
  Effect.gen(function* () {
    const card = memberCard(input.membership, input.message.senderAgentId);
    const expected = input.membership.members.map((member) => member.agentId);
    if (
      card === undefined ||
      input.message.recipientAgentIds.length !== expected.length ||
      input.message.recipientAgentIds.some(
        (recipient, index) => recipient !== expected[index],
      )
    ) {
      return yield* representationFailure();
    }
    const verified = yield* SignedMessage.verify({
      signedMessage: input.message,
      identityCard: card,
    }).pipe(Effect.mapError(representationFailure));
    return verified;
  }).pipe(Effect.withSpan("verifyOuterMessage"));

export interface VerifiedEvidence {
  readonly message: VerifiedSignedMessage;
  readonly statement: EvidenceStatement;
}

/**
 * Checks self-addressed evidence identity and signature. Reuses the statement
 * decoded at ingress and decodes stored evidence here.
 */
export const verifyStableEvidence = (input: {
  readonly representation: unknown;
  readonly membership: VerifiedMembership;
  readonly statement?: EvidenceStatement;
}): Effect.Effect<VerifiedEvidence, ClientRepresentationError> =>
  Effect.gen(function* () {
    const message = yield* Schema.decodeUnknown(SignedMessage)(
      input.representation,
      exactParseOptions,
    ).pipe(Effect.mapError(representationFailure));
    const card = memberCard(input.membership, message.senderAgentId);
    if (
      card === undefined ||
      message.recipientAgentIds.length !== 1 ||
      message.recipientAgentIds[0] !== message.senderAgentId
    ) {
      return yield* representationFailure();
    }
    const verifiedMessage = yield* SignedMessage.verify({
      signedMessage: message,
      identityCard: card,
    }).pipe(Effect.mapError(representationFailure));
    const statement =
      input.statement ??
      (yield* decodeCanonical(EvidenceStatement, message.body));
    if (
      !sameBytes(
        yield* encodeCanonical(EvidenceStatement, statement),
        message.body,
      )
    ) {
      return yield* representationFailure();
    }
    const expectedEnvelopeId = yield* deriveEvidenceEnvelopeId(statement);
    if (
      statement.signerAgentId !== message.senderAgentId ||
      expectedEnvelopeId !== message.envelopeId
    ) {
      return yield* representationFailure();
    }
    return { message: verifiedMessage, statement };
  }).pipe(Effect.withSpan("verifyStableEvidence"));

/**
 * Requires the outer envelope and its inner evidence to verify under the
 * same membership. The envelope must name every member.
 */
export const verifyDeliveredEvidence = (input: {
  readonly outer: LiveSignedMessage;
  readonly evidence: SignedMessageValue;
  readonly membership: VerifiedMembership;
  readonly statement?: EvidenceStatement;
}): Effect.Effect<VerifiedEvidence, ClientRepresentationError> =>
  verifyOuterMessage({
    message: input.outer,
    membership: input.membership,
  }).pipe(
    Effect.zipRight(
      Schema.encode(SignedMessage)(input.evidence).pipe(
        Effect.catchTag("ParseError", () => representationFailure()),
      ),
    ),
    Effect.flatMap((representation) =>
      verifyStableEvidence({
        representation,
        membership: input.membership,
        ...(input.statement === undefined
          ? {}
          : { statement: input.statement }),
      }),
    ),
    Effect.withSpan("verifyDeliveredEvidence"),
  );

type EvidenceKind = EvidenceStatement["kind"];

const verifyEvidenceSet = <Kind extends EvidenceKind>(input: {
  readonly representations: readonly unknown[];
  readonly expectedKind: Kind;
  readonly membership: VerifiedMembership;
  /**
   * Requires at least this many distinct member signers with one signature per
   * member. A requirement for every member is exact.
   */
  readonly requiredCount: number;
  readonly statementMatches: (
    statement: Extract<EvidenceStatement, { readonly kind: Kind }>,
  ) => Effect.Effect<boolean, ClientRepresentationError>;
}): Effect.Effect<readonly VerifiedEvidence[], ClientRepresentationError> =>
  Effect.gen(function* () {
    const length = input.representations.length;
    if (length > input.membership.members.length) {
      return yield* representationFailure();
    }
    if (length < input.requiredCount) {
      return yield* representationFailure();
    }
    const verified = yield* Effect.forEach(
      input.representations,
      (representation) =>
        verifyStableEvidence({ representation, membership: input.membership }),
      { concurrency: "inherit" },
    );
    const signers = verified.map(({ statement }) => statement.signerAgentId);
    if (!isStrictlyAscending(signers, compareAgentIds)) {
      return yield* representationFailure();
    }
    const matchesKind = (
      statement: EvidenceStatement,
    ): statement is Extract<EvidenceStatement, { readonly kind: Kind }> =>
      statement.kind === input.expectedKind;
    const matches = yield* Effect.forEach(
      verified,
      ({ statement }) =>
        matchesKind(statement)
          ? input.statementMatches(statement)
          : Effect.succeed(false),
      { concurrency: "inherit" },
    );
    if (matches.some((matchesStatement) => !matchesStatement)) {
      return yield* representationFailure();
    }
    return verified;
  });

export const quorumThreshold = (memberCount: number): number =>
  memberCount < 4
    ? memberCount
    : memberCount - Math.floor((memberCount - 1) / 3);

export interface VerifiedActionCore {
  readonly actionHash: ActionHash;
  readonly anchorHash: AnchorHash;
}

const actionReferencesMembership = (input: {
  readonly action: ActionCore;
  readonly membership: VerifiedMembership;
}): boolean => {
  const intent = input.action.messageIntent;
  if (
    input.action.conversationId !==
      input.membership.descriptor.conversationId ||
    intent.conversationId !== input.membership.descriptor.conversationId
  ) {
    return false;
  }
  if (
    intent.membershipHash !== input.membership.hash ||
    memberCard(input.membership, intent.authorAgentId) === undefined
  ) {
    return false;
  }
  return true;
};

export const verifyActionCore = (input: {
  readonly action: ActionCore;
  readonly membership: VerifiedMembership;
}): Effect.Effect<VerifiedActionCore, ClientRepresentationError> =>
  Effect.gen(function* () {
    const action = input.action;
    const intent = action.messageIntent;
    if (!actionReferencesMembership(input)) {
      return yield* representationFailure();
    }
    if ((yield* hashMessageIntent(intent)) !== action.messageIntentHash) {
      return yield* representationFailure();
    }
    if (action.kind === "FIRST_MESSAGE") {
      if (
        !(yield* equalCanonical(
          MembershipDescriptor,
          action.membership,
          input.membership.descriptor,
        )) ||
        action.anchor.conversationId !== action.conversationId ||
        action.anchor.membershipHash !== input.membership.hash
      ) {
        return yield* representationFailure();
      }
      return {
        actionHash: yield* hashAction(action),
        anchorHash: yield* hashAnchor(action.anchor),
      };
    }
    if (action.membershipHash !== input.membership.hash) {
      return yield* representationFailure();
    }
    return {
      actionHash: yield* hashAction(action),
      anchorHash: action.anchorHash,
    };
  }).pipe(Effect.withSpan("verifyActionCore"));

export const verifyActionProposal = (input: {
  readonly proposal: ActionProposal;
  readonly membership: VerifiedMembership;
  readonly outerSenderAgentId: AgentId;
}): Effect.Effect<VerifiedActionCore, ClientRepresentationError> =>
  Effect.gen(function* () {
    const verifiedAction = yield* verifyActionCore({
      action: input.proposal.action,
      membership: input.membership,
    });
    const authorAgentId = input.proposal.action.messageIntent.authorAgentId;
    if (input.outerSenderAgentId !== authorAgentId) {
      return yield* representationFailure();
    }
    return verifiedAction;
  }).pipe(Effect.withSpan("verifyActionProposal"));

/**
 * The caller has already verified `actionHash` against `action`.
 */
const verifyActionCertificateEvidence = (input: {
  readonly actionHash: ActionHash;
  readonly certificate: ActionCertifiedRecord["actionCertificate"];
  readonly action: ActionCore;
  readonly membership: VerifiedMembership;
}): Effect.Effect<ActionHash, ClientRepresentationError> =>
  Effect.gen(function* () {
    if (input.certificate.actionHash !== input.actionHash) {
      return yield* representationFailure();
    }
    const requiredCount =
      input.action.kind === "FIRST_MESSAGE"
        ? input.membership.members.length
        : quorumThreshold(input.membership.members.length);
    const evidence = yield* verifyEvidenceSet({
      representations: input.certificate.signatures,
      expectedKind: "action_signature",
      membership: input.membership,
      requiredCount,
      statementMatches: (statement) =>
        Effect.succeed(statement.actionHash === input.actionHash),
    });
    const authorAgentId = input.action.messageIntent.authorAgentId;
    if (
      !evidence.some(
        ({ statement }) => statement.signerAgentId === authorAgentId,
      )
    ) {
      return yield* representationFailure();
    }
    return input.actionHash;
  }).pipe(Effect.withSpan("verifyActionCertificate"));

export const verifyCompletedReanchor = (input: {
  readonly completed: CompletedReanchor;
  readonly membership: VerifiedMembership;
}): Effect.Effect<AnchorHash, ClientRepresentationError> =>
  Effect.gen(function* () {
    const expectedAnchorHash = yield* hashAnchor(input.completed.reanchor);
    if (
      expectedAnchorHash !== input.completed.anchorHash ||
      input.completed.certificate.anchorHash !== expectedAnchorHash ||
      input.completed.reanchor.membershipHash !== input.membership.hash ||
      input.completed.reanchor.conversationId !==
        input.membership.descriptor.conversationId
    ) {
      return yield* representationFailure();
    }
    yield* verifyEvidenceSet({
      representations: input.completed.certificate.votes,
      expectedKind: "reanchor_vote",
      membership: input.membership,
      requiredCount: quorumThreshold(input.membership.members.length),
      statementMatches: (statement) =>
        equalCanonical(
          ReanchorBody,
          statement.reanchor,
          input.completed.reanchor,
        ).pipe(
          Effect.map(
            (sameReanchor) =>
              sameReanchor && statement.anchorHash === expectedAnchorHash,
          ),
        ),
    });
    return expectedAnchorHash;
  }).pipe(Effect.withSpan("verifyCompletedReanchor"));

/**
 * FIRST_MESSAGE must carry the exact held descriptor and MESSAGE must name
 * its hash. The held membership must already be verified against the Registry.
 */
const verifyRecordMembership = (
  recordCore: RecordCore,
  membership: VerifiedMembership,
): Effect.Effect<void, ClientRepresentationError> =>
  ("membership" in recordCore
    ? equalCanonical(
        MembershipDescriptor,
        recordCore.membership,
        membership.descriptor,
      )
    : Effect.succeed(recordCore.membershipHash === membership.hash)
  ).pipe(
    Effect.filterOrFail((matches) => matches, representationFailure),
    Effect.asVoid,
  );

export const verifyRecordCore = (input: {
  readonly recordCore: RecordCore;
  readonly membership: VerifiedMembership;
}): Effect.Effect<RecordHash, ClientRepresentationError> =>
  Effect.gen(function* () {
    const { membership } = input;
    yield* verifyRecordMembership(input.recordCore, membership);
    const verifiedAction = yield* verifyActionCore({
      action: input.recordCore.action,
      membership,
    });
    if (
      input.recordCore.actionHash !== verifiedAction.actionHash ||
      input.recordCore.anchorHash !== verifiedAction.anchorHash
    ) {
      return yield* representationFailure();
    }
    return yield* hashRecord(input.recordCore);
  }).pipe(Effect.withSpan("verifyRecordCore"));

const verifyRecordRouterAnchor = (input: {
  readonly record: ActionCertifiedRecord;
  readonly membership: VerifiedMembership;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    const routerAnchor = input.record.routerAnchor;
    const anchorHash =
      routerAnchor.kind === "initial_anchor_body"
        ? yield* hashAnchor(routerAnchor)
        : yield* verifyCompletedReanchor({
            completed: routerAnchor,
            membership: input.membership,
          });
    if (anchorHash !== input.record.recordCore.anchorHash) {
      return yield* representationFailure();
    }
    if (routerAnchor.kind !== "initial_anchor_body") {
      return;
    }
    if (
      routerAnchor.conversationId !==
        input.membership.descriptor.conversationId ||
      routerAnchor.membershipHash !== input.membership.hash
    ) {
      return yield* representationFailure();
    }
  });

const verifyFirstMessageRouterAnchor = (input: {
  readonly record: ActionCertifiedRecord;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    const action = input.record.recordCore.action;
    if (action.kind !== "FIRST_MESSAGE") {
      return;
    }
    const routerAnchor = input.record.routerAnchor;
    if (routerAnchor.kind !== "initial_anchor_body") {
      return yield* representationFailure();
    }
    if (
      !(yield* equalCanonical(InitialAnchorBody, routerAnchor, action.anchor))
    ) {
      return yield* representationFailure();
    }
  });

export const verifyActionCertifiedRecord = (input: {
  readonly record: ActionCertifiedRecord;
  readonly membership: VerifiedMembership;
}): Effect.Effect<RecordHash, ClientRepresentationError> =>
  Effect.gen(function* () {
    const recordHash = yield* verifyRecordCore({
      recordCore: input.record.recordCore,
      membership: input.membership,
    });
    if (input.record.recordHash !== recordHash) {
      return yield* representationFailure();
    }
    yield* verifyActionCertificateEvidence({
      certificate: input.record.actionCertificate,
      action: input.record.recordCore.action,
      membership: input.membership,
      actionHash: input.record.recordCore.actionHash,
    });
    yield* verifyRecordRouterAnchor({
      record: input.record,
      membership: input.membership,
    });
    yield* verifyFirstMessageRouterAnchor({ record: input.record });
    return recordHash;
  }).pipe(Effect.withSpan("verifyActionCertifiedRecord"));

export const verifyCertifiedRecord = (input: {
  readonly record: CertifiedRecord;
  readonly membership: VerifiedMembership;
}): Effect.Effect<readonly VerifiedEvidence[], ClientRepresentationError> =>
  Effect.gen(function* () {
    const { membership } = input;
    const recordHash = yield* verifyActionCertifiedRecord({
      record: input.record.actionCertifiedRecord,
      membership,
    });
    const certificate = input.record.durabilityCertificate;
    if (certificate.recordHash !== recordHash) {
      return yield* representationFailure();
    }
    return yield* verifyEvidenceSet({
      representations: certificate.votes,
      expectedKind: "durability_vote",
      membership,
      requiredCount: quorumThreshold(membership.members.length),
      statementMatches: (statement) =>
        Effect.succeed(
          statement.conversationId === membership.descriptor.conversationId &&
            statement.membershipHash === membership.hash &&
            statement.recordHash === recordHash,
        ),
    });
  }).pipe(Effect.withSpan("verifyCertifiedRecord"));

const verifyCatchUpRequest = (input: {
  readonly request: CatchUpRequest;
  readonly membership: VerifiedMembership;
}): Effect.Effect<void, ClientRepresentationError> =>
  input.request.conversationId === input.membership.descriptor.conversationId &&
  input.request.membershipHash === input.membership.hash &&
  memberCard(input.membership, input.request.requesterAgentId) !== undefined
    ? Effect.void
    : Effect.fail(representationFailure());

const catchUpAttestationMatches = (input: {
  readonly statement: CatchUpAttestationStatement;
  readonly request: CatchUpRequest;
  readonly itemKind: CatchUpAttestationStatement["itemKind"];
  readonly itemHash: RecordHash | AnchorHash | null;
  readonly hasMore: boolean;
}): Effect.Effect<boolean, ClientRepresentationError> =>
  equalCanonical(CatchUpRequest, input.statement.request, input.request).pipe(
    Effect.map(
      (sameRequest) =>
        sameRequest &&
        input.statement.itemKind === input.itemKind &&
        input.statement.itemHash === input.itemHash &&
        input.statement.hasMore === input.hasMore,
    ),
  );

const verifyCatchUpAttestation = (input: {
  readonly representation: unknown;
  readonly membership: VerifiedMembership;
  readonly responseSenderAgentId: AgentId;
  readonly request: CatchUpRequest;
  readonly itemKind: CatchUpAttestationStatement["itemKind"];
  readonly itemHash: RecordHash | AnchorHash | null;
  readonly hasMore: boolean;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    const evidence = yield* verifyStableEvidence(input);
    if (
      evidence.statement.kind !== "catch_up_attestation" ||
      evidence.statement.signerAgentId !== input.responseSenderAgentId ||
      !(yield* catchUpAttestationMatches({
        statement: evidence.statement,
        request: input.request,
        itemKind: input.itemKind,
        itemHash: input.itemHash,
        hasMore: input.hasMore,
      }))
    ) {
      return yield* representationFailure();
    }
  });

interface VerifiedCatchUpItem {
  readonly itemKind: CatchUpAttestationStatement["itemKind"];
  readonly itemHash: RecordHash | AnchorHash;
}

const certifiedItemExtendsPosition = (input: {
  readonly request: CatchUpRequest;
  readonly record: CertifiedRecord;
}): boolean => {
  const action = input.record.actionCertifiedRecord.recordCore.action;
  if (input.request.knownRecordHash === null) {
    return (
      action.kind === "FIRST_MESSAGE" &&
      input.record.actionCertifiedRecord.routerAnchor.kind ===
        "initial_anchor_body"
    );
  }
  return (
    action.kind === "MESSAGE" &&
    action.previousRecordHash === input.request.knownRecordHash &&
    action.anchorHash === input.request.knownAnchorHash
  );
};

const verifyCertifiedCatchUpItem = (input: {
  readonly page: CatchUpPage;
  readonly item: CertifiedRecord;
  readonly membership: VerifiedMembership;
}): Effect.Effect<VerifiedCatchUpItem, ClientRepresentationError> =>
  Effect.gen(function* () {
    if (
      !certifiedItemExtendsPosition({
        request: input.page.request,
        record: input.item,
      })
    ) {
      return yield* representationFailure();
    }
    yield* verifyCertifiedRecord({
      record: input.item,
      membership: input.membership,
    });
    return {
      itemKind: "certified_record",
      itemHash: input.item.actionCertifiedRecord.recordHash,
    };
  });

const verifyReanchorCatchUpItem = (input: {
  readonly page: CatchUpPage;
  readonly item: CompletedReanchor;
  readonly membership: VerifiedMembership;
}): Effect.Effect<VerifiedCatchUpItem, ClientRepresentationError> =>
  Effect.gen(function* () {
    if (
      input.page.request.knownRecordHash === null ||
      input.item.reanchor.selectedRecordHash !==
        input.page.request.knownRecordHash ||
      input.item.reanchor.previousAnchorHash !==
        input.page.request.knownAnchorHash
    ) {
      return yield* representationFailure();
    }
    yield* verifyCompletedReanchor({
      completed: input.item,
      membership: input.membership,
    });
    return {
      itemKind: "completed_reanchor",
      itemHash: input.item.anchorHash,
    };
  });

const verifyCatchUpItem = (input: {
  readonly page: CatchUpPage;
  readonly membership: VerifiedMembership;
}): Effect.Effect<VerifiedCatchUpItem, ClientRepresentationError> => {
  const item = input.page.item;
  return item.kind === "certified_record"
    ? verifyCertifiedCatchUpItem({
        page: input.page,
        item,
        membership: input.membership,
      })
    : verifyReanchorCatchUpItem({
        page: input.page,
        item,
        membership: input.membership,
      });
};

export const verifyCatchUpPage = (input: {
  readonly page: CatchUpPage;
  readonly membership: VerifiedMembership;
  readonly responseSenderAgentId: AgentId;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    yield* verifyCatchUpRequest({
      request: input.page.request,
      membership: input.membership,
    });
    const verifiedItem = yield* verifyCatchUpItem(input);
    yield* verifyCatchUpAttestation({
      representation: input.page.attestation,
      membership: input.membership,
      responseSenderAgentId: input.responseSenderAgentId,
      request: input.page.request,
      itemKind: verifiedItem.itemKind,
      itemHash: verifiedItem.itemHash,
      hasMore: input.page.hasMore,
    });
  }).pipe(Effect.withSpan("verifyCatchUpPage"));

export const verifyCatchUpIncomplete = (input: {
  readonly incomplete: CatchUpIncomplete;
  readonly membership: VerifiedMembership;
  readonly responseSenderAgentId: AgentId;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    yield* verifyCatchUpRequest({
      request: input.incomplete.request,
      membership: input.membership,
    });
    yield* verifyCatchUpAttestation({
      representation: input.incomplete.attestation,
      membership: input.membership,
      responseSenderAgentId: input.responseSenderAgentId,
      request: input.incomplete.request,
      itemKind: "incomplete",
      itemHash: null,
      hasMore: false,
    });
  }).pipe(Effect.withSpan("verifyCatchUpIncomplete"));

/* eslint-enable jsdoc/require-jsdoc -- Restore package documentation rules. */
