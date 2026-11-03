/** @file Conversation records preserve canonical encodings and verify the evidence their participants sign. */

import { it } from "@effect/vitest";
import canonicalize from "canonicalize";
import { Effect, Encoding, Option, Schema } from "effect";
import { generateKeyPairSync } from "node:crypto";
import { describe, expect } from "vitest";
import {
  identifier,
  issueTestCard,
  makeTestAuthority,
  type RegistryKeyPair,
} from "../__tests__/agents.js";
import {
  ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
} from "../encoding/canonical-json.js";
import {
  AgentId,
  AgentIdentityCard,
  AgentName,
  type VerifiedAgentIdentityCard,
} from "../identity/agent-cards.js";
import {
  AgentPublicKey,
  type AgentSigningAuthority as AgentSigningAuthorityValue,
} from "../identity/keys.js";
import {
  EnvelopeId,
  SealedBody,
  SignedMessage,
} from "../identity/message-security.js";
import { WIRE_VERSION, WireVersions } from "../version.js";
import {
  Content,
  type Content as ContentValue,
  maximumContentBytes,
  maximumMembers,
  mintMsgId,
} from "./messages.js";
import {
  type ActionCertifiedRecord,
  type ActionCore,
  ActionHash,
  type ActionProposal,
  ActionSignatureStatement,
  CatchUpPage,
  CatchUpRequest,
  type CertifiedRecord,
  type CompletedReanchor,
  ConversationId,
  decodeOuterBody,
  deriveConversationId,
  deriveEvidenceEnvelopeId,
  DirectPacket,
  EvidenceStatement,
  hashAction,
  hashAnchor,
  hashMessageIntent,
  hashRecord,
  type InitialAnchorBody,
  MembershipDescriptor as MembershipDescriptorSchema,
  MembershipHash,
  type MessageIntent,
  quorumThreshold,
  type ReanchorBody,
  RecordCore as RecordCoreSchema,
  signEvidenceMessage,
  signOuterEvidence,
  signOuterPacket,
  type VerifiedMembership,
  verifyActionCertifiedRecord,
  verifyActionProposal,
  verifyCatchUpPage,
  verifyCertifiedRecord,
  verifyCompletedReanchor,
  verifyDeliveredEvidence,
  verifyMembershipDescriptor,
  verifyStableEvidence,
} from "./records.js";
import { RouterInstanceId } from "./router/client.js";

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

const encodedIdentifier = (
  prefix: string,
  byteLength: number,
  byte: number,
): string =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(byteLength).fill(byte))}`;

const firstAgentId = Schema.decodeUnknownSync(AgentId)(
  encodedIdentifier("agt_", 16, 1),
);
const secondAgentId = Schema.decodeUnknownSync(AgentId)(
  encodedIdentifier("agt_", 16, 2),
);
const conversationId = Schema.decodeUnknownSync(ConversationId)(
  encodedIdentifier("cnv_", 32, 3),
);
const membershipHash = Schema.decodeUnknownSync(MembershipHash)(
  encodedIdentifier("mbr_", 32, 4),
);

const canonicalActionHash = encodedIdentifier("ach_", 32, 5);

const canonicalInputBytes = (value: unknown): Uint8Array => {
  const text = Schema.decodeUnknownSync(Schema.String)(canonicalize(value));
  return utf8Encoder.encode(text);
};

const catchUpRequest = Schema.decodeUnknownSync(CatchUpRequest)({
  sharnessVersion: WIRE_VERSION,
  kind: "catch_up_request",
  conversationId,
  membershipHash,
  requesterAgentId: firstAgentId,
  knownRecordHash: null,
  knownAnchorHash: null,
});

const expectEncodingFailure = <Value>(
  effect: Effect.Effect<Value, ClientRepresentationError>,
) =>
  Effect.flip(effect).pipe(
    Effect.tap((failure) => {
      expect(failure).toBeInstanceOf(ClientRepresentationError);
      return Effect.void;
    }),
    Effect.asVoid,
  );

const verifiesCanonicalClosure = () =>
  Effect.gen(function* () {
    const bytes = yield* encodeCanonical(CatchUpRequest, catchUpRequest);
    expect(yield* decodeCanonical(CatchUpRequest, bytes)).toEqual(
      catchUpRequest,
    );

    const nonCanonical = utf8Encoder.encode(` ${utf8Decoder.decode(bytes)}`);
    yield* expectEncodingFailure(decodeCanonical(CatchUpRequest, nonCanonical));
    yield* expectEncodingFailure(
      decodeCanonical(
        CatchUpRequest,
        canonicalInputBytes({ ...catchUpRequest, unexpected: true }),
      ),
    );
  });

const derivesPrivateIdentifiers = () =>
  Effect.gen(function* () {
    const memberAgentIds = [firstAgentId, secondAgentId] as const;
    const firstConversationId = yield* deriveConversationId(memberAgentIds);
    const secondConversationId = yield* deriveConversationId(memberAgentIds);
    expect(firstConversationId).toBe(secondConversationId);
    yield* expectEncodingFailure(
      deriveConversationId([secondAgentId, firstAgentId] as const),
    );

    const firstMsgId = yield* mintMsgId();
    const secondMsgId = yield* mintMsgId();
    expect(secondMsgId).not.toBe(firstMsgId);
  });

const derivesStableEvidenceIdentity = () =>
  Effect.gen(function* () {
    const statement = Schema.decodeUnknownSync(ActionSignatureStatement)({
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: firstAgentId,
      actionHash: Schema.decodeUnknownSync(ActionHash)(
        encodedIdentifier("ach_", 32, 5),
      ),
    });
    const first = yield* deriveEvidenceEnvelopeId(statement);
    const retry = yield* deriveEvidenceEnvelopeId(statement);
    expect(retry).toBe(first);
  });

const makeMember = (byte: number, registryKeys: RegistryKeyPair) =>
  Effect.gen(function* () {
    const authority = yield* makeTestAuthority();
    const card = yield* issueTestCard({
      byte,
      name: `sealed-member-${byte}`,
      authority,
      registryKeys,
    });
    return { identityCard: card, signingAuthority: authority };
  });

const opensSealedBodiesOnlyForMembers = () =>
  Effect.gen(function* () {
    const registryKeys = generateKeyPairSync("ed25519");
    const sender = yield* makeMember(1, registryKeys);
    const receiver = yield* makeMember(2, registryKeys);
    const outsider = yield* makeMember(3, registryKeys);

    const sealed = yield* signOuterPacket({
      packet: catchUpRequest,
      membership: { members: [sender.identityCard, receiver.identityCard] },
      ...sender,
    });

    yield* expectEncodingFailure(decodeCanonical(CatchUpRequest, sealed.body));
    expect(
      yield* decodeOuterBody({ ...receiver, message: sealed }),
    ).toStrictEqual({ kind: "direct", packet: catchUpRequest });
    expect(
      yield* decodeOuterBody({ ...sender, message: sealed }),
    ).toStrictEqual({ kind: "direct", packet: catchUpRequest });
    yield* expectEncodingFailure(
      decodeOuterBody({ ...outsider, message: sealed }),
    );
  });

const refusesPlaintextOuterBody = () =>
  Effect.gen(function* () {
    const registryKeys = generateKeyPairSync("ed25519");
    const sender = yield* makeMember(1, registryKeys);
    const receiver = yield* makeMember(2, registryKeys);

    const plaintext = yield* SignedMessage.sign({
      identityCard: sender.identityCard,
      signingAuthority: sender.signingAuthority,
      recipientAgentIds: new Set([
        sender.identityCard.agentId,
        receiver.identityCard.agentId,
      ]),
      envelopeId: Schema.decodeUnknownSync(EnvelopeId)(
        encodedIdentifier("env_", 16, 6),
      ),
      body: yield* encodeCanonical(CatchUpRequest, catchUpRequest),
    });

    yield* expectEncodingFailure(
      decodeOuterBody({ ...receiver, message: plaintext }),
    );
  });

const refusesSealedBodyHoldingNoClientValue = () =>
  Effect.gen(function* () {
    const registryKeys = generateKeyPairSync("ed25519");
    const sender = yield* makeMember(1, registryKeys);
    const receiver = yield* makeMember(2, registryKeys);
    const envelopeId = Schema.decodeUnknownSync(EnvelopeId)(
      encodedIdentifier("env_", 16, 7),
    );
    const sealed = yield* SealedBody.seal({
      senderAgentId: sender.identityCard.agentId,
      recipientIdentityCards: [sender.identityCard, receiver.identityCard],
      envelopeId,
      plaintext: utf8Encoder.encode("{}"),
    });

    const message = yield* SignedMessage.sign({
      identityCard: sender.identityCard,
      signingAuthority: sender.signingAuthority,
      recipientAgentIds: new Set([
        sender.identityCard.agentId,
        receiver.identityCard.agentId,
      ]),
      envelopeId,
      body: sealed,
    });

    yield* expectEncodingFailure(decodeOuterBody({ ...receiver, message }));
  });

// @agent-code-guard/regression-only: these examples pin the accepted private Client wire boundary and hostile-input closure.
describe("Client protocol representation", () => {
  it.live(
    "accepts exact JCS and rejects alternate or open representations",
    verifiesCanonicalClosure,
  );
  it.live(
    "derives stable conversation and author-scoped message identities",
    derivesPrivateIdentifiers,
  );
  it.live(
    "derives a stable inner evidence envelope identity",
    derivesStableEvidenceIdentity,
  );
  it.live(
    "opens a sealed outer body for each member and for no one else",
    opensSealedBodiesOnlyForMembers,
  );
  it.live("refuses a plaintext outer body", refusesPlaintextOuterBody);
  it.live(
    "refuses a sealed outer body that holds no Client value",
    refusesSealedBodyHoldingNoClientValue,
  );
  // Every wire hash identifier admits only its prefix over the canonical
  // base64url of 32 bytes.
  it("accepts a hash identifier in the canonical 32-byte form", () => {
    expect(Schema.decodeUnknownSync(ActionHash)(canonicalActionHash)).toBe(
      canonicalActionHash,
    );
  });
  it.each([
    {
      form: "another identifier's prefix",
      candidate: encodedIdentifier("rch_", 32, 5),
    },
    { form: "31 bytes", candidate: encodedIdentifier("ach_", 31, 5) },
    {
      form: "non-zero base64url padding bits",
      candidate: `${canonicalActionHash.slice(0, -1)}V`,
    },
  ])("rejects a hash identifier with $form", ({ candidate }) => {
    expect(Schema.is(ActionHash)(candidate)).toBe(false);
  });
});

const maximumIdentityBodyBytes = 262_144;
const maximumIdentityRecipients = 128;

interface IdentityFixture {
  readonly card: VerifiedAgentIdentityCard;
  readonly authority: AgentSigningAuthorityValue;
}

interface ProtocolFixture {
  readonly identities: readonly IdentityFixture[];
  readonly membership: VerifiedMembership;
}

interface RecordFixture {
  readonly action: ActionCore;
  readonly actionHash: ActionHash;
  readonly actionRepresentations: readonly unknown[];
  readonly actionCertifiedRecord: ActionCertifiedRecord;
  readonly durabilityRepresentations: readonly unknown[];
  readonly certifiedRecord: CertifiedRecord;
}

const firstRouterInstanceId = Schema.decodeUnknownSync(RouterInstanceId)(
  identifier("rti_", 41),
);
const secondRouterInstanceId = Schema.decodeUnknownSync(RouterInstanceId)(
  identifier("rti_", 42),
);

const maximumAgentName = (index: number) =>
  Schema.decodeUnknownSync(AgentName)(
    `member-${String(index).padStart(2, "0")}-${"x".repeat(22)}`,
  );

function asNonEmpty<Value>(
  values: readonly Value[],
): readonly [Value, ...Value[]] {
  return [at(values, 0), ...values.slice(1)];
}

function asAtLeastTwo<Value>(
  values: readonly Value[],
): readonly [Value, Value, ...Value[]] {
  return [at(values, 0), at(values, 1), ...values.slice(2)];
}

function at<Value>(values: readonly Value[], index: number): Value {
  const value = values[index];
  if (value === undefined) {
    throw new Error(`missing fixture item ${index}`);
  }
  return value;
}

const makeProtocolFixture = (memberCount: number) =>
  Effect.gen(function* () {
    const registryKeys = generateKeyPairSync("ed25519");
    const registrySignerPublicKey = yield* Schema.decodeUnknown(AgentPublicKey)(
      registryKeys.publicKey.export({ format: "jwk" }),
    );
    const identities = yield* Effect.forEach(
      Array.from(Array(memberCount).keys(), (index) => index + 1),
      (byte) =>
        Effect.gen(function* () {
          const authority = yield* makeTestAuthority();
          const card = yield* issueTestCard({
            byte,
            name: maximumAgentName(byte),
            authority,
            registryKeys,
          });
          return { card, authority } satisfies IdentityFixture;
        }),
      { concurrency: 1 },
    );
    const encodedCards = yield* Effect.forEach(
      identities,
      ({ card }) => Schema.encode(AgentIdentityCard)(card),
      { concurrency: 1 },
    );
    const memberAgentIds = asAtLeastTwo(
      identities.map(({ card }) => card.agentId),
    );
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptorSchema)({
      sharnessVersion: WIRE_VERSION,
      kind: "membership_descriptor",
      conversationId: yield* deriveConversationId(memberAgentIds),
      members: asAtLeastTwo(encodedCards),
    });
    return {
      identities,
      membership: yield* verifyMembershipDescriptor(
        descriptor,
        registrySignerPublicKey,
      ),
    } satisfies ProtocolFixture;
  });

const encodeEvidence = (messages: readonly SignedMessage[]) =>
  Effect.forEach(messages, (message) => Schema.encode(SignedMessage)(message), {
    concurrency: 1,
  });

const signActionEvidence = (fixture: ProtocolFixture, actionHash: ActionHash) =>
  Effect.forEach(
    fixture.identities,
    ({ card, authority }) =>
      signEvidenceMessage({
        statement: {
          sharnessVersion: WIRE_VERSION,
          kind: "action_signature",
          signerAgentId: card.agentId,
          actionHash,
        },
        identityCard: card,
        signingAuthority: authority,
      }),
    { concurrency: 1 },
  );

const signDurabilityEvidence = (
  fixture: ProtocolFixture,
  recordHash: ActionCertifiedRecord["recordHash"],
) =>
  Effect.forEach(
    fixture.identities,
    ({ card, authority }) =>
      signEvidenceMessage({
        statement: {
          sharnessVersion: WIRE_VERSION,
          kind: "durability_vote",
          signerAgentId: card.agentId,
          conversationId: fixture.membership.descriptor.conversationId,
          membershipHash: fixture.membership.hash,
          recordHash,
        },
        identityCard: card,
        signingAuthority: authority,
      }),
    { concurrency: 1 },
  );

const buildRecord = (input: {
  readonly fixture: ProtocolFixture;
  readonly action: ActionCore;
  readonly routerAnchor: ActionCertifiedRecord["routerAnchor"];
}) =>
  Effect.gen(function* () {
    const actionHash = yield* hashAction(input.action);
    const actionMessages = yield* signActionEvidence(input.fixture, actionHash);
    const actionRepresentations = yield* encodeEvidence(actionMessages);
    const anchorHash =
      input.action.kind === "FIRST_MESSAGE"
        ? yield* hashAnchor(input.action.anchor)
        : input.action.anchorHash;
    const recordCore: ActionCertifiedRecord["recordCore"] =
      input.action.kind === "FIRST_MESSAGE"
        ? {
            sharnessVersion: WIRE_VERSION,
            kind: "record_core",
            membership: input.fixture.membership.descriptor,
            anchorHash,
            action: input.action,
            actionHash,
          }
        : {
            sharnessVersion: WIRE_VERSION,
            kind: "record_core",
            membershipHash: input.fixture.membership.hash,
            anchorHash,
            action: input.action,
            actionHash,
          };
    const recordHash = yield* hashRecord(recordCore);
    const actionCertifiedRecord: ActionCertifiedRecord = {
      sharnessVersion: WIRE_VERSION,
      kind: "action_certified_record",
      recordHash,
      recordCore,
      routerAnchor: input.routerAnchor,
      actionCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_certificate",
        actionHash,
        signatures: asNonEmpty(actionRepresentations),
      },
    };
    const durabilityMessages = yield* signDurabilityEvidence(
      input.fixture,
      recordHash,
    );
    const durabilityRepresentations = yield* encodeEvidence(durabilityMessages);
    const certifiedRecord: CertifiedRecord = {
      sharnessVersion: WIRE_VERSION,
      kind: "certified_record",
      actionCertifiedRecord,
      durabilityCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_certificate",
        recordHash,
        votes: asNonEmpty(durabilityRepresentations),
      },
    };
    return {
      action: input.action,
      actionHash,
      actionRepresentations,
      actionCertifiedRecord,
      durabilityRepresentations,
      certifiedRecord,
    } satisfies RecordFixture;
  });

const buildFirstMessage = (fixture: ProtocolFixture, content: ContentValue) =>
  Effect.gen(function* () {
    const firstIdentity = at(fixture.identities, 0);
    const messageIntent: MessageIntent = {
      sharnessVersion: WIRE_VERSION,
      kind: "message_intent",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      authorAgentId: firstIdentity.card.agentId,
      msgId: yield* mintMsgId(),
      content,
    };
    const anchor: InitialAnchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "initial_anchor_body",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      routerInstanceId: firstRouterInstanceId,
    };
    const action: ActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "FIRST_MESSAGE",
      conversationId: fixture.membership.descriptor.conversationId,
      membership: fixture.membership.descriptor,
      anchor,
      previousRecordHash: null,
      messageIntent,
      messageIntentHash: yield* hashMessageIntent(messageIntent),
    };
    return yield* buildRecord({ fixture, action, routerAnchor: anchor });
  });

/**
 * A MESSAGE that succeeds `firstMessage`, under `reanchored` when the conversation
 * re-anchored after it and under the initial anchor otherwise.
 */
const buildMessage = (
  fixture: ProtocolFixture,
  firstMessage: RecordFixture,
  content: ContentValue,
  reanchored?: CompletedReanchor,
) =>
  Effect.gen(function* () {
    const firstIdentity = at(fixture.identities, 0);
    const messageIntent: MessageIntent = {
      sharnessVersion: WIRE_VERSION,
      kind: "message_intent",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      authorAgentId: firstIdentity.card.agentId,
      msgId: yield* mintMsgId(),
      content,
    };
    const action: ActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "MESSAGE",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      anchorHash:
        reanchored?.anchorHash ??
        firstMessage.actionCertifiedRecord.recordCore.anchorHash,
      previousRecordHash: firstMessage.actionCertifiedRecord.recordHash,
      messageIntent,
      messageIntentHash: yield* hashMessageIntent(messageIntent),
    };
    return yield* buildRecord({
      fixture,
      action,
      routerAnchor:
        reanchored ?? firstMessage.actionCertifiedRecord.routerAnchor,
    });
  });

/**
 * Every member's vote to re-anchor the conversation at the second Router
 * instance, selecting `selected` as its last certified record.
 */
const voteReanchor = (fixture: ProtocolFixture, selected: RecordFixture) =>
  Effect.gen(function* () {
    const reanchor: ReanchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_body",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      previousAnchorHash: selected.actionCertifiedRecord.recordCore.anchorHash,
      selectedRecordHash: selected.actionCertifiedRecord.recordHash,
      routerInstanceId: secondRouterInstanceId,
    };
    const anchorHash = yield* hashAnchor(reanchor);
    const votes = yield* Effect.forEach(
      fixture.identities,
      ({ card, authority }) =>
        signEvidenceMessage({
          statement: {
            sharnessVersion: WIRE_VERSION,
            kind: "reanchor_vote",
            signerAgentId: card.agentId,
            anchorHash,
            reanchor,
          },
          identityCard: card,
          signingAuthority: authority,
        }),
      { concurrency: 1 },
    ).pipe(Effect.flatMap(encodeEvidence));
    return { reanchor, anchorHash, votes };
  });

/**
 * Expect `effect` to fail as a representation error. A value that verifies
 * instead fails as a short string: reporting the verified value itself,
 * membership keys included, can hang the run.
 * @param effect Verification expected to fail.
 * @returns Completion once the failure is checked.
 */
const expectRepresentationFailure = <Value>(
  effect: Effect.Effect<Value, ClientRepresentationError>,
) =>
  effect.pipe(
    Effect.as("it verified"),
    Effect.flip,
    Effect.tap((failure) => {
      expect(failure).toBeInstanceOf(ClientRepresentationError);
      return Effect.void;
    }),
    Effect.asVoid,
  );

const verifiesThreshold = (memberCount: number, expectedThreshold: number) =>
  Effect.gen(function* () {
    const fixture = yield* makeProtocolFixture(memberCount);
    const firstMessage = yield* buildFirstMessage(fixture, [
      { type: "text", text: `members-${memberCount}` },
    ]);
    const accepted: CertifiedRecord = {
      ...firstMessage.certifiedRecord,
      durabilityCertificate: {
        ...firstMessage.certifiedRecord.durabilityCertificate,
        votes: asNonEmpty(
          firstMessage.durabilityRepresentations.slice(0, expectedThreshold),
        ),
      },
    };
    expect(quorumThreshold(memberCount)).toBe(expectedThreshold);
    const votes = yield* verifyCertifiedRecord({
      record: accepted,
      membership: fixture.membership,
    });
    expect(votes.map(({ statement }) => statement)).toMatchObject(
      Array.from({ length: expectedThreshold }, () => ({
        kind: "durability_vote",
        recordHash: accepted.durabilityCertificate.recordHash,
      })),
    );
    const belowThreshold: CertifiedRecord = {
      ...accepted,
      durabilityCertificate: {
        ...accepted.durabilityCertificate,
        votes: asNonEmpty(
          firstMessage.durabilityRepresentations.slice(
            0,
            expectedThreshold - 1,
          ),
        ),
      },
    };
    yield* expectRepresentationFailure(
      verifyCertifiedRecord({
        record: belowThreshold,
        membership: fixture.membership,
      }),
    );
  });

const enforcesFirstMessageAndMessageEvidence = () =>
  Effect.gen(function* () {
    const fixture = yield* makeProtocolFixture(4);
    const firstMessage = yield* buildFirstMessage(fixture, [
      { type: "text", text: "first message" },
    ]);
    const nonUnanimousFirstMessage: ActionCertifiedRecord = {
      ...firstMessage.actionCertifiedRecord,
      actionCertificate: {
        ...firstMessage.actionCertifiedRecord.actionCertificate,
        signatures: asNonEmpty(firstMessage.actionRepresentations.slice(0, 3)),
      },
    };
    yield* expectRepresentationFailure(
      verifyActionCertifiedRecord({
        record: nonUnanimousFirstMessage,
        membership: fixture.membership,
      }),
    );

    const message = yield* buildMessage(fixture, firstMessage, [
      { type: "text", text: "ordinary message" },
    ]);
    const thresholdMessage: ActionCertifiedRecord = {
      ...message.actionCertifiedRecord,
      actionCertificate: {
        ...message.actionCertifiedRecord.actionCertificate,
        signatures: asNonEmpty(message.actionRepresentations.slice(0, 3)),
      },
    };
    yield* verifyActionCertifiedRecord({
      record: thresholdMessage,
      membership: fixture.membership,
    });
    const missingAuthor: ActionCertifiedRecord = {
      ...thresholdMessage,
      actionCertificate: {
        ...thresholdMessage.actionCertificate,
        signatures: [
          at(message.actionRepresentations, 1),
          at(message.actionRepresentations, 2),
          at(message.actionRepresentations, 3),
        ],
      },
    };
    yield* expectRepresentationFailure(
      verifyActionCertifiedRecord({
        record: missingAuthor,
        membership: fixture.membership,
      }),
    );

    const wrongEvidenceKind: CertifiedRecord = {
      ...message.certifiedRecord,
      durabilityCertificate: {
        ...message.certifiedRecord.durabilityCertificate,
        votes: [
          at(message.actionRepresentations, 0),
          at(message.durabilityRepresentations, 1),
          at(message.durabilityRepresentations, 2),
        ],
      },
    };
    yield* expectRepresentationFailure(
      verifyCertifiedRecord({
        record: wrongEvidenceKind,
        membership: fixture.membership,
      }),
    );
  });

/**
 * A MESSAGE record names its membership only by `MembershipHash`, so it verifies
 * against the membership an endpoint holds from the conversation's FIRST_MESSAGE and
 * not against another conversation's membership. A FIRST_MESSAGE record verifies
 * only against the descriptor it carries. Fails when a record verifies against
 * a membership it does not name, when a FIRST_MESSAGE or MESSAGE core names another
 * membership than the one its action and the endpoint hold, or when a core
 * whose membership field does not fit its action decodes.
 * @returns Completion after every case is checked.
 */
const verifiesRecordsAgainstTheHeldMembership = () =>
  Effect.gen(function* () {
    const fixture = yield* makeProtocolFixture(4);
    const other = yield* makeProtocolFixture(4);
    const firstMessage = yield* buildFirstMessage(fixture, [
      { type: "text", text: "first message" },
    ]);
    const message = yield* buildMessage(fixture, firstMessage, [
      { type: "text", text: "ordinary message" },
    ]);

    yield* verifyCertifiedRecord({
      record: firstMessage.certifiedRecord,
      membership: fixture.membership,
    });
    yield* verifyCertifiedRecord({
      record: message.certifiedRecord,
      membership: fixture.membership,
    });
    yield* expectRepresentationFailure(
      verifyCertifiedRecord({
        record: firstMessage.certifiedRecord,
        membership: other.membership,
      }),
    );
    yield* expectRepresentationFailure(
      verifyCertifiedRecord({
        record: message.certifiedRecord,
        membership: other.membership,
      }),
    );

    const messageCore = message.actionCertifiedRecord.recordCore;
    if (!("membershipHash" in messageCore)) {
      return yield* Effect.dieMessage("core lacks membership hash");
    }
    const renamedCore = {
      ...messageCore,
      membershipHash: other.membership.hash,
    };
    yield* expectRepresentationFailure(
      verifyActionCertifiedRecord({
        record: {
          ...message.actionCertifiedRecord,
          recordCore: renamedCore,
          recordHash: yield* hashRecord(renamedCore),
        },
        membership: fixture.membership,
      }),
    );

    const firstMessageCore = firstMessage.actionCertifiedRecord.recordCore;
    if (!("membership" in firstMessageCore)) {
      return yield* Effect.dieMessage("core lacks membership descriptor");
    }
    const foreignCore = {
      ...firstMessageCore,
      membership: other.membership.descriptor,
    };
    yield* expectRepresentationFailure(
      verifyActionCertifiedRecord({
        record: {
          ...firstMessage.actionCertifiedRecord,
          recordCore: foreignCore,
          recordHash: yield* hashRecord(foreignCore),
        },
        membership: fixture.membership,
      }),
    );

    const decodesAsRecordCore = (value: unknown) =>
      Schema.decodeUnknown(RecordCoreSchema)(value).pipe(
        Effect.as(true),
        Effect.orElseSucceed(() => false),
      );
    const { sharnessVersion, ...unversionedCore } = messageCore;
    expect(
      yield* decodesAsRecordCore({
        ...unversionedCore,
        moltzapVersion: sharnessVersion,
      }),
    ).toBe(false);
    expect(
      yield* decodesAsRecordCore({
        ...messageCore,
        moltzapVersion: sharnessVersion,
      }),
    ).toBe(false);
    const { membershipHash, ...messageCoreFields } = messageCore;
    expect(
      yield* decodesAsRecordCore({
        ...messageCoreFields,
        membership: fixture.membership.descriptor,
      }),
    ).toBe(false);
    const { membership: firstMessageDescriptor, ...firstMessageCoreFields } =
      firstMessageCore;
    expect(
      yield* decodesAsRecordCore({
        ...firstMessageCoreFields,
        membershipHash: fixture.membership.hash,
      }),
    ).toBe(false);
    expect(
      yield* decodesAsRecordCore({ ...messageCoreFields, membershipHash }),
    ).toBe(true);
    expect(
      yield* decodesAsRecordCore({
        ...firstMessageCoreFields,
        membership: firstMessageDescriptor,
      }),
    ).toBe(true);
  });

const verifiesProposalEnvelopeAttribution = () =>
  Effect.gen(function* () {
    const fixture = yield* makeProtocolFixture(4);
    const firstMessage = yield* buildFirstMessage(fixture, [
      { type: "text", text: "proposal" },
    ]);
    const author = at(fixture.identities, 0).card.agentId;
    const proposal: ActionProposal = {
      sharnessVersion: WIRE_VERSION,
      kind: "action_proposal",
      action: firstMessage.action,
    };
    yield* verifyActionProposal({
      proposal,
      membership: fixture.membership,
      outerSenderAgentId: author,
    });
    yield* expectRepresentationFailure(
      verifyActionProposal({
        proposal,
        membership: fixture.membership,
        outerSenderAgentId: at(fixture.identities, 1).card.agentId,
      }),
    );
  });

const verifiesReanchorCatchUpBindings = () =>
  Effect.gen(function* () {
    const fixture = yield* makeProtocolFixture(4);
    const firstMessage = yield* buildFirstMessage(fixture, [
      { type: "text", text: "catch up" },
    ]);
    const {
      reanchor,
      anchorHash,
      votes: reanchorRepresentations,
    } = yield* voteReanchor(fixture, firstMessage);
    const completed: CompletedReanchor = {
      sharnessVersion: WIRE_VERSION,
      kind: "completed_reanchor",
      anchorHash,
      reanchor,
      certificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_certificate",
        anchorHash,
        votes: asNonEmpty(reanchorRepresentations.slice(0, 3)),
      },
    };
    yield* verifyCompletedReanchor({
      completed,
      membership: fixture.membership,
    });

    const responder = at(fixture.identities, 0);
    const request: CatchUpRequest = {
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_request",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      requesterAgentId: at(fixture.identities, 1).card.agentId,
      knownRecordHash: firstMessage.actionCertifiedRecord.recordHash,
      knownAnchorHash: firstMessage.actionCertifiedRecord.recordCore.anchorHash,
    };
    const attestation = yield* signEvidenceMessage({
      statement: {
        sharnessVersion: WIRE_VERSION,
        kind: "catch_up_attestation",
        signerAgentId: responder.card.agentId,
        request,
        itemKind: "completed_reanchor",
        itemHash: completed.anchorHash,
        hasMore: false,
      },
      identityCard: responder.card,
      signingAuthority: responder.authority,
    });
    const page: CatchUpPage = {
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_page",
      request,
      item: completed,
      hasMore: false,
      attestation: yield* Schema.encode(SignedMessage)(attestation),
    };
    yield* verifyCatchUpPage({
      page,
      membership: fixture.membership,
      responseSenderAgentId: responder.card.agentId,
    });
    yield* expectRepresentationFailure(
      verifyCatchUpPage({
        page,
        membership: fixture.membership,
        responseSenderAgentId: at(fixture.identities, 1).card.agentId,
      }),
    );
  });

/**
 * A member that holds no record catches up FIRST_MESSAGE first: a page for an empty
 * position must carry FIRST_MESSAGE, whose descriptor gives the member the
 * membership a later MESSAGE page names only by hash. A MESSAGE page then verifies
 * at the position FIRST_MESSAGE sets. Fails when an empty position accepts a MESSAGE
 * record, which the member could not resolve without the descriptor, or
 * another conversation's FIRST_MESSAGE.
 * @returns Completion after every page is checked.
 */
const catchesUpFirstMessageFirst = () =>
  Effect.gen(function* () {
    const fixture = yield* makeProtocolFixture(4);
    const firstMessage = yield* buildFirstMessage(fixture, [
      { type: "text", text: "first message" },
    ]);
    const message = yield* buildMessage(fixture, firstMessage, [
      { type: "text", text: "after first message" },
    ]);
    const responder = at(fixture.identities, 0);
    const request = (
      known: Pick<CatchUpRequest, "knownRecordHash" | "knownAnchorHash">,
    ): CatchUpRequest => ({
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_request",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      requesterAgentId: at(fixture.identities, 1).card.agentId,
      ...known,
    });
    const page = (asked: CatchUpRequest, item: CertifiedRecord) =>
      signEvidenceMessage({
        statement: {
          sharnessVersion: WIRE_VERSION,
          kind: "catch_up_attestation",
          signerAgentId: responder.card.agentId,
          request: asked,
          itemKind: "certified_record",
          itemHash: item.actionCertifiedRecord.recordHash,
          hasMore: false,
        },
        identityCard: responder.card,
        signingAuthority: responder.authority,
      }).pipe(
        Effect.flatMap((attestation) =>
          Schema.encode(SignedMessage)(attestation),
        ),
        Effect.map(
          (attestation): CatchUpPage => ({
            sharnessVersion: WIRE_VERSION,
            kind: "catch_up_page",
            request: asked,
            item,
            hasMore: false,
            attestation,
          }),
        ),
      );
    const verify = (candidate: CatchUpPage) =>
      verifyCatchUpPage({
        page: candidate,
        membership: fixture.membership,
        responseSenderAgentId: responder.card.agentId,
      });
    const empty = request({ knownRecordHash: null, knownAnchorHash: null });
    const atFirstMessage = request({
      knownRecordHash: firstMessage.actionCertifiedRecord.recordHash,
      knownAnchorHash: firstMessage.actionCertifiedRecord.recordCore.anchorHash,
    });

    const elsewhere = yield* buildFirstMessage(yield* makeProtocolFixture(4), [
      { type: "text", text: "elsewhere" },
    ]);

    yield* verify(yield* page(empty, firstMessage.certifiedRecord));
    yield* expectRepresentationFailure(
      verify(yield* page(empty, message.certifiedRecord)),
    );
    yield* expectRepresentationFailure(
      verify(yield* page(empty, elsewhere.certifiedRecord)),
    );
    yield* verify(yield* page(atFirstMessage, message.certifiedRecord));
  });

/**
 * Builds the largest catch-up page: a MESSAGE of the largest content at the
 * maximum membership, certified by every member under a re-anchor every
 * member voted for.
 */
const buildMaximumArtifact = Effect.gen(function* () {
  const fixture = yield* makeProtocolFixture(maximumMembers);
  const empty = [{ type: "text", text: "" }] as const;
  const fixedBytes = yield* encodeCanonical(Content, empty);
  const content = yield* Schema.decodeUnknown(Content)([
    {
      type: "text",
      text: "x".repeat(maximumContentBytes - fixedBytes.byteLength),
    },
  ]);
  const firstMessage = yield* buildFirstMessage(fixture, content);
  const { reanchor, anchorHash, votes } = yield* voteReanchor(
    fixture,
    firstMessage,
  );
  const reanchored: CompletedReanchor = {
    sharnessVersion: WIRE_VERSION,
    kind: "completed_reanchor",
    anchorHash,
    reanchor,
    certificate: {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_certificate",
      anchorHash,
      votes: asNonEmpty(votes),
    },
  };
  const message = yield* buildMessage(
    fixture,
    firstMessage,
    content,
    reanchored,
  );
  const responder = at(fixture.identities, 0);
  const request: CatchUpRequest = {
    sharnessVersion: WIRE_VERSION,
    kind: "catch_up_request",
    conversationId: fixture.membership.descriptor.conversationId,
    membershipHash: fixture.membership.hash,
    requesterAgentId: at(fixture.identities, 1).card.agentId,
    knownRecordHash: firstMessage.actionCertifiedRecord.recordHash,
    knownAnchorHash: anchorHash,
  };
  const attestation = yield* signEvidenceMessage({
    statement: {
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_attestation",
      signerAgentId: responder.card.agentId,
      request,
      itemKind: "certified_record",
      itemHash: message.actionCertifiedRecord.recordHash,
      hasMore: false,
    },
    identityCard: responder.card,
    signingAuthority: responder.authority,
  });
  const page: CatchUpPage = {
    sharnessVersion: WIRE_VERSION,
    kind: "catch_up_page",
    request,
    item: message.certifiedRecord,
    hasMore: false,
    attestation: yield* Schema.encode(SignedMessage)(attestation),
  };
  return { fixture, responder, page, attestation };
});

/**
 * The largest catch-up page, built once and shared by every case that checks
 * it; the first case to read it pays for the build.
 */
const maximumArtifact = Effect.runSync(Effect.cached(buildMaximumArtifact));

/**
 * Bounds a hang while building or checking a maximum-membership page. The
 * fixture issues every member's identity and signs five complete certificates;
 * cases then verify the page or seal it to every member. This cryptographic
 * work slows with machine load; no assertion depends on the timeout.
 */
const MAXIMUM_PAGE_TIMEOUT_MS = 30_000;

const verifiesMaximumPage = () =>
  Effect.gen(function* () {
    const { fixture, responder, page } = yield* maximumArtifact;

    yield* verifyCatchUpPage({
      page,
      membership: fixture.membership,
      responseSenderAgentId: responder.card.agentId,
    });
  });

const verifiesHistoryOnlyCatchUpItem = () =>
  Effect.gen(function* () {
    const { fixture, responder, page } = yield* maximumArtifact;
    const outer = yield* signOuterPacket({
      packet: page,
      membership: fixture.membership,
      identityCard: responder.card,
      signingAuthority: responder.authority,
    });
    const encodedOuter = yield* Schema.encode(SignedMessage)(outer);
    const encodedPage = yield* Schema.encode(CatchUpPage)(page);
    yield* Effect.gen(function* () {
      const retained = yield* Schema.decodeUnknown(CatchUpPage)(encodedPage);
      yield* verifyCatchUpPage({
        page: retained,
        membership: fixture.membership,
        responseSenderAgentId: responder.card.agentId,
      });
      expect(retained.item.sharnessVersion).toBe(WIRE_VERSION);
      expect(
        yield* Schema.decodeUnknown(SignedMessage.live)(encodedOuter).pipe(
          Effect.isFailure,
        ),
      ).toBe(true);
    }).pipe(
      Effect.provideService(WireVersions, {
        live: ["2099.102.0"],
        history: [WIRE_VERSION, "2099.102.0"],
      }),
    );
  });

const sealsMaximumPageInsideIdentityLimits = () =>
  Effect.gen(function* () {
    const { fixture, responder, page } = yield* maximumArtifact;

    const pagePlaintext = yield* encodeCanonical(DirectPacket, page);
    const outer = yield* signOuterPacket({
      packet: page,
      membership: fixture.membership,
      identityCard: responder.card,
      signingAuthority: responder.authority,
    });

    expect(pagePlaintext.byteLength).toBeLessThanOrEqual(
      Option.getOrThrow(SealedBody.maximumPlaintextByteLength(maximumMembers)),
    );
    expect(outer.body.byteLength).toBe(
      Option.getOrThrow(
        SealedBody.sealedByteLength({
          plaintextByteLength: pagePlaintext.byteLength,
          recipientCount: maximumMembers,
        }),
      ),
    );
    expect(outer.body.byteLength).toBeLessThanOrEqual(maximumIdentityBodyBytes);
    expect(outer.recipientAgentIds).toHaveLength(maximumMembers);
    expect(outer.recipientAgentIds.length).toBeLessThanOrEqual(
      maximumIdentityRecipients,
    );
    expect(SignedMessage.encodedByteLength(outer)).toBeLessThanOrEqual(
      SignedMessage.maximumEncodedByteLength,
    );
  });

const relaysMaximumAttestationInsideIdentityLimits = () =>
  Effect.gen(function* () {
    const { fixture, responder, attestation } = yield* maximumArtifact;

    const evidencePlaintext = yield* encodeCanonical(
      SignedMessage,
      attestation,
    );
    const relayedEvidence = yield* signOuterEvidence({
      evidence: attestation,
      membership: fixture.membership,
      identityCard: responder.card,
      signingAuthority: responder.authority,
    });

    expect(relayedEvidence.body.byteLength).toBe(
      Option.getOrThrow(
        SealedBody.sealedByteLength({
          plaintextByteLength: evidencePlaintext.byteLength,
          recipientCount: maximumMembers,
        }),
      ),
    );
    expect(relayedEvidence.body.byteLength).toBeLessThanOrEqual(
      maximumIdentityBodyBytes,
    );
  });

// @agent-code-guard/regression-only: these cases pin the accepted quorum, evidence, recovery, and size boundaries.
/** A valid signature and statement id cannot authenticate a different body. */
const rejectsSubstitutedEvidenceStatements = () =>
  Effect.gen(function* () {
    const fixture = yield* makeProtocolFixture(2);
    const signer = at(fixture.identities, 0);
    const statement: EvidenceStatement = {
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_attestation",
      signerAgentId: signer.card.agentId,
      request: {
        sharnessVersion: WIRE_VERSION,
        kind: "catch_up_request",
        conversationId: fixture.membership.descriptor.conversationId,
        membershipHash: fixture.membership.hash,
        requesterAgentId: signer.card.agentId,
        knownRecordHash: null,
        knownAnchorHash: null,
      },
      itemKind: "incomplete",
      itemHash: null,
      hasMore: false,
    };
    const evidence = yield* SignedMessage.sign({
      identityCard: signer.card,
      signingAuthority: signer.authority,
      recipientAgentIds: new Set([signer.card.agentId]),
      envelopeId: yield* deriveEvidenceEnvelopeId(statement),
      body: yield* encodeCanonical(EvidenceStatement, {
        ...statement,
        request: {
          ...statement.request,
          requesterAgentId: at(fixture.identities, 1).card.agentId,
        },
      }),
    });
    const representation = yield* Schema.encode(SignedMessage)(evidence);
    yield* expectRepresentationFailure(
      verifyStableEvidence({
        representation,
        membership: fixture.membership,
        statement,
      }),
    );
    const outer = yield* signOuterEvidence({
      evidence,
      membership: fixture.membership,
      identityCard: signer.card,
      signingAuthority: signer.authority,
    });
    yield* expectRepresentationFailure(
      verifyDeliveredEvidence({
        outer,
        evidence,
        membership: fixture.membership,
        statement,
      }),
    );
    const matching = yield* signEvidenceMessage({
      statement,
      identityCard: signer.card,
      signingAuthority: signer.authority,
    });
    const verified = yield* verifyStableEvidence({
      representation: yield* Schema.encode(SignedMessage)(matching),
      membership: fixture.membership,
      statement,
    });
    expect(verified.statement).toEqual(statement);
  });

describe("Client protocol acceptance", () => {
  it.live(
    "rejects caller statements that differ from the signed evidence body",
    rejectsSubstitutedEvidenceStatements,
  );
  it.live.each([
    [2, 2],
    [3, 3],
    [4, 3],
    [10, 7],
  ] as const)(
    "accepts q(%i)=%i durability evidence and rejects one fewer vote",
    ([memberCount, threshold]) => verifiesThreshold(memberCount, threshold),
  );
  it.live(
    "requires unanimous FIRST_MESSAGE and author-inclusive MESSAGE evidence without hashing either certificate",
    enforcesFirstMessageAndMessageEvidence,
  );
  it.live(
    "verifies a record only against the membership it names, held from FIRST_MESSAGE",
    verifiesRecordsAgainstTheHeldMembership,
  );
  it.live(
    "requires the proposal envelope sender to be the message author",
    verifiesProposalEnvelopeAttribution,
  );
  it.live(
    "binds re-anchor and catch-up evidence to the exact position and responder",
    verifiesReanchorCatchUpBindings,
  );
  it.live(
    "catches up FIRST_MESSAGE first, so a later MESSAGE page resolves its membership",
    catchesUpFirstMessageFirst,
  );
  describe(
    "the largest catch-up page, a re-anchored MESSAGE at its maximum membership",
    { timeout: MAXIMUM_PAGE_TIMEOUT_MS },
    () => {
      it.live("verifies from its responder", verifiesMaximumPage);
      it.live(
        "verifies a history-only item inside a live catch-up page",
        verifiesHistoryOnlyCatchUpItem,
      );
      it.live(
        "fits inside Identity limits sealed to every member",
        sealsMaximumPageInsideIdentityLimits,
      );
      it.live(
        "relays its attestation inside Identity limits sealed to every member",
        relaysMaximumAttestationInsideIdentityLimits,
      );
    },
  );
});
