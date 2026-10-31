/** @file Cryptographic acceptance tests for addressed FIRST_MESSAGE and MESSAGE records. */

import { live as it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import { generateKeyPairSync } from "node:crypto";
import { describe, expect } from "vitest";
import {
  identifier,
  issueTestCard,
  makeTestAuthority,
} from "../../__tests__/agent-card-fixtures.js";
import {
  ClientRepresentationError,
  encodeCanonical,
} from "../../encoding/canonical-json.js";
import { AgentCard, type VerifiedAgentCard } from "../../identity/cards.js";
import { SealedBody } from "../../identity/envelope/sealed-body.js";
import { SignedMessage } from "../../identity/envelope/signed-message.js";
import { AgentName } from "../../identity/identifiers.js";
import {
  type AgentSigningAuthority as AgentSigningAuthorityValue,
  Ed25519PublicKey,
} from "../../identity/keys.js";
import { WIRE_VERSION, WireVersions } from "../../identity/wire-version.js";
import { RouterInstanceId } from "../router/api/contract.js";
import {
  type ActionCertifiedRecord,
  type ActionCore,
  type ActionHash,
  type ActionProposal,
  CatchUpPage,
  type CatchUpRequest,
  type CertifiedRecord,
  type CompletedReanchor,
  deriveConversationId,
  deriveEvidenceEnvelopeId,
  DirectPacket,
  EvidenceStatement,
  hashAction,
  hashAnchor,
  hashMessageIntent,
  hashRecord,
  type InitialAnchorBody,
  maximumContentBytes,
  maximumMembers,
  MembershipDescriptor as MembershipDescriptorSchema,
  type MessageIntent,
  mintMsgId,
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
} from "./index.js";
import { Content, type Content as ContentValue } from "./values.js";

const maximumIdentityBodyBytes = 262_144;
const maximumIdentityRecipients = 128;

interface IdentityFixture {
  readonly card: VerifiedAgentCard;
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
    const registrySignerPublicKey = yield* Schema.decodeUnknown(
      Ed25519PublicKey,
    )(registryKeys.publicKey.export({ format: "jwk" }));
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
      ({ card }) => Schema.encode(AgentCard)(card),
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
        agentCard: card,
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
        agentCard: card,
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
          agentCard: card,
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
      agentCard: responder.card,
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
        agentCard: responder.card,
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
    agentCard: responder.card,
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
      agentCard: responder.card,
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
      agentCard: responder.card,
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
      agentCard: responder.card,
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
      agentCard: signer.card,
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
      agentCard: signer.card,
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
      agentCard: signer.card,
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
  it(
    "rejects caller statements that differ from the signed evidence body",
    rejectsSubstitutedEvidenceStatements,
  );
  it.each([
    [2, 2],
    [3, 3],
    [4, 3],
    [10, 7],
  ] as const)(
    "accepts q(%i)=%i durability evidence and rejects one fewer vote",
    ([memberCount, threshold]) => verifiesThreshold(memberCount, threshold),
  );
  it(
    "requires unanimous FIRST_MESSAGE and author-inclusive MESSAGE evidence without hashing either certificate",
    enforcesFirstMessageAndMessageEvidence,
  );
  it(
    "verifies a record only against the membership it names, held from FIRST_MESSAGE",
    verifiesRecordsAgainstTheHeldMembership,
  );
  it(
    "requires the proposal envelope sender to be the message author",
    verifiesProposalEnvelopeAttribution,
  );
  it(
    "binds re-anchor and catch-up evidence to the exact position and responder",
    verifiesReanchorCatchUpBindings,
  );
  it(
    "catches up FIRST_MESSAGE first, so a later MESSAGE page resolves its membership",
    catchesUpFirstMessageFirst,
  );
  describe(
    "the largest catch-up page, a re-anchored MESSAGE at its maximum membership",
    { timeout: MAXIMUM_PAGE_TIMEOUT_MS },
    () => {
      it("verifies from its responder", verifiesMaximumPage);
      it(
        "verifies a history-only item inside a live catch-up page",
        verifiesHistoryOnlyCatchUpItem,
      );
      it(
        "fits inside Identity limits sealed to every member",
        sealsMaximumPageInsideIdentityLimits,
      );
      it(
        "relays its attestation inside Identity limits sealed to every member",
        relaysMaximumAttestationInsideIdentityLimits,
      );
    },
  );
});
