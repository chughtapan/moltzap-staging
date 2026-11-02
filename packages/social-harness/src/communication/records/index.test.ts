/** @file Small boundary tests for the private addressed Client representation. */

import { it } from "@effect/vitest";
import canonicalize from "canonicalize";
import { Effect, Encoding, Schema } from "effect";
import { generateKeyPairSync } from "node:crypto";
import { describe, expect } from "vitest";
import {
  issueTestCard,
  makeTestAuthority,
  type RegistryKeyPair,
} from "../../__tests__/agent-card-fixtures.js";
import {
  ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
} from "../../encoding/canonical-json.js";
import { AgentId } from "../../identity/agent.js";
import { SealedBody } from "../../identity/envelope/sealed-body.js";
import {
  EnvelopeId,
  SignedMessage,
} from "../../identity/envelope/signed-message.js";
import { WIRE_VERSION } from "../../identity/wire-version.js";
import {
  decodeOuterBody,
  deriveConversationId,
  deriveEvidenceEnvelopeId,
  mintMsgId,
  signOuterPacket,
} from "./encoding.js";
import {
  ActionHash,
  ActionSignatureStatement,
  CatchUpRequest,
  ConversationId,
  MembershipHash,
} from "./schemas.js";
import { Content, maximumContentBytes } from "./values.js";

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

const identifier = (prefix: string, byteLength: number, byte: number): string =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(byteLength).fill(byte))}`;

const firstAgentId = Schema.decodeUnknownSync(AgentId)(
  identifier("agt_", 16, 1),
);
const secondAgentId = Schema.decodeUnknownSync(AgentId)(
  identifier("agt_", 16, 2),
);
const conversationId = Schema.decodeUnknownSync(ConversationId)(
  identifier("cnv_", 32, 3),
);
const membershipHash = Schema.decodeUnknownSync(MembershipHash)(
  identifier("mbr_", 32, 4),
);

const canonicalActionHash = identifier("ach_", 32, 5);

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

const expectRepresentationFailure = <Value>(
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
    yield* expectRepresentationFailure(
      decodeCanonical(CatchUpRequest, nonCanonical),
    );
    yield* expectRepresentationFailure(
      decodeCanonical(
        CatchUpRequest,
        canonicalInputBytes({ ...catchUpRequest, unexpected: true }),
      ),
    );
    const { sharnessVersion, ...unversioned } = catchUpRequest;
    yield* expectRepresentationFailure(
      decodeCanonical(
        CatchUpRequest,
        canonicalInputBytes({
          ...unversioned,
          moltzapVersion: sharnessVersion,
        }),
      ),
    );
    yield* expectRepresentationFailure(
      decodeCanonical(
        CatchUpRequest,
        canonicalInputBytes({
          ...catchUpRequest,
          moltzapVersion: sharnessVersion,
        }),
      ),
    );
  });

const derivesPrivateIdentifiers = () =>
  Effect.gen(function* () {
    const memberAgentIds = [firstAgentId, secondAgentId] as const;
    const firstConversationId = yield* deriveConversationId(memberAgentIds);
    const secondConversationId = yield* deriveConversationId(memberAgentIds);
    expect(firstConversationId).toBe(secondConversationId);
    yield* expectRepresentationFailure(
      deriveConversationId([secondAgentId, firstAgentId] as const),
    );

    const firstMsgId = yield* mintMsgId();
    const secondMsgId = yield* mintMsgId();
    expect(secondMsgId).not.toBe(firstMsgId);
  });

const enforcesContentBounds = () =>
  Effect.gen(function* () {
    const empty = [{ type: "text", text: "" }] as const;
    const fixedBytes = yield* encodeCanonical(Content, empty);
    const maximumText = "x".repeat(maximumContentBytes - fixedBytes.byteLength);
    const maximumContent = [{ type: "text", text: maximumText }] as const;
    expect(yield* encodeCanonical(Content, maximumContent)).toHaveLength(
      maximumContentBytes,
    );
    yield* expectRepresentationFailure(
      encodeCanonical(Content, [{ type: "text", text: `${maximumText}x` }]),
    );
  });

const derivesStableEvidenceIdentity = () =>
  Effect.gen(function* () {
    const statement = Schema.decodeUnknownSync(ActionSignatureStatement)({
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: firstAgentId,
      actionHash: Schema.decodeUnknownSync(ActionHash)(
        identifier("ach_", 32, 5),
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

    yield* expectRepresentationFailure(
      decodeCanonical(CatchUpRequest, sealed.body),
    );
    expect(
      yield* decodeOuterBody({ ...receiver, message: sealed }),
    ).toStrictEqual({ kind: "direct", packet: catchUpRequest });
    expect(
      yield* decodeOuterBody({ ...sender, message: sealed }),
    ).toStrictEqual({ kind: "direct", packet: catchUpRequest });
    yield* expectRepresentationFailure(
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
        identifier("env_", 16, 6),
      ),
      body: yield* encodeCanonical(CatchUpRequest, catchUpRequest),
    });

    yield* expectRepresentationFailure(
      decodeOuterBody({ ...receiver, message: plaintext }),
    );
  });

const refusesSealedBodyHoldingNoClientValue = () =>
  Effect.gen(function* () {
    const registryKeys = generateKeyPairSync("ed25519");
    const sender = yield* makeMember(1, registryKeys);
    const receiver = yield* makeMember(2, registryKeys);
    const envelopeId = Schema.decodeUnknownSync(EnvelopeId)(
      identifier("env_", 16, 7),
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

    yield* expectRepresentationFailure(
      decodeOuterBody({ ...receiver, message }),
    );
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
    "enforces the exact canonical content byte bound",
    enforcesContentBounds,
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
      candidate: identifier("rch_", 32, 5),
    },
    { form: "31 bytes", candidate: identifier("ach_", 31, 5) },
    {
      form: "non-zero base64url padding bits",
      candidate: `${canonicalActionHash.slice(0, -1)}V`,
    },
  ])("rejects a hash identifier with $form", ({ candidate }) => {
    expect(Schema.is(ActionHash)(candidate)).toBe(false);
  });
});
