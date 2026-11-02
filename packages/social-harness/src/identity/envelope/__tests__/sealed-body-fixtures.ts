/**
 * @file Registered members, sealing, signing, and opening for the SealedBody
 * tests.
 */

import { Effect, Encoding, Redacted, Schema } from "effect";
import { generateKeyPairSync } from "node:crypto";
import {
  AgentIdentityCardIssuedAt,
  makeVerifiedCard,
} from "../../__tests__/agent-card-fixtures.js";
import {
  AgentId,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "../../agent.js";
import { type AgentPublicKey, AgentSigningAuthority } from "../../keys.js";
import { SealedBody } from "../sealed-body.js";
import {
  EnvelopeId,
  SignedMessage,
  type VerifiedSignedMessage,
} from "../signed-message.js";

/**
 * Building fixtures dominates these tests: every group member costs an
 * Ed25519 key, two PKCS#8 imports, and an issued AgentIdentityCard, and the largest
 * groups have 129 members and seal about 256 KB to 128 recipients. Loaded CI
 * hosts need more than the default five seconds.
 */
export const KEY_AGREEMENT_HEAVY_TIMEOUT_MS = 60_000;

/** How many members `makeGroup` builds, and tests open as, at once. */
export const FIXTURE_CONCURRENCY = 8;

/** A short text plaintext that the tests seal unless they need other bytes. */
export const plaintext = new TextEncoder().encode("sealed outer body");

/** A registered agent: its verified AgentIdentityCard and its signing authority. */
export interface Member {
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly authority: AgentSigningAuthority;
}

const identifier = (prefix: string, byte: number): string =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`;

const makeAuthority = () =>
  AgentSigningAuthority.fromPkcs8(
    Redacted.make(
      generateKeyPairSync("ed25519").privateKey.export({
        format: "pem",
        type: "pkcs8",
      }),
    ),
  );

/**
 * Issues an AgentIdentityCard whose identifiers all repeat `byte`, so two cards with
 * the same byte share an AgentId.
 *
 * @param registrySigningAuthority Registry that signs the card.
 * @param byte Byte repeated through the AgentId, PrincipalId, and name.
 * @param publicKey Ed25519 key the card carries.
 * @returns The verified card.
 */
export const issueCard = (
  registrySigningAuthority: AgentSigningAuthority,
  byte: number,
  publicKey: AgentPublicKey,
) =>
  makeVerifiedCard({
    agentId: Schema.decodeUnknownSync(AgentId)(identifier("agt_", byte)),
    principalId: Schema.decodeUnknownSync(PrincipalId)(
      identifier("prn_", byte),
    ),
    agentName: Schema.decodeUnknownSync(AgentName)(`member-${byte}`),
    publicKey,
    issuedAt: Schema.decodeUnknownSync(AgentIdentityCardIssuedAt)(
      "2026-10-05T12:00:00Z",
    ),
    registrySigningAuthority,
  });

/**
 * Generates a fresh authority and has the Registry issue its AgentIdentityCard, with
 * `byte` repeated through the card's identifiers.
 */
export const makeMember = (
  registrySigningAuthority: AgentSigningAuthority,
  byte: number,
) =>
  Effect.gen(function* () {
    const authority = yield* makeAuthority();
    const identityCard = yield* issueCard(
      registrySigningAuthority,
      byte,
      AgentSigningAuthority.publicKey(authority),
    );
    const member: Member = { identityCard, authority };
    return member;
  }).pipe(Effect.withSpan("makeMember"));

/**
 * Builds a sender, a peer, an outsider, and a recipient list that starts with
 * the sender, then the peer, then further members up to `recipientCount`.
 * The identifier bytes increase along the list, so it is in canonical order.
 *
 * @param recipientCount Number of recipients, sender included.
 * @returns The named members, the recipient list, and the Registry.
 */
export const makeGroup = (recipientCount: number) =>
  Effect.gen(function* () {
    const registrySigningAuthority = yield* makeAuthority();
    const sender = yield* makeMember(registrySigningAuthority, 1);
    const peer = yield* makeMember(registrySigningAuthority, 2);
    const outsider = yield* makeMember(registrySigningAuthority, 255);
    const others = yield* Effect.all(
      Array.from(Array(Math.max(recipientCount - 2, 0)).keys(), (index) =>
        makeMember(registrySigningAuthority, index + 3),
      ),
      { concurrency: FIXTURE_CONCURRENCY },
    );
    const recipients = [sender, peer, ...others].slice(0, recipientCount);
    return { sender, peer, outsider, recipients, registrySigningAuthority };
  }).pipe(Effect.withSpan("makeGroup"));

/** The members and Registry that `makeGroup` builds. */
export type Group = Effect.Effect.Success<ReturnType<typeof makeGroup>>;

/**
 * The EnvelopeId whose 16 bytes all repeat `byte`. `sealFrom` and `signBody`
 * both default to byte 1, so a test that names no EnvelopeId seals and signs
 * under the same one.
 */
export const envelopeIdOf = (byte: number) =>
  Schema.decodeUnknownSync(EnvelopeId)(identifier("env_", byte));

/**
 * Seals `bytes` from `sender` to the recipients' AgentCards.
 *
 * @param sender Sender whose AgentId the header binds.
 * @param recipients Recipients in the order the sender lists their cards.
 * @param bytes Plaintext to seal.
 * @param envelopeIdByte Byte repeated through the EnvelopeId the header binds.
 * @returns The sealed body.
 */
export const sealFrom = (
  sender: Member,
  recipients: readonly Member[],
  bytes: Uint8Array,
  envelopeIdByte = 1,
) =>
  SealedBody.seal({
    senderAgentId: sender.identityCard.agentId,
    recipientIdentityCards: recipients.map((member) => member.identityCard),
    envelopeId: envelopeIdOf(envelopeIdByte),
    plaintext: bytes,
  });

/**
 * Signs `body` from `signer` to the recipients.
 *
 * @param signer Member that signs.
 * @param recipients Recipients the SignedMessage names.
 * @param body Body bytes, sealed or not.
 * @param envelopeIdByte Byte repeated through the EnvelopeId.
 * @returns The verified SignedMessage.
 */
export const signBody = (
  signer: Member,
  recipients: readonly Member[],
  body: Uint8Array,
  envelopeIdByte = 1,
) =>
  SignedMessage.sign({
    identityCard: signer.identityCard,
    signingAuthority: signer.authority,
    recipientAgentIds: new Set(
      recipients.map((member) => member.identityCard.agentId),
    ),
    envelopeId: envelopeIdOf(envelopeIdByte),
    body,
  });

/**
 * Opens `signedMessage` as `member`, with its own card and authority.
 *
 * @param member Local agent.
 * @param signedMessage Verified SignedMessage carrying a sealed body.
 * @returns The plaintext.
 */
export const openAs = (member: Member, signedMessage: VerifiedSignedMessage) =>
  SealedBody.open({
    identityCard: member.identityCard,
    signingAuthority: member.authority,
    signedMessage,
  });
