/** @file Real Registry-signed identities for HTTP compatibility tests. */
import { Effect, Encoding, Redacted, Schema } from "effect";
import { GeneralSign } from "jose";
import { generateKeyPairSync } from "node:crypto";
import {
  AgentIdentityCardIssuedAt,
  makeVerifiedCard,
} from "../../__tests__/agent-card-fixtures.js";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
} from "../../../encoding/canonical-json.js";
import {
  AgentId,
  AgentIdentityCard,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
} from "../../agent.js";
import {
  AgentSigningAuthority,
  agentSigningPrivateKey,
  ed25519PublicKeyThumbprintUri,
} from "../../keys.js";
import { AuthenticationFailedError } from "../proof.js";

/** Independent Registry and agent signing keys bound by the verified card. */
export interface IdentityFixture {
  readonly registryAuthority: AgentSigningAuthority;
  readonly identityCard: VerifiedAgentIdentityCard;
  readonly signingAuthority: AgentSigningAuthority;
}

const identifier = (prefix: string, byte: number): string =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`;
const makeAgentId = (byte: number) =>
  Schema.decodeUnknownSync(AgentId)(identifier("agt_", byte));

const makeSigningAuthority = () => {
  const { privateKey } = generateKeyPairSync("ed25519");
  return AgentSigningAuthority.fromPkcs8(
    Redacted.make(privateKey.export({ format: "pem", type: "pkcs8" })),
  );
};

/** Current-version card and both signing authorities for authentic requests. */
export const makeIdentityFixture = Effect.gen(function* () {
  const registryAuthority = yield* makeSigningAuthority();
  const signingAuthority = yield* makeSigningAuthority();
  const identityCard = yield* makeVerifiedCard({
    agentId: makeAgentId(1),
    principalId: Schema.decodeUnknownSync(PrincipalId)(identifier("prn_", 2)),
    agentName: Schema.decodeUnknownSync(AgentName)("http-signer"),
    publicKey: AgentSigningAuthority.publicKey(signingAuthority),
    issuedAt: Schema.decodeUnknownSync(AgentIdentityCardIssuedAt)(
      "2026-07-30T12:00:00Z",
    ),
    registrySigningAuthority: registryAuthority,
  });
  return {
    identityCard,
    signingAuthority,
    registryAuthority,
  } satisfies IdentityFixture;
}).pipe(Effect.withSpan("makeIdentityFixture"));

/** The Registry attests a history-only card without changing the agent key. */
export const retainedIdentityFixture = Effect.gen(function* () {
  const fixture = yield* makeIdentityFixture;
  const encoded = yield* Schema.encode(AgentIdentityCard)(fixture.identityCard);
  const representation = yield* Schema.decodeUnknown(
    Schema.Struct({ payload: Schema.String }),
  )(encoded);
  const bytes = yield* Encoding.decodeBase64Url(representation.payload);
  const fields = yield* decodeCanonicalJson(
    Schema.Record({ key: Schema.String, value: Schema.Unknown }),
    bytes,
  );
  const payload = yield* encodeCanonicalJson({
    ...fields,
    sharnessVersion: "2099.101.0",
  });
  const registrySignerPublicKey = AgentSigningAuthority.publicKey(
    fixture.registryAuthority,
  );
  const kid = yield* ed25519PublicKeyThumbprintUri(registrySignerPublicKey);
  const signed = yield* Effect.tryPromise({
    try: () =>
      new GeneralSign(payload)
        .addSignature(agentSigningPrivateKey(fixture.registryAuthority))
        .setProtectedHeader({
          alg: "Ed25519",
          kid,
          typ: "application/vnd.sharness.agent-card+jws",
        })
        .sign(),
    catch: () => new AuthenticationFailedError(),
  });
  const identityCard = yield* Schema.decodeUnknown(AgentIdentityCard)(
    signed,
  ).pipe(
    Effect.flatMap((identityCard) =>
      AgentIdentityCard.verify({ identityCard, registrySignerPublicKey }),
    ),
  );
  return { ...fixture, identityCard };
}).pipe(Effect.withSpan("retainedIdentityFixture"));
