/** @file Complete Registry-owned client, request, and representation contract. */

import { Effect, type Redacted, Schema } from "effect";
import {
  decodeCanonicalJson,
  encodeCanonicalJson,
} from "../../encoding/canonical-json.js";
import { exactParseOptions, exactStruct } from "../../encoding/exact.js";
import { canonicalIdentifier } from "../../encoding/identifier.js";
import {
  AgentId,
  AgentIdentityCard,
  AgentName,
  PrincipalId,
  type VerifiedAgentIdentityCard,
  verifiedAgentIdentityCardSchema,
} from "../agent.js";
import { AgentPublicKey, type AgentSigningAuthority } from "../keys.js";

/** Idempotency identity for a registration operation. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const RegistrationId = canonicalIdentifier("RegistrationId", "opn_", 16);
/** Validated nominal value decoded by RegistrationId. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type RegistrationId = typeof RegistrationId.Type;

/** Closed Registry bootstrap registration request. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const RegistryRegisterRequest = exactStruct({
  operationId: RegistrationId,
  principalId: PrincipalId,
  agentName: AgentName,
  publicKey: AgentPublicKey,
}).annotations({ identifier: "RegistryRegisterRequest" });
/** Validated Registry bootstrap registration request. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type RegistryRegisterRequest = typeof RegistryRegisterRequest.Type;

/** Closed Registry lookup selector. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the public domain name they decode.
export const RegistryLookupRequest = Schema.Union(
  exactStruct({
    agentId: Schema.propertySignature(AgentId).annotations({
      description:
        "Look up this Registry-minted agent identity (agt_ plus canonical base64url for 16 bytes); use alone.",
    }),
  }),
  exactStruct({
    agentName: Schema.propertySignature(AgentName).annotations({
      description:
        "Look up this immutable Registry-wide agent handle; use alone.",
    }),
  }),
).annotations({
  identifier: "RegistryLookupRequest",
  parseOptions: exactParseOptions,
});
/** Validated Registry lookup selector. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one boundary model.
export type RegistryLookupRequest = typeof RegistryLookupRequest.Type;

/** Name, key and idempotency conflicts are results; authentication and transport failures remain errors. */
export type RegistryRegisterResult =
  | Readonly<{ kind: "registered"; identityCard: VerifiedAgentIdentityCard }>
  | Readonly<{ kind: "name_taken" }>
  | Readonly<{ kind: "key_already_registered" }>
  | Readonly<{ kind: "idempotency_conflict" }>;

/** A found card is Registry-verified. A missing identity is a normal result. */
export type RegistryLookupResult =
  | Readonly<{ kind: "found"; identityCard: VerifiedAgentIdentityCard }>
  | Readonly<{ kind: "not_found" }>;

/** The admission credential authorizes registration, and the signing authority proves possession of the agent key. */
export type RegistryRegisterCall = Readonly<{
  request: RegistryRegisterRequest;
  admissionCredential: Redacted.Redacted;
  signingAuthority: AgentSigningAuthority;
}>;

/** Validated registration outcomes shared through registryContract. */
const registerResultSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("registered"),
    agentCard: verifiedAgentIdentityCardSchema,
  }),
  exactStruct({ kind: Schema.Literal("name_taken") }),
  exactStruct({ kind: Schema.Literal("key_already_registered") }),
  exactStruct({ kind: Schema.Literal("idempotency_conflict") }),
);

/** Validated lookup outcomes shared through registryContract. */
const lookupResultSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("found"),
    agentCard: verifiedAgentIdentityCardSchema,
  }),
  exactStruct({ kind: Schema.Literal("not_found") }),
);

/** Exact registration response representation used by both HTTP adapters. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private HTTP and storage adapters share this Schema; the package root does not export it.
export const registerResponseSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("registered"),
    agentCard: AgentIdentityCard,
  }),
  exactStruct({ kind: Schema.Literal("name_taken") }),
  exactStruct({ kind: Schema.Literal("key_already_registered") }),
  exactStruct({ kind: Schema.Literal("idempotency_conflict") }),
);

/** Exact lookup response representation used by both HTTP adapters. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Private HTTP adapters share this Schema; the package root does not export it.
export const lookupResponseSchema = Schema.Union(
  exactStruct({
    kind: Schema.Literal("found"),
    agentCard: AgentIdentityCard,
  }),
  exactStruct({ kind: Schema.Literal("not_found") }),
);

/** Exact bootstrap registration HTTP body. */
const registrationBody = exactStruct({
  request: RegistryRegisterRequest,
});

/** Exact bootstrap registration request path. */
export const REGISTER_PATH = "/v1/identities:register";
/** Exact public lookup request path. */
export const LOOKUP_PATH = "/v1/identities:lookup";
/** Exact Registry readiness path. */
const HEALTH_PATH = "/healthz";

/** Effect Router patterns escape the literal colon in Registry paths. */
const REGISTER_ROUTE = "/v1/identities::register";
/** Effect Router pattern for the literal lookup path. */
const LOOKUP_ROUTE = "/v1/identities::lookup";
/** Exact paths used to classify method mismatches at the fallback route. */
const KNOWN_PATHS = new Set([REGISTER_PATH, LOOKUP_PATH, HEALTH_PATH]);

const maximumIdentifier = "agt_AAAAAAAAAAAAAAAAAAAAAA";
const maximumOperationId = "opn_AAAAAAAAAAAAAAAAAAAAAA";
const maximumPrincipalId = "prn_AAAAAAAAAAAAAAAAAAAAAA";
const maximumName = "a".repeat(32);
const maximumCoordinate = "A".repeat(43);
const byteLength = (value: unknown): number =>
  new TextEncoder().encode(JSON.stringify(value)).byteLength;

/** Derived exact maximum bootstrap registration body size. */
const REGISTER_BODY_CAP = byteLength({
  request: {
    operationId: maximumOperationId,
    principalId: maximumPrincipalId,
    agentName: maximumName,
    publicKey: {
      crv: "Ed25519",
      kty: "OKP",
      x: maximumCoordinate,
    },
  },
});

/** Derived exact maximum public lookup body size. */
const LOOKUP_BODY_CAP = Math.max(
  byteLength({ agentId: maximumIdentifier }),
  byteLength({ agentName: maximumName }),
);

/** Registration body, durable operation identity, and response share exact codecs. */
export const registryRegisterCodec = {
  request: { encode: canonicalCodec(RegistryRegisterRequest).encode },
  body: { encode: canonicalCodec(registrationBody).encode },
  response: canonicalCodec(registerResponseSchema),
};

/** Public lookup bytes use the same closed representations on both HTTP sides. */
export const registryLookupCodec = {
  request: canonicalCodec(RegistryLookupRequest),
  response: canonicalCodec(lookupResponseSchema),
};

/** Stored cards retain their complete canonical signed representation. */
const registryIdentityCardCodec = canonicalCodec(AgentIdentityCard);

function canonicalCodec<A, I>(schema: Schema.Schema<A, I>) {
  return {
    encode: (value: A) =>
      Schema.encode(schema)(value).pipe(Effect.flatMap(encodeCanonicalJson)),
    decode: (bytes: Uint8Array) => decodeCanonicalJson(schema, bytes),
  };
}

/** Registry HTTP adapters and persistence share these exact routes, bounds, request and result schemas, and canonical codecs. */
export const registryContract = Object.freeze({
  HEALTH_PATH,
  KNOWN_PATHS,
  LOOKUP_BODY_CAP,
  LOOKUP_PATH,
  LOOKUP_ROUTE,
  lookupResultSchema,
  RegistrationId,
  REGISTER_BODY_CAP,
  REGISTER_PATH,
  REGISTER_ROUTE,
  registerResultSchema,
  registrationBody,
  registryIdentityCardCodec,
  registryLookupCodec,
  RegistryLookupRequest,
  registryRegisterCodec,
  RegistryRegisterRequest,
});
