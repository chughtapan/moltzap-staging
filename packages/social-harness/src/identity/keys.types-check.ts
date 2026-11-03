/** @file Signing authority exposes opaque capability values and exact Effect channels. */

import type { Effect, Redacted, Types } from "effect";
import type { AgentPublicKey, AgentSigningAuthority } from "./agent-cards.js";
import type { InvalidAgentPrivateKeyError } from "./keys.js";

type Expect<Value extends true> = Value;

type FromPkcs8 = typeof AgentSigningAuthority.fromPkcs8;
type FromPkcs8Result = ReturnType<FromPkcs8>;
type InputIsRedacted = Expect<
  Types.Equals<Parameters<FromPkcs8>[0], Redacted.Redacted>
>;
type ImportSuccessIsAuthority = Expect<
  Types.Equals<Effect.Effect.Success<FromPkcs8Result>, AgentSigningAuthority>
>;
type ImportFailureIsExact = Expect<
  Types.Equals<
    Effect.Effect.Error<FromPkcs8Result>,
    InvalidAgentPrivateKeyError
  >
>;
type ImportNeedsNoContext = Expect<
  Types.Equals<Effect.Effect.Context<FromPkcs8Result>, never>
>;
type PublicKeyIsTotal = Expect<
  Types.Equals<
    ReturnType<typeof AgentSigningAuthority.publicKey>,
    AgentPublicKey
  >
>;
type PublicCapabilityIsExact = Expect<
  Types.Equals<keyof typeof AgentSigningAuthority, "fromPkcs8" | "publicKey">
>;
type AuthorityHasNoStringMembers = Expect<
  Types.Equals<Extract<keyof AgentSigningAuthority, string>, never>
>;

/** Compile-time evidence for the signing authority's public contract. */
export type AgentSigningAuthorityCanaries = [
  InputIsRedacted,
  ImportSuccessIsAuthority,
  ImportFailureIsExact,
  ImportNeedsNoContext,
  PublicKeyIsTotal,
  PublicCapabilityIsExact,
  AuthorityHasNoStringMembers,
];
