/** @file Immutable card fixtures signed through the Identity capability. */

import { Effect } from "effect";
import {
  AgentIdentityCard,
  type AgentIdentityCardClaims,
  agentIdentityCardSigning,
  type AgentIdentityCardVerificationError,
  AgentSigningAuthority,
  type VerifiedAgentIdentityCard,
} from "../agent.js";
import { wireVersions } from "../wire-version.js";

/** Whole-second UTC issuance evidence carried by an AgentIdentityCard. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the package-owned domain name they decode.
export const AgentIdentityCardIssuedAt = agentIdentityCardSigning.issuedAt;
/** Validated nominal value decoded by AgentIdentityCardIssuedAt. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one package boundary model.
export type AgentIdentityCardIssuedAt = typeof AgentIdentityCardIssuedAt.Type;

interface CardFixtureInput
  extends Omit<AgentIdentityCardClaims, "kind" | "sharnessVersion"> {
  readonly registrySigningAuthority: AgentSigningAuthority;
}

/**
 * Signs and verifies immutable card claims for Identity fixtures.
 *
 * @param input Complete identity fields and Registry signing authority.
 * @returns A Registry-verified immutable AgentIdentityCard.
 */
export const makeVerifiedCard = (
  input: CardFixtureInput,
): Effect.Effect<
  VerifiedAgentIdentityCard,
  AgentIdentityCardVerificationError
> =>
  Effect.gen(function* () {
    const { registrySigningAuthority, ...claims } = input;
    const card = yield* agentIdentityCardSigning.sign(
      {
        kind: "agentCard",
        sharnessVersion: yield* wireVersions.emitted,
        ...claims,
      },
      registrySigningAuthority,
    );
    return yield* AgentIdentityCard.verify({
      identityCard: card,
      registrySignerPublicKey: AgentSigningAuthority.publicKey(
        registrySigningAuthority,
      ),
    });
  }).pipe(Effect.withSpan("makeVerifiedCard"));
