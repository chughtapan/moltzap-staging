/** @file Registry-owned immutable AgentIdentityCard issuance. */

import {
  type AgentId,
  AgentIdentityCard,
  type AgentName,
  type AgentPublicKey,
  AgentSigningAuthority,
  type PrincipalId,
  type VerifiedAgentIdentityCard,
} from "@moltzap/social-harness";
import {
  agentIdentityCardSigning,
  type AgentIdentityCardVerificationError,
  wireVersions,
} from "@moltzap/social-harness/internal";
import { Effect } from "effect";

/** Whole-second UTC issuance evidence carried by an AgentIdentityCard. */
// eslint-disable-next-line @typescript-eslint/naming-convention -- Effect Schemas share the package-owned domain name they decode.
export const AgentIdentityCardIssuedAt = agentIdentityCardSigning.issuedAt;
/** Validated nominal value decoded by AgentIdentityCardIssuedAt. */
// eslint-disable-next-line @typescript-eslint/no-redeclare -- The same-named Schema and type form one package boundary model.
export type AgentIdentityCardIssuedAt = typeof AgentIdentityCardIssuedAt.Type;

interface IssueAgentIdentityCardInput {
  readonly agentId: AgentId;
  readonly principalId: PrincipalId;
  readonly agentName: AgentName;
  readonly publicKey: AgentPublicKey;
  readonly issuedAt: AgentIdentityCardIssuedAt;
  readonly registrySigningAuthority: AgentSigningAuthority;
}

/**
 * Issues one exact AgentIdentityCard for Registry storage.
 *
 * @param input Complete identity fields and Registry signing authority.
 * @returns A Registry-verified immutable AgentIdentityCard.
 */
export const issueAgentIdentityCard = (
  input: IssueAgentIdentityCardInput,
): Effect.Effect<
  VerifiedAgentIdentityCard,
  AgentIdentityCardVerificationError
> =>
  Effect.gen(function* () {
    const card = yield* agentIdentityCardSigning.sign(
      {
        kind: "agentCard",
        sharnessVersion: yield* wireVersions.emitted,
        agentId: input.agentId,
        principalId: input.principalId,
        agentName: input.agentName,
        publicKey: input.publicKey,
        issuedAt: input.issuedAt,
      },
      input.registrySigningAuthority,
    );
    return yield* AgentIdentityCard.verify({
      identityCard: card,
      registrySignerPublicKey: AgentSigningAuthority.publicKey(
        input.registrySigningAuthority,
      ),
    });
  }).pipe(Effect.withSpan("issueAgentIdentityCard"));
