/** @file Envelope operations preserve verification requirements and exact Effect channels. */

import type { Effect, Option, Types } from "effect";
import type {
  AgentId,
  AgentSigningAuthority,
  VerifiedAgentIdentityCard,
} from "./agent-cards.js";
import type {
  EnvelopeId,
  SealedBody,
  SealedBodyOpeningError,
  SealedBodySealingError,
  SignedMessage,
  VerifiedSignedMessage,
} from "./message-security.js";

type Expect<Value extends true> = Value;

type Seal = typeof SealedBody.seal;
type SealResult = ReturnType<Seal>;
type Open = typeof SealedBody.open;
type OpenResult = ReturnType<Open>;

type SealInputIsExact = Expect<
  Types.Equals<
    Parameters<Seal>[0],
    {
      readonly senderAgentId: AgentId;
      readonly recipientIdentityCards: readonly VerifiedAgentIdentityCard[];
      readonly envelopeId: EnvelopeId;
      readonly plaintext: Uint8Array;
    }
  >
>;
type SealSuccessIsBytes = Expect<
  Types.Equals<Effect.Effect.Success<SealResult>, Uint8Array>
>;
type SealFailureIsExact = Expect<
  Types.Equals<Effect.Effect.Error<SealResult>, SealedBodySealingError>
>;
type SealNeedsNoContext = Expect<
  Types.Equals<Effect.Effect.Context<SealResult>, never>
>;
type OpenInputIsExact = Expect<
  Types.Equals<
    Parameters<Open>[0],
    {
      readonly identityCard: VerifiedAgentIdentityCard;
      readonly signingAuthority: AgentSigningAuthority;
      readonly signedMessage: VerifiedSignedMessage;
    }
  >
>;
type OpenRefusesUnverifiedMessages = Expect<
  Types.Equals<
    SignedMessage extends Parameters<Open>[0]["signedMessage"] ? true : false,
    false
  >
>;
type OpenSuccessIsBytes = Expect<
  Types.Equals<Effect.Effect.Success<OpenResult>, Uint8Array>
>;
type OpenFailureIsExact = Expect<
  Types.Equals<Effect.Effect.Error<OpenResult>, SealedBodyOpeningError>
>;
type OpenNeedsNoContext = Expect<
  Types.Equals<Effect.Effect.Context<OpenResult>, never>
>;
type SealedByteLengthIsExact = Expect<
  Types.Equals<
    typeof SealedBody.sealedByteLength,
    (input: {
      readonly plaintextByteLength: number;
      readonly recipientCount: number;
    }) => Option.Option<number>
  >
>;
type MaximumPlaintextByteLengthIsExact = Expect<
  Types.Equals<
    typeof SealedBody.maximumPlaintextByteLength,
    (recipientCount: number) => Option.Option<number>
  >
>;
type CapabilityIsExact = Expect<
  Types.Equals<
    keyof typeof SealedBody,
    "maximumPlaintextByteLength" | "open" | "seal" | "sealedByteLength"
  >
>;

/** Compile-time evidence for the sealed-body public contract. */
export type SealedBodyCanaries = [
  SealInputIsExact,
  SealSuccessIsBytes,
  SealFailureIsExact,
  SealNeedsNoContext,
  OpenInputIsExact,
  OpenRefusesUnverifiedMessages,
  OpenSuccessIsBytes,
  OpenFailureIsExact,
  OpenNeedsNoContext,
  SealedByteLengthIsExact,
  MaximumPlaintextByteLengthIsExact,
  CapabilityIsExact,
];
