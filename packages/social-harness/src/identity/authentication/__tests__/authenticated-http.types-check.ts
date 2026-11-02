/** @file Authentication exposes one verified caller value and a closed failure channel. */
import type { Effect, Types } from "effect";
import type {
  Authentication,
  AuthenticationError,
  VerifiedAgentRequest,
} from "../service.js";
type Expect<Value extends true> = Value;
/** Callers handle only authentication failures and supply only Authentication. */
export type AuthenticationChannels = Expect<
  Types.Equals<
    ReturnType<typeof Authentication.verifyRequest>,
    Effect.Effect<VerifiedAgentRequest, AuthenticationError, Authentication>
  >
>;
