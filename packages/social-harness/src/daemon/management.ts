/** @file Registration and saved identity status for the daemon's management interface. */

import { Data, Effect, Option, Ref, Schema } from "effect";
import type {
  HarnessMcpOperations,
  ManagementRegisterResult,
  ManagementStatusResult,
} from "./mcp/index.js";
import {
  AgentCard,
  agentViewOf,
  type VerifiedAgentCard,
} from "../identity/cards.js";
import {
  Identity,
  LocalIdentity,
  type RegistrationError,
} from "../identity/local.js";

class DaemonManagementError extends Data.TaggedError("DaemonManagementError")<{
  readonly reason:
    | "admission-refused"
    | "dependency-unavailable"
    | "incompatible-daemon"
    | "not-registered"
    | "persistence-failed";
}> {}

/** Registration and status remain available while communication recovers. */
export type DaemonManagementOperations = Pick<
  HarnessMcpOperations,
  "readStatus" | "register"
>;

/** Captures the identity service without retaining protocol or history dependencies. */
export const makeDaemonManagementOperations = () =>
  Effect.gen(function* () {
    const identity = yield* Identity;
    const current = yield* LocalIdentity;
    return {
      readStatus: () => Ref.get(current).pipe(Effect.flatMap(encodeStatus)),
      register: (request) =>
        Effect.gen(function* () {
          yield* identity.register(request);
          const card = yield* Ref.get(current);
          if (Option.isNone(card)) {
            return yield* new DaemonManagementError({
              reason: "not-registered",
            });
          }
          return {
            kind: "registered" as const,
            agentCard: yield* encodeAgentCard(card.value),
            agent: agentViewOf(card.value),
          };
        }).pipe(Effect.catchTag("RegistrationError", registrationFailure)),
    } satisfies DaemonManagementOperations;
  }).pipe(Effect.withSpan("makeDaemonManagementOperations"));

function encodeStatus(
  card: Option.Option<VerifiedAgentCard>,
): Effect.Effect<ManagementStatusResult, DaemonManagementError> {
  if (Option.isNone(card)) {
    return Effect.succeed({ kind: "unregistered" });
  }
  return encodeAgentCard(card.value).pipe(
    Effect.map((agentCard) => ({
      kind: "active",
      agentCard,
      agent: agentViewOf(card.value),
    })),
  );
}

function encodeAgentCard(
  agentCard: VerifiedAgentCard,
): Effect.Effect<unknown, DaemonManagementError> {
  return Schema.encode(AgentCard)(agentCard).pipe(
    Effect.catchTag("ParseError", () =>
      Effect.fail(new DaemonManagementError({ reason: "incompatible-daemon" })),
    ),
  );
}

/** Wire adapters preserve Registry refusal outcomes while Identity owns the durable binding. */
function registrationFailure(
  error: RegistrationError,
): Effect.Effect<ManagementRegisterResult, DaemonManagementError> {
  switch (error.reason) {
    case "name_taken":
    case "key_already_registered":
    case "idempotency_conflict":
      return Effect.succeed({ kind: error.reason });
    case "admission-refused":
    case "dependency-unavailable":
    case "persistence-failed":
    case "incompatible-daemon":
      return new DaemonManagementError({ reason: error.reason });
    default: {
      const exhaustive: never = error.reason;
      return exhaustive;
    }
  }
}
