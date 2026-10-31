/** @file Registration and saved identity status for the daemon's management interface. */

import { type Context, Data, Effect, Option, Ref, Schema } from "effect";
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
import { Identity, LocalIdentity } from "../identity/local.js";

class DaemonManagementError extends Data.TaggedError("DaemonManagementError")<{
  readonly reason:
    | "admission-refused"
    | "dependency-unavailable"
    | "incompatible-daemon"
    | "not-registered"
    | "persistence-failed";
}> {}

interface ManagementInput {
  readonly identity: Context.Tag.Service<typeof Identity>;
  readonly current: Context.Tag.Service<typeof LocalIdentity>;
}

/** Registration and status remain available while communication recovers. */
export type DaemonManagementOperations = Pick<
  HarnessMcpOperations,
  "readStatus" | "register"
>;

/** Captures the identity service without retaining protocol or history dependencies. */
export const makeDaemonManagementOperations = () =>
  Effect.gen(function* () {
    const input: ManagementInput = {
      identity: yield* Identity,
      current: yield* LocalIdentity,
    };
    return {
      readStatus: readStatusOperation(input.current),
      register: registerOperation(input),
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
    Effect.mapError(incompatibleDaemon),
  );
}

function readActiveCard(
  current: Context.Tag.Service<typeof LocalIdentity>,
): Effect.Effect<VerifiedAgentCard, DaemonManagementError> {
  return Ref.get(current).pipe(
    Effect.flatMap((card) =>
      Option.isSome(card)
        ? Effect.succeed(card.value)
        : Effect.fail(managementFailure("not-registered")),
    ),
  );
}

function readStatusOperation(
  current: Context.Tag.Service<typeof LocalIdentity>,
): DaemonManagementOperations["readStatus"] {
  return () => Ref.get(current).pipe(Effect.flatMap(encodeStatus));
}

/** Wire adapters preserve Registry refusal outcomes while Identity owns the durable binding. */
function registerOperation(
  input: ManagementInput,
): DaemonManagementOperations["register"] {
  return (request) =>
    input.identity.register(request).pipe(
      Effect.flatMap(() => readActiveCard(input.current)),
      Effect.flatMap((card) =>
        encodeAgentCard(card).pipe(
          Effect.map(
            (agentCard): ManagementRegisterResult => ({
              kind: "registered",
              agentCard,
              agent: agentViewOf(card),
            }),
          ),
        ),
      ),
      Effect.catchTag("RegistrationError", (error) => {
        switch (error.reason) {
          case "name_taken":
            return Effect.succeed<ManagementRegisterResult>({
              kind: "name_taken",
            });
          case "key_already_registered":
            return Effect.succeed<ManagementRegisterResult>({
              kind: "key_already_registered",
            });
          case "idempotency_conflict":
            return Effect.succeed<ManagementRegisterResult>({
              kind: "idempotency_conflict",
            });
          case "admission-refused":
          case "dependency-unavailable":
          case "persistence-failed":
          case "incompatible-daemon":
            return Effect.fail(managementFailure(error.reason));
          default: {
            const exhaustive: never = error.reason;
            return exhaustive;
          }
        }
      }),
    );
}

function incompatibleDaemon(): DaemonManagementError {
  return managementFailure("incompatible-daemon");
}

function managementFailure(
  reason: DaemonManagementError["reason"],
): DaemonManagementError {
  return new DaemonManagementError({ reason });
}
