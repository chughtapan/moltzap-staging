/** @file Starts the per-agent services and serves their MCP interface. */

import { NodeFileSystem } from "@effect/platform-node";
import { Cause, Console, Data, Effect, FiberSet, Option } from "effect";
import type { LoadedMessagePolicy } from "../firewall/rules/config.js";
import type { FirewallOptions } from "../firewall/service.js";
import type { DaemonBootstrap } from "./config.js";
import { Communication, CommunicationError } from "../communication/index.js";
import { Identity } from "../identity/service.js";
import { WIRE_VERSION } from "../identity/wire-version.js";
import { packageVersion } from "../release.js";
import { LOOPBACK_HOST, makeMcpHttpApp, serveMcp } from "./mcp/index.js";
import { makeHistoryExport, noHistoryExport } from "./message-log.js";
import { SocialHarness } from "./service.js";

// safer-arch-ignore no-trivial-sink-file: Process supervision owns startup, serving and scope failure independently of configuration parsing.

/** Startup exposes only closed local failure categories. */
export class DaemonRuntimeError extends Data.TaggedError("DaemonRuntimeError")<{
  readonly phase: "storage" | "listener" | "registration";
  readonly reason?:
    | "outbox-too-old"
    | "agent-not-admitted"
    | "admission-refused"
    | "name_taken"
    | "key_already_registered"
    | "idempotency_conflict"
    | "dependency-unavailable"
    | "persistence-failed"
    | "incompatible-daemon";
}> {}

/** The process scope owns message processing, registration and the loopback listener. */
export const runDaemonRuntime = Effect.fn("runDaemonRuntime")(function* (
  bootstrap: DaemonBootstrap,
  source?: FirewallOptions | LoadedMessagePolicy,
) {
  const workers = yield* FiberSet.make();
  const policy =
    source !== undefined && "policy" in source ? source.policy : source;
  const historyExport =
    bootstrap.configuration.historyExport === undefined
      ? noHistoryExport
      : yield* makeHistoryExport(bootstrap.configuration.historyExport).pipe(
          Effect.provide(NodeFileSystem.layer),
        );
  const harness = SocialHarness.layer(
    {
      historyExport,
      communication: (identityCard) =>
        Communication.layer({
          identityCard,
          signingAuthority: bootstrap.signingAuthority,
          registrySignerPublicKey:
            bootstrap.configuration.registrySignerPublicKey,
          maximumHistoryRecords: policy?.maximumHistoryRecords,
        }),
      maximumHistoryRecords: policy?.maximumHistoryRecords,
      maximumConcurrentChecks: policy?.maximumConcurrentChecks,
      hideGroups: bootstrap.configuration.hideGroups,
      hideCollectives: bootstrap.configuration.hideCollectives,
    },
    workers,
  );
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const service = yield* SocialHarness;
      const identity = yield* Identity;
      if (
        (yield* identity.me()).kind === "unregistered" &&
        bootstrap.configuration.registration !== undefined
      ) {
        yield* service.register(bootstrap.configuration.registration).pipe(
          Effect.catchTag("RegistrationError", (error) =>
            Effect.fail(
              new DaemonRuntimeError({
                phase: "registration",
                reason: error.reason,
              }),
            ),
          ),
        );
      }
      const app = yield* makeMcpHttpApp({
        implementation: { name: "sharnessd", version: packageVersion },
        credential: bootstrap.mcpCredential,
      });
      yield* serveMcp(bootstrap.configuration.mcpPort, app).pipe(
        Effect.catchTag("ServeError", () =>
          Effect.fail(new DaemonRuntimeError({ phase: "listener" })),
        ),
      );
      yield* Console.log(
        `sharnessd listening on http://${LOOPBACK_HOST}:${bootstrap.configuration.mcpPort}/mcp (package ${packageVersion}; WIRE_VERSION ${WIRE_VERSION})`,
      );
      return yield* Effect.never;
    }),
  ).pipe(
    Effect.provide(harness),
    Effect.raceFirst(FiberSet.join(workers)),
    Effect.catchAllCause(stopRuntime),
  );
}, Effect.withSpan("runDaemonRuntime"));

function stopRuntime(
  cause: Cause.Cause<unknown>,
): Effect.Effect<never, DaemonRuntimeError> {
  if (Cause.isInterruptedOnly(cause)) {
    return Effect.failCause(Cause.stripFailures(cause));
  }
  const failure = Option.getOrUndefined(Cause.failureOption(cause));
  let error = new DaemonRuntimeError({ phase: "storage" });
  if (failure instanceof DaemonRuntimeError) {
    error = failure;
  } else if (
    failure instanceof CommunicationError &&
    (failure.reason === "outbox-too-old" ||
      failure.reason === "agent-not-admitted")
  ) {
    error = new DaemonRuntimeError({
      phase: "storage",
      reason: failure.reason,
    });
  }
  return Effect.logError("Daemon stopped", cause).pipe(
    Effect.zipRight(Effect.fail(error)),
  );
}
