/** @file Retains one scoped harness and MCP application for each durable agent. */
import { type HttpApp, HttpBody, HttpServerResponse } from "@effect/platform";
import { SqlClient } from "@effect/sql";
import {
  Cause,
  Context,
  Data,
  Deferred,
  Duration,
  Effect,
  Exit,
  FiberSet,
  Layer,
  Match,
  type Redacted,
  type Scope,
  ScopedCache,
  Stream,
} from "effect";
import type { DaemonSettings } from "./config.js";
import { Communication } from "../communication/service.js";
import { Firewall } from "../firewall/service.js";
import { Identity } from "../identity/service.js";
import { packageVersion } from "../version.js";
import { makeAgentMcpApp } from "./mcp/server.js";
import {
  getOrCreateOwnedAgent,
  listOwnedAgentKeys,
  loadAgentRegistration,
  type OwnerAccount,
} from "./ownership.js";
import { SocialHarness } from "./social-harness.js";
import { type HistoryExport, makeHistoryExport } from "./traffic-export.js";

/**
 * A failed agent cannot stop unrelated contexts or disclose its state through HTTP.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class RunningAgentError extends Data.TaggedError("RunningAgentError") {}

interface RunningAgent {
  readonly app: HttpApp.Default<never, Scope.Scope>;
  readonly stopped: Deferred.Deferred<undefined>;
}

type RunningAgentFailure =
  | RunningAgentError
  | Effect.Effect.Error<ReturnType<typeof makeRunningAgent>>
  | Effect.Effect.Error<ReturnType<typeof getOrCreateOwnedAgent>>;

function liveApp(agent: RunningAgent) {
  return Deferred.isDone(agent.stopped).pipe(
    Effect.flatMap((stopped) =>
      stopped
        ? Effect.fail(new RunningAgentError())
        : Effect.succeed(agent.app),
    ),
  );
}

/** Context lifetimes belong to the daemon, independent of MCP connection lifetimes. */
export class RunningHarnesses extends Context.Tag(
  "SocialHarness/RunningHarnesses",
)<
  RunningHarnesses,
  {
    readonly get: (
      owner: OwnerAccount,
    ) => Effect.Effect<
      HttpApp.Default<never, Scope.Scope>,
      RunningAgentFailure,
      Scope.Scope
    >;
  }
>() {
  /** Successful acquisitions stay cached; a failed acquisition may be retried on the next connection. */
  static readonly layer = (
    settings: DaemonSettings,
    unlockKey: Redacted.Redacted<Uint8Array>,
  ) =>
    Layer.scoped(
      RunningHarnesses,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const historyExport =
          settings.historyExport === undefined
            ? undefined
            : yield* makeHistoryExport(settings.historyExport);
        const cache = yield* ScopedCache.makeWith({
          capacity: Number.MAX_SAFE_INTEGER,
          lookup: (agentKey: string) =>
            makeRunningAgent(settings, agentKey, unlockKey, historyExport).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
          timeToLive: (exit) =>
            Exit.isSuccess(exit) ? Duration.infinity : Duration.zero,
        });
        yield* Effect.forEach(
          yield* listOwnedAgentKeys,
          (agentKey) =>
            cache.get(agentKey).pipe(
              Effect.catchAll((error) =>
                Effect.logError("Agent restoration failed").pipe(
                  Effect.annotateLogs({
                    agentKey,
                    failure: "_tag" in error ? error._tag : "ProtocolError",
                  }),
                ),
              ),
            ),
          { concurrency: 4, discard: true },
        ).pipe(Effect.forkScoped);
        return RunningHarnesses.of({
          get: (owner) =>
            getOrCreateOwnedAgent(owner, unlockKey).pipe(
              Effect.flatMap((agentKey) => cache.get(agentKey)),
              Effect.flatMap(liveApp),
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
        });
      }),
    );
}

function makeRunningAgent(
  settings: DaemonSettings,
  agentKey: string,
  unlockKey: Redacted.Redacted<Uint8Array>,
  historyExport?: HistoryExport,
) {
  return Effect.gen(function* () {
    const registration = yield* loadAgentRegistration(agentKey, unlockKey);
    const workers = yield* FiberSet.make();
    const stopped = yield* Deferred.make<undefined>();
    const identity = Identity.layer({
      agentKey,
      signingAuthority: registration.signingAuthority,
      registrySignerPublicKey: settings.registrySignerPublicKey,
      admissionCredential:
        settings.admissionCredential === undefined
          ? Effect.fail(new RunningAgentError())
          : Effect.succeed(settings.admissionCredential),
    });
    const policy = settings.messagePolicy?.policy;
    const harness = SocialHarness.layer(
      {
        agentKey,
        historyExport,
        communication: (identityCard) =>
          Communication.layer({
            agentKey,
            identityCard,
            signingAuthority: registration.signingAuthority,
            registrySignerPublicKey: settings.registrySignerPublicKey,
            maximumHistoryRecords: policy?.maximumHistoryRecords,
          }),
        maximumHistoryRecords: policy?.maximumHistoryRecords,
        maximumConcurrentChecks: policy?.maximumConcurrentChecks,
        hideGroups: settings.hideGroups,
        hideCollectives: settings.hideCollectives,
      },
      workers,
    ).pipe(
      Layer.provideMerge(
        Firewall.layer(agentKey, settings.messagePolicy).pipe(
          Layer.provideMerge(identity),
        ),
      ),
    );
    const services = yield* Layer.build(harness);
    const service = Context.get(services, SocialHarness);
    const identityService = Context.get(services, Identity);
    if ((yield* identityService.me()).kind === "unregistered") {
      yield* service.register(registration.registration);
    }
    const app = yield* makeAgentMcpApp({
      agentKey,
      unlockKey,
      implementation: { name: "sharnessd", version: packageVersion },
    }).pipe(Effect.provide(services));
    yield* FiberSet.join(workers).pipe(
      Effect.matchCauseEffect({
        onFailure: (cause) =>
          Cause.isInterruptedOnly(cause)
            ? Effect.void
            : Effect.logError("Agent worker stopped").pipe(
                Effect.annotateLogs({ agentKey }),
              ),
        onSuccess: () => Effect.void,
      }),
      Effect.ensuring(Deferred.succeed(stopped, undefined)),
      Effect.forkScoped,
    );
    return {
      app: app.pipe(
        Effect.raceFirst(
          Deferred.await(stopped).pipe(
            Effect.interruptible,
            Effect.as(
              HttpServerResponse.text("Agent unavailable", { status: 503 }),
            ),
          ),
        ),
        Effect.flatMap((response) => responseWhileRunning(response, stopped)),
      ),
      stopped,
    } satisfies RunningAgent;
  }).pipe(Effect.withSpan("makeRunningAgent"));
}

/** Streams retain the worker-lifetime signal after the HTTP app returns its response. */
function responseWhileRunning(
  response: HttpServerResponse.HttpServerResponse,
  stopped: Deferred.Deferred<undefined>,
) {
  return Match.value(response.body).pipe(
    Match.tag("Stream", (body) =>
      HttpServerResponse.setBody(
        response,
        HttpBody.stream(
          body.stream.pipe(Stream.interruptWhen(Deferred.await(stopped))),
          body.contentType,
        ),
      ),
    ),
    Match.orElse(() => response),
  );
}
