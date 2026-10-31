/** @file Composes messaging services and serves the agent's MCP interface. */

import { NodeFileSystem } from "@effect/platform-node";
import {
  Cause,
  Console,
  Data,
  Effect,
  FiberSet,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import type { DaemonBootstrap } from "./bootstrap.js";
import { Collectives } from "../communication/collectives/index.js";
import { CommunicationError } from "../communication/index.js";
import { communicationLive } from "../communication/service/index.js";
import { DeliveryToken } from "../communication/store/index.js";
import { Firewall } from "../firewall/index.js";
import { Identity, LocalIdentity } from "../identity/local.js";
import { WIRE_VERSION } from "../identity/wire-version.js";
import { packageVersion } from "../release.js";
import {
  makeHistoryExport,
  noHistoryExport,
} from "./inbox/host-traffic-log.js";
import {
  type DaemonManagementOperations,
  makeDaemonManagementOperations,
} from "./management.js";
import {
  acquireHarnessMcpHttpServer,
  LOOPBACK_HOST,
  makeHarnessMcpHttpHandler,
  type ManagementRegisterRequest,
} from "./mcp/index.js";
import {
  HostMessaging,
  hostMessagingLayer,
  McpInbox,
} from "./messaging/index.js";

/** Closed private daemon failure projected onto the public startup phases. */
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

/** Bootstrap is fixed before the service graph opens host calls. */
export interface DaemonStartup {
  readonly bootstrap: DaemonBootstrap;
}

/** Native scopes own workers, and a critical failure ends the process. */
export const runDaemonRuntime = (input: DaemonStartup) =>
  Effect.gen(function* () {
    const workers = yield* FiberSet.make<never>();
    const path = input.bootstrap.configuration.historyExport;
    const historyExport =
      path === undefined
        ? noHistoryExport
        : yield* makeHistoryExport(path).pipe(
            Effect.provide(NodeFileSystem.layer),
          );
    const communication = communicationLive(
      {
        signingAuthority: input.bootstrap.signingAuthority,
        registrySignerPublicKey:
          input.bootstrap.configuration.registrySignerPublicKey,
      },
      workers,
    );
    const collectives = Collectives.layer.pipe(
      Layer.provideMerge(communication),
    );
    const messaging = hostMessagingLayer(
      {
        historyExport,
        experiments: input.bootstrap.configuration,
        registrySignerPublicKey:
          input.bootstrap.configuration.registrySignerPublicKey,
      },
      workers,
    ).pipe(Layer.provide(collectives));
    return yield* Effect.scoped(serveMcp(input, workers)).pipe(
      Effect.provide(messaging),
    );
  }).pipe(
    Effect.catchAllCause(stopRuntime),
    Effect.withSpan("runDaemonRuntime"),
  );

function serveMcp(input: DaemonStartup, workers: FiberSet.FiberSet<never>) {
  const failure = FiberSet.join(workers).pipe(
    Effect.catchAllCause(stopRuntime),
  );
  return Effect.gen(function* () {
    const identity = yield* Identity;
    const management = yield* makeDaemonManagementOperations();
    yield* registerAtStart(
      management,
      (yield* identity.me()).kind === "registered",
      input.bootstrap.configuration.registration,
    );
    const handler = yield* makeHandler(
      management,
      input.bootstrap.mcpCredential,
    );
    yield* acquireHarnessMcpHttpServer({
      port: input.bootstrap.configuration.mcpPort,
      handler,
    }).pipe(Effect.mapError(() => runtimeFailure("listener")));
    yield* watchChanges(handler);
    yield* Console.log(
      `sharnessd listening on http://${LOOPBACK_HOST}:${input.bootstrap.configuration.mcpPort}/mcp (package ${packageVersion}; WIRE_VERSION ${WIRE_VERSION})`,
    );
    return yield* failure;
  }).pipe(Effect.raceFirst(failure));
}

function makeHandler(
  management: DaemonManagementOperations,
  credential: DaemonBootstrap["mcpCredential"],
) {
  return Effect.gen(function* () {
    const messaging = yield* HostMessaging;
    const inbox = yield* McpInbox;
    const firewall = yield* Firewall;
    return yield* makeHarnessMcpHttpHandler({
      implementation: { name: "sharnessd", version: packageVersion },
      credential,
      eventStore: inbox,
      operations: {
        ...management,
        send: messaging.send,
        searchConversations: messaging.listConversations,
        readConversation: messaging.readHistory,
        readInbox: messaging.listInbox,
        readInboxSummary: inbox.readInboxSummary,
        readEvent: ({ eventId }) =>
          Schema.decodeUnknown(DeliveryToken)(eventId).pipe(
            Effect.catchTag("ParseError", () =>
              Effect.fail({ reason: "invalid-event" }),
            ),
            Effect.flatMap(messaging.getInboxItem),
          ),
        acknowledgeDelivery: messaging.acknowledgeDelivery,
        readDecisions: firewall.readDecisions,
        ...(firewall.configuration === undefined
          ? {}
          : {
              readPolicy: firewall.readPolicy,
              updatePolicy: firewall.updatePolicy,
            }),
      },
    });
  });
}

function watchChanges(
  handler: Effect.Effect.Success<ReturnType<typeof makeHandler>>,
) {
  return Effect.gen(function* () {
    const messaging = yield* HostMessaging;
    yield* messaging.inboxChanged.pipe(
      Stream.runForEach(() => Effect.sync(handler.notifyPending)),
      Effect.forkScoped,
    );
    const registered = yield* LocalIdentity;
    yield* registered.changes.pipe(
      Stream.drop(1),
      Stream.runForEach(() =>
        Effect.sync(() => {
          handler.notify.toolsChanged();
        }),
      ),
      Effect.forkScoped,
    );
  });
}

function registerAtStart(
  management: DaemonManagementOperations,
  registered: boolean,
  request?: ManagementRegisterRequest,
) {
  if (registered || request === undefined) {
    return Effect.void;
  }
  return management.register(request).pipe(
    Effect.flatMap((result) =>
      result.kind === "registered"
        ? Effect.void
        : Effect.fail(
            new DaemonRuntimeError({
              phase: "registration",
              reason: result.kind,
            }),
          ),
    ),
  );
}

function stopRuntime(
  cause: Cause.Cause<unknown>,
): Effect.Effect<never, DaemonRuntimeError> {
  if (Cause.isInterruptedOnly(cause)) {
    return Effect.failCause(Cause.stripFailures(cause));
  }
  const failure = classifyFailure(
    Option.getOrUndefined(Cause.failureOption(cause)),
  );
  return Effect.logError("Daemon stopped", cause).pipe(
    Effect.zipRight(Effect.fail(failure)),
  );
}

function classifyFailure(error: unknown): DaemonRuntimeError {
  if (error instanceof DaemonRuntimeError) {
    return error;
  }
  if (!(error instanceof CommunicationError)) {
    return runtimeFailure("storage");
  }
  if (error.reason === "outbox-too-old") {
    return new DaemonRuntimeError({
      phase: "storage",
      reason: "outbox-too-old",
    });
  }
  if (error.reason === "agent-not-admitted") {
    return new DaemonRuntimeError({
      phase: "listener",
      reason: "agent-not-admitted",
    });
  }
  return runtimeFailure(
    error.reason === "network-unavailable" ? "listener" : "storage",
  );
}

function runtimeFailure(
  phase: DaemonRuntimeError["phase"],
): DaemonRuntimeError {
  return new DaemonRuntimeError({ phase });
}
