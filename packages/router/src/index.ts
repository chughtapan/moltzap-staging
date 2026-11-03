/** @file Runs one volatile Router process and its authenticated HTTP listener. */
import { HttpServer } from "@effect/platform";
import { NodeHttpClient, NodeHttpServer } from "@effect/platform-node";
import { WIRE_VERSION } from "@moltzap/social-harness";
import { Authentication, Registry } from "@moltzap/social-harness/internal";
import { Console, Data, Duration, Effect, Layer } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer.make requires Node's server factory at this adapter boundary.
import { createServer } from "node:http";
import {
  configurationMessage,
  loadRouterConfiguration,
  processHelp,
  type RouterConfiguration,
} from "./config.js";
import { makeRouterFeed } from "./feed.js";
import { makeRouterHttpApp } from "./http.js";
import {
  generatePollCursorKey,
  generateRouterInstanceId,
  makePollCursorCodec,
  makePollWaiters,
  makeRouterPoll,
} from "./poll.js";
import { makeRouterSend } from "./send.js";

// eslint-disable-next-line @typescript-eslint/no-namespace -- The namespace keeps the process layer and its closed startup error under one RouterServer capability.
export namespace RouterServer {
  /** Lists required settings and defaults without reading configuration or starting a listener. */
  export const help = processHelp;

  /** Closed Router startup phase. */
  export class StartupError extends Data.TaggedError(
    "RouterServerStartupError",
  )<{
    readonly phase: "configuration" | "listener";
    readonly detail?: string;
  }> {
    /** Rejected environment values never enter the operator message. */
    override get message(): string {
      const detail = this.detail === undefined ? "" : `: ${this.detail}`;
      return `sharness-router startup failed in phase ${this.phase}${detail}`;
    }
  }

  const runRouterServer = Effect.gen(function* () {
    const configuration = yield* loadRouterConfiguration.pipe(
      Effect.mapError(configurationFailure),
    );
    const app = yield* buildRouterApp(configuration);
    const httpServer = yield* NodeHttpServer.make(createServer, {
      host: configuration.host,
      port: configuration.port,
    }).pipe(Effect.mapError(listenerFailure));
    yield* HttpServer.serveEffect(app).pipe(
      Effect.provideService(HttpServer.HttpServer, httpServer),
      Effect.provide(authenticatedHttpLayer(configuration)),
      Effect.provide(NodeHttpServer.layerContext),
    );
    yield* Console.log(
      `sharness-router listening on ${listeningUrl(httpServer.address)} (WIRE_VERSION ${WIRE_VERSION})`,
    );
  }).pipe(Effect.withSpan("RouterServer.layer"));

  /**
   * Acquires one HTTP listener and its bounded volatile feed for this scope.
   * Every acquisition mints a fresh Router instance and cursor key, so a
   * restart invalidates continuations and requires endpoint recovery.
   *
   * Configuration is read from the active Effect ConfigProvider. Missing or
   * invalid settings and listener acquisition fail with `StartupError`.
   * Releasing the layer closes the listener and discards its feed.
   */
  export const layer: Layer.Layer<never, StartupError> =
    Layer.scopedDiscard(runRouterServer);

  function buildRouterApp(configuration: RouterConfiguration) {
    return Effect.gen(function* () {
      const routerInstanceId = yield* Effect.try({
        try: generateRouterInstanceId,
        catch: () => new StartupError({ phase: "configuration" }),
      });
      const cursorKey = yield* Effect.try({
        try: generatePollCursorKey,
        catch: () => new StartupError({ phase: "configuration" }),
      });
      const feed = yield* makeRouterFeed({
        routerInstanceId,
        retainedMessageCapacity: configuration.retainedMessageCapacity,
        retainedMessageByteCapacity: configuration.retainedMessageByteCapacity,
      });
      const pollWaiters = yield* makePollWaiters(
        configuration.heldPollCapacity,
      );
      const send = makeRouterSend({
        routerInstanceId,
        feed,
        pollWaiters,
      });
      const poll = makeRouterPoll({
        routerInstanceId,
        feed,
        pollWaiters,
        cursorCodec: makePollCursorCodec({
          key: cursorKey,
          routerInstanceId,
        }),
        pollMessageLimit: configuration.pollMessageLimit,
        pollResponseByteLimit: configuration.pollResponseByteLimit,
      });
      return makeRouterHttpApp({
        send,
        poll,
        feed,
        requestSemaphore: yield* Effect.makeSemaphore(
          configuration.requestConcurrencyLimit,
        ),
      });
    });
  }

  function authenticatedHttpLayer(configuration: RouterConfiguration) {
    const registryLayer = Registry.layer({
      origin: configuration.registryOrigin,
      registrySignerPublicKey: configuration.registrySignerPublicKey,
      requestTimeout: Duration.millis(configuration.registryLookupTimeoutMs),
    }).pipe(Layer.provide(NodeHttpClient.layer));
    return Layer.unwrapEffect(
      Effect.gen(function* () {
        const registry = yield* Registry;
        return Authentication.layer({
          liveNonceCapacity: configuration.liveNonceCapacity,
          identityCardCacheCapacity: configuration.identityCardCacheCapacity,
          registryLookupConcurrencyLimit:
            configuration.registryLookupConcurrencyLimit,
          lookup: registry.lookup,
        });
      }),
    ).pipe(Layer.provide(registryLayer));
  }

  function configurationFailure(
    cause: Effect.Effect.Error<typeof loadRouterConfiguration>,
  ) {
    return new StartupError({
      phase: "configuration",
      detail: configurationMessage(cause),
    });
  }

  function listenerFailure() {
    return new StartupError({ phase: "listener" });
  }
}

/** IPv6 listener addresses need brackets in a URL authority. */
function listeningUrl(address: HttpServer.Address): string {
  if (address._tag === "UnixAddress") {
    return HttpServer.formatAddress(address);
  }
  const host = address.hostname.includes(":")
    ? `[${address.hostname}]`
    : address.hostname;
  return `http://${host}:${address.port}`;
}
