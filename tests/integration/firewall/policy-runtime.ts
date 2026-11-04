/** @file Composes real domain services with deterministic policy providers for integration traces. */
import { HttpClient } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import {
  Communication,
  Firewall,
  type FirewallOptions,
  Identity,
  Registry,
  SocialHarness,
} from "@moltzap/social-harness/service";
import {
  databaseAccess,
  databaseConnection,
  getOrCreateOwnedAgent,
  loadAgentRegistration,
  loadDaemonSettings,
  localOwner,
  makeAgentMcpApp,
  makeMcpHttpApp,
  makeWebhookEvents,
  Router,
  serveMcp,
  spanExport,
  webhookHttpClientLayer,
} from "@moltzap/social-harness/test";
import { Context, Deferred, Duration, Effect, FiberSet, Layer } from "effect";

/** Custom policy evaluation replaces only the model provider; storage, transport and MCP remain real. */
export function runPolicyRuntime(policy: FirewallOptions) {
  return Effect.gen(function* () {
    const access = yield* databaseAccess;
    const database = yield* Layer.build(databaseConnection(access.locator));
    const sql = Context.get(database, SqlClient.SqlClient);
    return yield* Effect.gen(function* () {
      const { settings, agent: seed } = yield* loadDaemonSettings();
      if (
        settings.authentication.kind !== "local" ||
        settings.admissionCredential === undefined
      ) {
        return yield* Effect.dieMessage(
          "Policy fixtures require local authenticated configuration",
        );
      }
      const agentKey = yield* getOrCreateOwnedAgent(
        localOwner,
        access.unlockKey,
        seed,
      );
      const registration = yield* loadAgentRegistration(
        agentKey,
        access.unlockKey,
      );
      const workers = yield* FiberSet.make();
      const identity = Identity.layer({
        agentKey,
        signingAuthority: registration.signingAuthority,
        registrySignerPublicKey: settings.registrySignerPublicKey,
        admissionCredential: Effect.succeed(settings.admissionCredential),
      });
      const network = Layer.merge(
        Registry.layer({
          origin: settings.registryOrigin,
          registrySignerPublicKey: settings.registrySignerPublicKey,
          requestTimeout: Duration.seconds(30),
        }),
        Router.layer({
          origin: settings.routerOrigin,
          sendTimeout: Duration.seconds(30),
          pollTimeout: Duration.seconds(35),
        }),
      ).pipe(Layer.provide(NodeHttpClient.layer));
      const services = yield* Layer.build(
        SocialHarness.layer(
          {
            agentKey,
            communication: (identityCard) =>
              Communication.layer({
                agentKey,
                identityCard,
                signingAuthority: registration.signingAuthority,
                registrySignerPublicKey: settings.registrySignerPublicKey,
              }),
          },
          workers,
        ).pipe(
          Layer.provideMerge(
            Firewall.layer(agentKey, policy).pipe(Layer.provideMerge(identity)),
          ),
          Layer.provide(network),
        ),
      );
      const identityService = Context.get(services, Identity);
      if ((yield* identityService.me()).kind === "unregistered") {
        yield* Context.get(services, SocialHarness).register(
          registration.registration,
        );
      }
      const callbackClient = yield* Layer.build(webhookHttpClientLayer);
      const webhook = yield* makeWebhookEvents(agentKey, access.unlockKey).pipe(
        Effect.provide(services),
        Effect.provideService(
          HttpClient.HttpClient,
          Context.get(callbackClient, HttpClient.HttpClient),
        ),
      );
      const agent = yield* makeAgentMcpApp({
        webhook,
        stopped: yield* Deferred.make<undefined>(),
        implementation: { name: "policy-integration", version: "1" },
      }).pipe(Effect.provide(services));
      const app = yield* makeMcpHttpApp(settings.authentication, () =>
        Effect.succeed(agent),
      );
      yield* serveMcp(settings.port, app, settings.host);
      yield* Effect.log(
        `sharnessd listening on http://${settings.host}:${settings.port}/mcp (policy integration)`,
      );
      return yield* FiberSet.join(workers);
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
  }).pipe(
    Effect.withSpan("runPolicyRuntime"),
    Effect.provide(Layer.merge(NodeFileSystem.layer, spanExport)),
  );
}
