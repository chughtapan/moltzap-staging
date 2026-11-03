/** @file Composes real domain services with deterministic policy providers for integration traces. */
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
  Router,
  serveMcp,
} from "@moltzap/social-harness/test";
import { Context, Duration, Effect, FiberSet, Layer } from "effect";

/** Custom policy evaluation replaces only the model provider; storage, transport and MCP remain real. */
export function runPolicyRuntime(policy: FirewallOptions) {
  return Effect.gen(function* () {
    const access = yield* databaseAccess;
    const database = yield* Layer.build(databaseConnection(access.locator));
    const sql = Context.get(database, SqlClient.SqlClient);
    return yield* Effect.gen(function* () {
      const settings = yield* loadDaemonSettings(access.unlockKey);
      if (
        settings.authentication.kind !== "local" ||
        settings.admissionCredential === undefined
      ) {
        return yield* Effect.dieMessage(
          "Policy fixtures require local authenticated configuration",
        );
      }
      const profile = yield* getOrCreateOwnedAgent(
        localOwner,
        access.unlockKey,
      );
      const registration = yield* loadAgentRegistration(
        profile.agent_key,
        access.unlockKey,
      );
      const workers = yield* FiberSet.make();
      const identity = Identity.layer({
        agentKey: profile.agent_key,
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
            agentKey: profile.agent_key,
            communication: (identityCard) =>
              Communication.layer({
                agentKey: profile.agent_key,
                identityCard,
                signingAuthority: registration.signingAuthority,
                registrySignerPublicKey: settings.registrySignerPublicKey,
              }),
          },
          workers,
        ).pipe(
          Layer.provideMerge(
            Firewall.layer(profile.agent_key, policy).pipe(
              Layer.provideMerge(identity),
            ),
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
      const agent = yield* makeAgentMcpApp({
        agentKey: profile.agent_key,
        implementation: { name: "policy-integration", version: "1" },
      }).pipe(Effect.provide(services));
      const app = yield* makeMcpHttpApp(
        { credential: settings.authentication.credential },
        () => Effect.succeed(agent),
      );
      yield* serveMcp(settings.port, app, settings.host);
      yield* Effect.log(
        `sharnessd listening on http://${settings.host}:${settings.port}/mcp (policy integration)`,
      );
      return yield* FiberSet.join(workers);
    }).pipe(Effect.provideService(SqlClient.SqlClient, sql));
  }).pipe(
    Effect.provide(NodeFileSystem.layer),
    Effect.withSpan("runPolicyRuntime"),
  );
}
