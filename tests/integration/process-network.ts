/** @file Promise-native access to scoped process fixtures for node:test consumers. */
import {
  acquireHarnessEndpoint,
  type AgentView,
  type HarnessEndpoint,
  type InboundItem,
  MessageAddressInput,
  SendInput,
  type SendResult,
} from "@moltzap/social-harness";
import { Chunk, Effect, Exit, Option, Schema, Scope, Stream } from "effect";
import {
  acquireDaemonManagementClient,
  acquireProcessInfrastructure,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  makeRegistrationRequest,
  processTestError,
  type ProcessTestError,
} from "./daemon/daemon-process-harness.ts";
import { historyMessage } from "./saved-history.js";

/* eslint-disable agent-code-guard/async-keyword, agent-code-guard/promise-type -- node:test consumes promises at the fixture boundary. */

/** A registered daemon's coordinates, shared with a host container. */
interface Daemon {
  readonly address: string;
  readonly mcp: URL;
  readonly mcpCredentialFile: string;
}

/**
 * A test's own harness endpoint on a daemon, with management history. It
 * subscribes to inbound deliveries only while `next` waits, so a host that
 * shares the daemon receives every delivery the test does not ask for.
 */
export interface Endpoint extends Daemon {
  readonly agent: AgentView;
  readonly send: (input: unknown) => Promise<SendResult>;
  readonly next: (count: number) => Promise<readonly InboundItem[]>;
  readonly history: (address: string) => Promise<readonly unknown[]>;
}

/** One scope owns the infrastructure, daemons and participant subscriptions. */
export interface Network {
  readonly daemon: (name: string) => Promise<Daemon>;
  readonly endpoint: (name: string) => Promise<Endpoint>;
  readonly holdRouter: () => Promise<() => void>;
  readonly close: () => Promise<undefined>;
}

/** Starts one fixture's daemon, which a consumer may run in its own container; the fixture's environment carries its startup registration. */
type StartDaemon = (
  fixture: DaemonProcessFixture,
) => Effect.Effect<unknown, ProcessTestError, Scope.Scope>;

/** Acquires the shared process network, closing partial startup on failure. */
export async function startNetwork(startDaemon: StartDaemon): Promise<Network> {
  const scope = Effect.runSync(Scope.make());
  const close = () =>
    Effect.runPromise(Scope.close(scope, Exit.void).pipe(Effect.as(undefined)));
  try {
    const infrastructure = await runInScope(
      scope,
      acquireProcessInfrastructure,
    );
    const daemon = (name: string) =>
      runInScope(
        scope,
        makeDaemonProcessFixture(infrastructure, name).pipe(
          Effect.map(withStartupRegistration),
          Effect.tap(startDaemon),
        ),
      );
    return {
      daemon: async (name) => {
        const fixture = await daemon(name);
        return {
          address: `agent:${fixture.agentName}`,
          mcp: fixture.endpoint,
          mcpCredentialFile: fixture.mcpCredentialFile,
        };
      },
      endpoint: async (name) =>
        await networkEndpoint(scope, await daemon(name)),
      holdRouter: () => runInScope(scope, infrastructure.holdRouter),
      close,
    };
  } catch (cause) {
    await close();
    throw cause;
  }
}

/**
 * Hosting starts a daemon with its registration in the environment, and the
 * daemon registers before it listens, so a started fixture is registered.
 */
function withStartupRegistration(
  fixture: DaemonProcessFixture,
): DaemonProcessFixture {
  const registration = makeRegistrationRequest(fixture);
  return {
    ...fixture,
    environment: {
      ...fixture.environment,
      SHARNESS_REGISTRATION_AGENT_NAME: registration.agentName,
      SHARNESS_REGISTRATION_OPERATION_ID: registration.operationId,
      SHARNESS_REGISTRATION_PRINCIPAL_ID: registration.principalId,
    },
  };
}

async function networkEndpoint(
  scope: Scope.CloseableScope,
  fixture: DaemonProcessFixture,
): Promise<Endpoint> {
  const harness = await runInScope(
    scope,
    acquireHarnessEndpoint(fixture.endpoint, {
      credential: fixture.mcpCredential,
      receiveMode: "push",
    }),
  );
  const agent = await runInScope(scope, readAgent(fixture));
  return {
    agent,
    address: `agent:${fixture.agentName}`,
    mcp: fixture.endpoint,
    mcpCredentialFile: fixture.mcpCredentialFile,
    send: (input) =>
      Effect.runPromise(
        Schema.decodeUnknown(SendInput)(input).pipe(
          Effect.flatMap((decoded) => harness.send(decoded)),
        ),
      ),
    next: (count) => Effect.runPromise(nextItems(harness, count)),
    history: (address) => runInScope(scope, readContents(fixture, address)),
  };
}

function readAgent(fixture: DaemonProcessFixture) {
  return Effect.scoped(
    Effect.gen(function* () {
      const management = yield* acquireDaemonManagementClient(fixture);
      const status = yield* management.status();
      return status.kind === "active"
        ? status.agent
        : yield* processTestError("started daemon is not registered");
    }),
  );
}

/**
 * Awaits the next `count` deliveries on a subscription opened for this call,
 * acknowledging them before it closes, since a closed subscription's receipts
 * are stale.
 */
function nextItems(harness: HarnessEndpoint, count: number) {
  return harness.messages.pipe(
    Stream.grouped(count),
    Stream.tap((batch) =>
      Effect.forEach(batch, ({ acknowledge }) => acknowledge, {
        concurrency: 1,
        discard: true,
      }),
    ),
    Stream.runHead,
    Effect.map(
      Option.match({
        onNone: (): readonly InboundItem[] => [],
        onSome: (batch) => Chunk.toReadonlyArray(batch).map(({ item }) => item),
      }),
    ),
    Effect.timeoutFail({
      duration: "60 seconds",
      onTimeout: () =>
        processTestError(`timed out awaiting ${String(count)} deliveries`),
    }),
  );
}

function readContents(fixture: DaemonProcessFixture, address: string) {
  return Effect.scoped(
    acquireDaemonManagementClient(fixture).pipe(
      Effect.flatMap((management) =>
        management.readConversation(
          Schema.decodeUnknownSync(MessageAddressInput)(address),
        ),
      ),
      Effect.map((result) =>
        result.records.map((record) => historyMessage(record).content),
      ),
    ),
  );
}

function runInScope<A, E>(
  scope: Scope.CloseableScope,
  effect: Effect.Effect<A, E, Scope.Scope>,
) {
  return Effect.runPromise(Scope.extend(effect, scope));
}

/* eslint-enable agent-code-guard/async-keyword, agent-code-guard/promise-type -- Restore Effect-only application boundaries. */
