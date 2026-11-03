/** @file Promise-native access to scoped process fixtures for node:test consumers. */
import {
  type AgentCard,
  AgentIdentityCard,
  MessageAddressInput,
  type PermittedItem,
  SendRequest,
  type SendResult,
} from "@moltzap/social-harness";
import { Chunk, Effect, Exit, Option, Schema, Scope, Stream } from "effect";
import {
  acquireMcpConnection,
  callTool,
  type McpConnection,
} from "../../plugins/shared/mcp-client.js";
import {
  acquireDaemonManagementClient,
  acquireProcessInfrastructure,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  processTestError,
  type ProcessTestError,
} from "./daemon/daemon-fixture.ts";
import { inboxStream } from "./mcp-inbox.js";
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
  readonly agent: AgentCard;
  readonly send: (input: unknown) => Promise<SendResult>;
  readonly next: (count: number) => Promise<readonly PermittedItem[]>;
  readonly history: (address: string) => Promise<readonly unknown[]>;
}

/** One scope owns the infrastructure, daemons and participant subscriptions. */
export interface Network {
  readonly daemon: (name: string) => Promise<Daemon>;
  readonly endpoint: (name: string) => Promise<Endpoint>;
  readonly holdRouter: () => Promise<() => void>;
  readonly close: () => Promise<undefined>;
}

/** Starts one fixture's daemon, which a consumer may run in its own container; the configured SQL database carries its registration seed. */
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

async function networkEndpoint(
  scope: Scope.CloseableScope,
  fixture: DaemonProcessFixture,
): Promise<Endpoint> {
  const harness = await runInScope(
    scope,
    acquireMcpConnection(
      { endpoint: fixture.endpoint, credential: fixture.mcpCredential },
      { name: "process-test", version: "test" },
    ),
  );
  const agent = await runInScope(scope, readAgent(fixture));
  return {
    agent,
    address: `agent:${fixture.agentName}`,
    mcp: fixture.endpoint,
    mcpCredentialFile: fixture.mcpCredentialFile,
    send: (input) =>
      Effect.runPromise(
        Schema.decodeUnknown(SendRequest)(input).pipe(
          Effect.flatMap((decoded) =>
            callTool(harness.client, {
              name: "send_message",
              arguments: { input: decoded },
            }),
          ),
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
      return status.kind === "registered"
        ? yield* Schema.decodeUnknown(AgentIdentityCard)(
            status.identityCard,
          ).pipe(
            Effect.map(({ agentId, agentName, principalId }) => ({
              agentId,
              agentName,
              principalId,
            })),
          )
        : yield* processTestError("started daemon is not registered");
    }),
  );
}

/**
 * Awaits the next `count` deliveries on a subscription opened for this call,
 * acknowledging them before it closes, since a closed subscription's receipts
 * are stale.
 */
function nextItems(harness: McpConnection, count: number) {
  return inboxStream(harness).pipe(
    Stream.grouped(count),
    Stream.runHead,
    Effect.map(
      Option.match({
        onNone: (): readonly PermittedItem[] => [],
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
