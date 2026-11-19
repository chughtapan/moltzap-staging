/** @file Promise access to one daemon and its agents for node:test suites. */
import type { PermittedItem, SendResult } from "@social-harness/social-harness";
import { Chunk, Effect, Exit, Option, Scope, Stream } from "effect";
import { send } from "./agent-fixture.js";
import {
  type Daemon,
  ProcessTestError,
  SERVER_AGENT_OWNER,
  sharedDaemon,
  type SignedIn,
} from "./daemon/daemon-fixture.ts";
import { inboxStream } from "./mcp-inbox.js";
import { historyMessage } from "./saved-history.js";
import { callTool } from "./tool-call.js";

/* eslint-disable agent-code-guard/async-keyword, agent-code-guard/promise-type -- node:test consumes promises at the fixture boundary. */

/**
 * One owner's agent: what a host needs to connect as it, and the test's own
 * connection. `next` reads the inbox only while it waits, so a host connected
 * as the same agent receives every delivery the test does not ask for.
 */
export interface AgentAccess {
  readonly agent: SignedIn["agent"];
  readonly address: SignedIn["address"];
  readonly mcp: URL;
  readonly mcpCredentialFile: string;
  readonly send: (input: unknown) => Promise<SendResult>;
  readonly next: (count: number) => Promise<readonly PermittedItem[]>;
  /** The contents of the server agent's conversation with `client`, or of a client's one conversation. */
  readonly history: (client?: AgentAccess) => Promise<readonly unknown[]>;
}

/** One scope owns the daemon and every agent's connection. */
export interface DaemonAccess {
  /** Signs in the server agent's owner, or the named owner of a client. */
  readonly signIn: (owner?: string) => Promise<AgentAccess>;
  readonly close: () => Promise<undefined>;
}

/** Runs a prepared daemon, which a consumer may do in its own container. */
type StartDaemon = (
  daemon: Daemon,
) => Effect.Effect<unknown, ProcessTestError, Scope.Scope>;

/** Prepares one daemon and runs it with `startDaemon`, closing partial startup on failure. */
export async function openDaemon(
  startDaemon: StartDaemon,
): Promise<DaemonAccess> {
  const scope = Effect.runSync(Scope.make());
  const close = () =>
    Effect.runPromise(Scope.close(scope, Exit.void).pipe(Effect.as(undefined)));
  try {
    const daemon = await runInScope(
      scope,
      sharedDaemon().pipe(Effect.tap(startDaemon)),
    );
    return {
      signIn: async (owner = SERVER_AGENT_OWNER) =>
        agentAccess(daemon, await runInScope(scope, daemon.signInAs(owner))),
      close,
    };
  } catch (cause) {
    await close();
    throw cause;
  }
}

function agentAccess(daemon: Daemon, signedIn: SignedIn): AgentAccess {
  return {
    agent: signedIn.agent,
    address: signedIn.address,
    mcp: daemon.endpoint,
    mcpCredentialFile: signedIn.credentialFile,
    send: (input) => Effect.runPromise(send(signedIn, input)),
    next: (count) => Effect.runPromise(nextItems(signedIn, count)),
    history: (client) =>
      Effect.runPromise(
        callTool(signedIn.connection.mcp, {
          name: "read_conversation",
          arguments: client === undefined ? {} : { address: client.address },
        }).pipe(
          Effect.map((page) =>
            page.records.map((record) => historyMessage(record).content),
          ),
        ),
      ),
  };
}

/** The next `count` deliveries, read by a subscription opened for this call. */
function nextItems(signedIn: SignedIn, count: number) {
  return inboxStream(signedIn.connection).pipe(
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
        new ProcessTestError({
          message: `timed out awaiting ${String(count)} deliveries`,
        }),
    }),
  );
}

function runInScope<A, E>(
  scope: Scope.CloseableScope,
  effect: Effect.Effect<A, E, Scope.Scope>,
) {
  return Effect.runPromise(Scope.extend(effect, scope));
}

/* eslint-enable agent-code-guard/async-keyword, agent-code-guard/promise-type -- Restore Effect-only application boundaries. */
