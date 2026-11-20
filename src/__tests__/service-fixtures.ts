/** @file Fixtures of one daemon's agents: the server agent behind real policy over stored messages in a test database, with the clients that message it. */

import { Effect, PubSub, Schema } from "effect";
import {
  AgentAddress,
  conversationsOf,
  type Daemon,
  historyRecords,
  readInboxEntries,
  receive,
  send,
  type SendRequest,
} from "../daemon/agent.js";
import { readDecisions } from "../daemon/store.js";
import {
  type DecisionProvider,
  type DecisionRequest,
  type DecisionResponse,
  type Firewall,
  type LoadedMessagePolicy,
  makeFirewall,
} from "../firewall.js";
import { type AgentCard, Message } from "../messages.js";
import { type InstalledNorms, structureCheck } from "../norms/index.js";
import { createTestAgent } from "./agents.js";
import { openTestDatabase } from "./test.js";

/**
 * Deterministic judgments still exercise the real question construction. Every
 * rule gets `compliance` at `probability`; below the policy's bar, an answer is
 * unsure.
 */
export function decisionResponse(
  request: DecisionRequest,
  compliance: "compliant" | "violation" = "compliant",
  probability = 1,
): DecisionResponse {
  return {
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => [
        id,
        {
          choice: compliance,
          probabilities: Object.fromEntries(
            Object.keys(question.criteria).map((choice) => [
              choice,
              choice === compliance ? probability : 1 - probability,
            ]),
          ),
        },
      ]),
    ),
  };
}

/** These fields describe observable policy evidence rather than the provider's private trace. */
export const decisionState = Schema.decodeUnknownSync(
  Schema.parseJson(
    Schema.Struct({
      direction: Schema.Literal("incoming", "outgoing"),
      candidate: Schema.Struct({ text: Schema.String, sender: Schema.String }),
      history: Schema.Array(Schema.Struct({ text: Schema.String })),
      earlierHistoryOmitted: Schema.Boolean,
    }),
  ),
);

/** A message policy and the provider that judges its text rules in place of Jev. */
interface TestPolicy {
  readonly source: LoadedMessagePolicy;
  readonly provider: DecisionProvider;
}

/**
 * One text rule makes check order and saved advice
 * observable. With `action`, the rule replies to or silently withholds the
 * incoming messages it rejects.
 */
export function composedPolicy(
  provider: DecisionProvider,
  action?: "deny" | "reply",
): TestPolicy {
  return {
    provider,
    source: {
      policy: {
        rules: [
          {
            id: "privacy",
            applies: "both",
            text: "Protect the owner's private information.",
            guidance: "Check before sharing private information.",
            feedback: "Keep private information private.",
            ...(action === undefined ? {} : { action }),
          },
        ],
        settings: {
          minimumProbability: 0.8,
          timeoutMilliseconds: 5_000,
        },
      },
    },
  };
}

/** What a daemon shares when `serverAgent` is its server agent behind `firewall`, with no norms. */
export const testDaemon = (serverAgent: AgentCard, firewall: Firewall) =>
  Effect.map(
    PubSub.unbounded<string>(),
    (readable): Daemon => ({
      serverAgent,
      firewall,
      turns: new Map(),
      readable,
    }),
  );

/** How Alice's daemon opens: the history limit `main.ts` defaults to unless given, and no notice unless given. */
interface OpenOptions {
  readonly historyLimit?: number;
  readonly notice?: string;
  readonly policy?: TestPolicy;
  /** The norms the agent starts with. */
  readonly norms?: InstalledNorms;
  readonly decorateFirewall?: (firewall: Firewall) => Firewall;
}

/**
 * Alice, the server agent, with her clients Bob, Carol and any peer a test
 * adds, on one daemon. `open` installs Alice's firewall and gives her
 * operations as `harness`; a client then sends on the same daemon.
 */
export const makeHarnessFixture = Effect.gen(function* () {
  const database = yield* openTestDatabase();
  const join = (name: string) => database.run(createTestAgent(name));
  const alice = yield* join("alice");
  const bob = yield* join("bob");
  const carol = yield* join("carol");
  const agents = new Map(
    [alice, bob, carol].map((agent) => [agent.agentName, agent]),
  );
  /** The agent an address names, which a test knows. */
  const addressed = (address: AgentAddress) =>
    Effect.orDie(
      Effect.fromNullable(
        [...agents.values()].find(
          (agent) => `agent:${agent.agentName}` === address,
        ),
      ),
    );
  let daemon: Daemon;
  /** The operations of `agent` on the open daemon, each on the agent its address names. */
  const harnessOf = (agent: AgentCard) => ({
    send: ({ to, ...message }: SendRequest) =>
      Effect.flatMap(addressed(to), (recipient) =>
        send(daemon, { ...message, author: agent, recipient }),
      ).pipe(
        Effect.map((id) => ({ id, address: to })),
        database.run,
      ),
    receive: (request: Parameters<typeof receive>[2]) =>
      database.run(receive(daemon, agent, request)),
    listConversations: () => database.run(conversationsOf(agent)),
    readHistory: (request: {
      readonly address: AgentAddress;
      readonly cursor?: string;
    }) =>
      Effect.flatMap(addressed(request.address), (other) =>
        historyRecords(agent, other, request.cursor),
      ).pipe(database.run),
  });
  const open = Effect.fn(function* ({
    policy,
    norms,
    decorateFirewall,
    historyLimit = 256,
    notice,
  }: OpenOptions = {}) {
    const firewall = makeFirewall({
      ...(policy ?? {}),
      me: alice,
      historyLimit,
      ...(notice === undefined ? {} : { notice }),
      ...(norms === undefined ? {} : { structureCheck: structureCheck(norms) }),
    });
    daemon = yield* testDaemon(alice, decorateFirewall?.(firewall) ?? firewall);
    return {
      harness: harnessOf(alice),
      /** The record of each check Alice's firewall has made, newest first. */
      decisions: database.run(readDecisions(0)),
      /** Every entry of Alice's inbox, in read order. */
      inbox: readInboxEntries(alice.agentId, {
        limit: 1000,
        cursor: "0",
      }).pipe(
        Effect.map((page) => page.entries),
        database.run,
        Effect.orDie,
      ),
    };
  }, database.run);
  const aliceAddress = AgentAddress.make("agent:alice");
  return {
    self: alice,
    bob,
    carol,
    open,
    /** Every message Alice has sent, in the order it was stored. */
    sent: database.sql<{
      message: string;
    }>`SELECT message FROM message_log
        WHERE author_id = ${alice.agentId} ORDER BY sequence`.pipe(
      Effect.flatMap(
        Schema.decodeUnknown(
          Schema.Array(Schema.Struct({ message: Schema.parseJson(Message) })),
        ),
      ),
      Effect.map((rows) => rows.map((row) => row.message)),
      Effect.orDie,
    ),
    /** Sends Alice a message as a client, Bob unless given. */
    sendFrom: (request: Omit<SendRequest, "to">, author?: AgentCard) =>
      harnessOf(author ?? bob).send({ ...request, to: aliceAddress }),
    /** The kinds of the records a client, Bob unless given, reads of its conversation with Alice. */
    historyOf: (reader?: AgentCard) =>
      harnessOf(reader ?? bob)
        .readHistory({ address: aliceAddress })
        .pipe(Effect.map(({ records }) => records.map(({ kind }) => kind))),
    addPeer: (name: string) =>
      Effect.map(join(name), (peer) => {
        agents.set(peer.agentName, peer);
        return peer;
      }),
  };
}).pipe(Effect.withSpan("makeHarnessFixture"));
