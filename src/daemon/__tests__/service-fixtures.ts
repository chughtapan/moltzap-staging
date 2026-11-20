/** @file Fixtures of one daemon's agents: the server agent behind real policy over stored messages in a test database, with the clients that message it. */

import { Context, Effect, Layer, PubSub, Schema } from "effect";
import type {
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
  LoadedMessagePolicy,
} from "../../firewall/index.js";
import { createTestAgent } from "../../__tests__/agents.js";
import { Message } from "../../communication/index.js";
import { Firewall } from "../../firewall/service.js";
import { type AgentCard, Identity } from "../../identity.js";
import { type InstalledNorms, structureCheck } from "../../norms/index.js";
import { openTestDatabase } from "../../test.js";
import {
  conversationsOf,
  type Daemon,
  historyRecords,
  type HostedAgent,
  readInboxEntries,
  receive,
  send,
  type SendRequest,
} from "../agent.js";
import { AgentAddress } from "../values.js";

type Policy = Context.Tag.Service<typeof Firewall>;

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

/**
 * What a daemon shares when `serverAgent` is its server agent behind
 * `firewall`, with the history limit `runDaemon` defaults to and no norms
 * unless `settings` say otherwise.
 */
export const testDaemon = (
  serverAgent: HostedAgent,
  firewall: Policy,
  settings: Partial<Daemon> = {},
) =>
  Effect.map(
    PubSub.unbounded<string>(),
    (readable): Daemon => ({
      serverAgent,
      firewall,
      historyLimit: 256,
      turns: new Map(),
      readable,
      ...settings,
    }),
  );

/** How Alice's daemon opens. */
interface OpenOptions
  extends Partial<Pick<Daemon, "historyLimit" | "withheldReply">> {
  readonly policy?: TestPolicy;
  /** The contracts the agent starts with. */
  readonly contracts?: InstalledNorms["contracts"];
  readonly decorateFirewall?: (firewall: Policy) => Policy;
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
  const peers = new Map([bob, carol].map((peer) => [peer.card.agentId, peer]));
  let daemon: Daemon;
  /** The operations of `agent` on the open daemon. */
  const harnessOf = (agent: HostedAgent) => ({
    send: (request: SendRequest) => database.run(send(daemon, agent, request)),
    receive: (request: Parameters<typeof receive>[2]) =>
      database.run(receive(daemon, agent, request)),
    listConversations: () => database.run(conversationsOf(agent)),
    readHistory: (request: Parameters<typeof historyRecords>[1]) =>
      database.run(historyRecords(agent, request)),
  });
  const open = Effect.fn(function* ({
    policy,
    contracts,
    decorateFirewall,
    ...settings
  }: OpenOptions = {}) {
    const firewall = yield* Layer.build(
      Firewall.layer(
        alice.agentKey,
        policy?.source,
        contracts && structureCheck({ contracts }),
        policy?.provider,
      ).pipe(Layer.provide(Layer.succeed(Identity, { me: alice.card }))),
    ).pipe(Effect.map(Context.get(Firewall)));
    daemon = yield* testDaemon(
      alice,
      decorateFirewall?.(firewall) ?? firewall,
      settings,
    );
    return {
      harness: harnessOf(alice),
      firewall,
      /** Every entry of Alice's inbox, in read order. */
      inbox: readInboxEntries(alice.agentKey, {
        limit: 1000,
        cursor: "0",
      }).pipe(
        Effect.map((page) => page.entries),
        database.run,
        Effect.orDie,
      ),
    };
  }, database.run);
  /** The operations of a client of the open daemon. */
  const client = (card: AgentCard) =>
    Effect.map(Effect.fromNullable(peers.get(card.agentId)), (peer) =>
      harnessOf(peer),
    );
  const aliceAddress = AgentAddress.make("agent:alice");
  return {
    self: alice.card,
    bob: bob.card,
    carol: carol.card,
    open,
    /** Every message Alice has sent, in the order it was stored. */
    sent: database.sql<{
      message: string;
    }>`SELECT message FROM message_log WHERE agent_key = ${alice.agentKey}
        AND author_agent_id = ${alice.card.agentId} ORDER BY sequence`.pipe(
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
      Effect.flatMap(client(author ?? bob.card), (own) =>
        own.send({ ...request, to: aliceAddress }),
      ),
    /** The kinds of the records a client, Bob unless given, reads of its conversation with Alice. */
    historyOf: (reader?: AgentCard) =>
      Effect.flatMap(client(reader ?? bob.card), (own) =>
        own.readHistory({ address: aliceAddress }),
      ).pipe(Effect.map(({ records }) => records.map(({ kind }) => kind))),
    addPeer: (name: string) =>
      Effect.map(join(name), (peer) => {
        peers.set(peer.card.agentId, peer);
        return peer.card;
      }),
  };
}).pipe(Effect.withSpan("makeHarnessFixture"));
