/** @file SocialHarness fixtures: one agent's harness over real policy, identity and communication in a test database, with the peers that message it. */

import { SqlClient } from "@effect/sql";
import { Context, Effect, flow, Layer, PubSub, Schema, Scope } from "effect";
import type {
  CompiledContract,
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
  LoadedMessagePolicy,
} from "../../firewall/index.js";
import type { AgentCard } from "../../identity.js";
import { createTestAgent } from "../../__tests__/agents.js";
import { CommunicationError, type Message } from "../../communication/index.js";
import { Communication } from "../../communication/service.js";
import { Firewall } from "../../firewall/service.js";
import { openTestDatabase } from "../../test.js";
import {
  type HarnessSettings,
  readInboxEntries,
  SocialHarness,
} from "../agent.js";
import { identityLayer } from "../owners.js";
import { AgentAddress, type SendRequest } from "../values.js";

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
        format: "org.social-harness/message-policy/v2",
        facts: [],
        text: {
          fixedRules: [
            {
              id: "privacy",
              applies: "both",
              text: "Protect the owner's private information.",
              guidance: "Check before sharing private information.",
              feedback: "Keep private information private.",
              ...(action === undefined ? {} : { action }),
            },
          ],
        },
        textSettings: {
          minimumProbability: 0.8,
          maximumRequestBytes: 100_000,
          timeoutMilliseconds: 5_000,
        },
      },
    },
  };
}

/** How a harness opens. */
interface OpenOptions extends Omit<HarnessSettings, "agentKey" | "checkedBy"> {
  readonly policy?: TestPolicy;
  /** The contracts the agent starts with. */
  readonly contracts?: readonly CompiledContract[];
  readonly decorateFirewall?: (firewall: Policy) => Policy;
}

type Harness = Context.Tag.Service<typeof SocialHarness>;

/**
 * Alice, the server agent whose harness a test opens, with her clients Bob,
 * Carol and any peer a test adds, on one daemon. A client sends through a
 * harness of its own, which has Alice's open harness check the message.
 * `submitted` lists what Alice's harness handed to communication, and after
 * `failSends` communication stores none of it.
 */
export const makeHarnessFixture = Effect.gen(function* () {
  const database = yield* openTestDatabase();
  const scope = yield* Effect.scope;
  const readable = yield* PubSub.unbounded<string>();
  const submitted: Message[] = [];
  let sendsFail = false;
  let server: Harness | undefined;
  const clients = new Map<string, Harness>();
  /** Creates a client, whose harness closes with the fixture. */
  const join = Effect.fn(
    function* (name: string) {
      const agent = yield* createTestAgent(name);
      const services = yield* Layer.build(
        SocialHarness.layer({
          agentKey: agent.agentKey,
          checkedBy: Effect.suspend(() => Effect.fromNullable(server)).pipe(
            Effect.orDie,
          ),
        }).pipe(
          Layer.provide(Communication.layer({ ...agent, readable })),
          Layer.provide(
            Firewall.layer(agent.agentKey).pipe(
              Layer.provideMerge(identityLayer(agent.agentKey)),
            ),
          ),
        ),
      );
      clients.set(agent.card.agentId, Context.get(services, SocialHarness));
      return agent.card;
    },
    flow(database.run, Scope.extend(scope)),
  );
  const alice = yield* database.run(createTestAgent("alice"));
  const bob = yield* join("bob");
  const carol = yield* join("carol");
  /** Alice's communication, recording each send and failing it once sends fail. */
  const communication = Layer.effect(
    Communication,
    Effect.map(Communication, (real) => ({
      ...real,
      send: (...[message, received]: Parameters<typeof real.send>) =>
        Effect.suspend(() => {
          submitted.push(message);
          return sendsFail
            ? Effect.fail(
                new CommunicationError({ reason: "persistence-failed" }),
              )
            : real.send(message, received);
        }),
    })),
  ).pipe(Layer.provide(Communication.layer({ ...alice, readable })));
  const open = Effect.fn(function* (options: OpenOptions = {}) {
    const services = yield* Layer.build(
      Firewall.layer(
        alice.agentKey,
        options.policy?.source,
        options.contracts,
        options.policy?.provider,
      ).pipe(Layer.provideMerge(identityLayer(alice.agentKey))),
    );
    const firewall = Context.get(services, Firewall);
    const context = yield* Layer.build(
      SocialHarness.layer({ ...options, agentKey: alice.agentKey }).pipe(
        Layer.provide(communication),
      ),
    ).pipe(
      Effect.provide(
        options.decorateFirewall === undefined
          ? services
          : Context.add(services, Firewall, options.decorateFirewall(firewall)),
      ),
    );
    const harness = Context.get(context, SocialHarness);
    server = harness;
    return {
      harness,
      firewall,
      /** Every entry of Alice's inbox, in read order. */
      inbox: readInboxEntries(alice.agentKey, {
        limit: 1000,
        cursor: "0",
      }).pipe(
        Effect.map((page) => page.entries),
        Effect.provideService(SqlClient.SqlClient, database.sql),
        Effect.orDie,
      ),
    };
  }, database.run);
  return {
    self: alice.card,
    bob,
    carol,
    submitted,
    open,
    /** Sends Alice a message through the harness of its author, Bob unless given. */
    sendFrom: (request: Omit<SendRequest, "to">, author: AgentCard = bob) =>
      Effect.flatMap(
        Effect.fromNullable(clients.get(author.agentId)),
        (client) =>
          client.send({ ...request, to: AgentAddress.make("agent:alice") }),
      ),
    /** The kinds of the records a client, Bob unless given, reads of its conversation with Alice. */
    historyOf: (client: AgentCard = bob) =>
      Effect.flatMap(Effect.fromNullable(clients.get(client.agentId)), (own) =>
        own.readHistory({ address: AgentAddress.make("agent:alice") }),
      ).pipe(Effect.map(({ records }) => records.map(({ kind }) => kind))),
    addPeer: join,
    failSends: () =>
      Effect.sync(() => {
        sendsFail = true;
      }),
  };
}).pipe(Effect.withSpan("makeHarnessFixture"));
