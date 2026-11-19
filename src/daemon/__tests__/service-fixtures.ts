/** @file SocialHarness fixtures: one agent's harness over real policy, identity and communication in a test database, with the peers that message it. */

import { Context, Effect, flow, Layer, Schedule, Schema, Scope } from "effect";
import type {
  CompiledContract,
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
  LoadedMessagePolicy,
} from "../../firewall/index.js";
import type { AgentCard } from "../../identity.js";
import { createTestAgent } from "../../__tests__/agents.js";
import { CommunicationError, Message } from "../../communication/index.js";
import { Communication } from "../../communication/service.js";
import { Firewall } from "../../firewall/service.js";
import { openTestDatabase } from "../../test.js";
import { type HarnessSettings, SocialHarness } from "../agent.js";
import { type Inbox, makeInbox } from "../inbox.js";
import { identityLayer } from "../owners.js";

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
 * incoming messages it rejects. `maximumConcurrentChecks` bounds how many
 * checks run at once.
 */
export function composedPolicy(
  provider: DecisionProvider,
  action?: "deny" | "reply",
  maximumConcurrentChecks?: number,
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
        maximumConcurrentChecks,
      },
    },
  };
}

/** Waits until the inbox holds `count` entries, and reads every entry it holds in the order they were allowed. */
export function waitForInbox(
  runtime: {
    readonly inbox: Inbox;
  },
  count = 1,
) {
  const read = runtime.inbox.listInbox(1000, "0");
  return read.pipe(
    Effect.repeat({
      until: (page) => page.entries.length >= count,
      schedule: Schedule.spaced("5 millis"),
    }),
    Effect.zipRight(read),
    Effect.timeout("10 seconds"),
  );
}

/** How a harness opens. */
interface OpenOptions extends Omit<HarnessSettings, "agentKey"> {
  readonly policy?: TestPolicy;
  /** The contracts the agent starts with. */
  readonly contracts?: readonly CompiledContract[];
  readonly decorateFirewall?: (firewall: Policy) => Policy;
}

/**
 * Alice, whose harness a test opens, with Bob, Carol and any peer a test adds,
 * on one daemon. `submitted` lists what Alice's harness handed to
 * communication, and after `failSends` communication stores none of it.
 */
export const makeHarnessFixture = Effect.gen(function* () {
  const database = yield* openTestDatabase();
  const scope = yield* Effect.scope;
  const consumers = new Map<string, Effect.Latch>();
  const submitted: Message[] = [];
  let sendsFail = false;
  const senders = new Map<string, Context.Tag.Service<typeof Communication>>();
  /** Creates an agent that sends through a communication service of its own, which closes with the fixture. */
  const join = Effect.fn(
    function* (name: string) {
      const agent = yield* createTestAgent(name);
      const services = yield* Layer.build(
        Communication.layer({ ...agent, consumers }),
      );
      senders.set(agent.card.agentId, Context.get(services, Communication));
      return agent;
    },
    flow(database.run, Scope.extend(scope)),
  );
  const alice = yield* join("alice");
  const bob = (yield* join("bob")).card;
  const carol = (yield* join("carol")).card;
  /** Alice's communication, recording each send and failing it once sends fail. */
  const communication = Layer.effect(
    Communication,
    Effect.map(Communication, (real) => ({
      ...real,
      send: (message: Message) =>
        Effect.suspend(() => {
          submitted.push(message);
          return sendsFail
            ? Effect.fail(
                new CommunicationError({ reason: "persistence-failed" }),
              )
            : real.send(message);
        }),
    })),
  ).pipe(Layer.provide(Communication.layer({ ...alice, consumers })));
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
    return {
      harness: Context.get(context, SocialHarness),
      inbox: yield* makeInbox(alice.agentKey),
      firewall,
    };
  }, database.run);
  return {
    self: alice.card,
    bob,
    carol,
    submitted,
    open,
    /** Sends `message` as its author, past the author's firewall. */
    deliver: (message: Message) =>
      Effect.flatMap(
        Effect.fromNullable(senders.get(message.author.agentId)),
        (sender) => sender.send(message),
      ),
    addPeer: (name: string) => Effect.map(join(name), (peer) => peer.card),
    failSends: () =>
      Effect.sync(() => {
        sendsFail = true;
      }),
    message: (text: string, author: AgentCard = bob) =>
      Message.create({
        author,
        recipients: [alice.card],
        content: [{ type: "text", text }],
      }),
  };
}).pipe(Effect.withSpan("makeHarnessFixture"));
