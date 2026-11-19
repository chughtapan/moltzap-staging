/** @file SocialHarness fixtures combine real policy and a real database with controllable identity and communication. */

import { SqlClient } from "@effect/sql";
import {
  Context,
  Deferred,
  Effect,
  Layer,
  Queue,
  Schedule,
  Schema,
} from "effect";
import type {
  CompiledContract,
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
  LoadedMessagePolicy,
} from "../../firewall/index.js";
import { testAgentCard } from "../../__tests__/agents.js";
import { CommunicationError, Message } from "../../communication/index.js";
import {
  Communication,
  type HistoryPage,
  type HistoryQuery,
} from "../../communication/service.js";
import { compareAscii } from "../../encoding.js";
import { Firewall } from "../../firewall/service.js";
import {
  type AgentCard,
  type AgentName,
  Identity,
  IdentityError,
} from "../../identity.js";
import { openTestDatabase, type TestDatabase } from "../../test.js";
import { type HarnessSettings, SocialHarness } from "../agent.js";
import { type Inbox, makeInbox } from "../inbox.js";

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

interface HistoryPosition {
  readonly messages: readonly Message[];
  readonly after: number;
  readonly address: string;
}
interface Intake {
  readonly message: Message;
  readonly accepted: Deferred.Deferred<undefined>;
}
interface Network {
  readonly self: AgentCard;
  readonly messages: Message[];
  readonly submitted: Message[];
  readonly conversations: Map<string, readonly AgentCard[]>;
  readonly historyPositions: Map<string, HistoryPosition>;
  readonly incoming: Queue.Queue<Intake>;
  cursorId: number;
  sendFailure?: CommunicationError;
}
/** How a harness opens. */
interface OpenOptions extends Omit<HarnessSettings, "agentKey"> {
  readonly policy?: TestPolicy;
  /** The contracts the agent starts with. */
  readonly contracts?: readonly CompiledContract[];
  readonly decorateFirewall?: (firewall: Policy) => Policy;
}
/** Every harness a fixture opens stores its rows under this key. */
const agentKey = "alice";

interface FixtureEnvironment {
  readonly database: TestDatabase;
  /** The agents `resolve` finds by name. */
  readonly cards: Map<AgentName, AgentCard>;
  readonly network: Network;
  readonly communication: Context.Tag.Service<typeof Communication>;
}

/** Each opened harness shares the fixture's communication history; the database keeps the harness's own inbox and decisions. */
export const makeHarnessFixture = Effect.gen(function* () {
  const self = testAgentCard("agent:alice");
  const network: Network = {
    self,
    messages: [],
    submitted: [],
    conversations: new Map(),
    historyPositions: new Map(),
    cursorId: 0,
    incoming: yield* Queue.unbounded<Intake>(),
  };
  const environment: FixtureEnvironment = {
    network,
    database: yield* openTestDatabase(),
    communication: communicationFor(network),
    cards: new Map([[self.agentName, self]]),
  };
  const bob = addPeer(environment, "bob");
  const carol = addPeer(environment, "carol");
  return {
    self,
    bob,
    carol,
    submitted: network.submitted,
    open: (options: OpenOptions = {}) => openHarness(environment, options),
    deliver: (message: Message) => deliver(network, message),
    addPeer: (name: string) => Effect.sync(() => addPeer(environment, name)),
    retain: (message: Message) =>
      Effect.sync(() => {
        retain(network, message);
      }),
    addConversation: (members: readonly AgentCard[]) =>
      Effect.sync(() => {
        network.conversations.set(addressOf(self, members), members);
      }),
    failSends: () =>
      Effect.sync(() => {
        network.sendFailure = new CommunicationError({
          reason: "persistence-failed",
        });
      }),
    message: (text: string, author: AgentCard = bob) =>
      Message.create({
        author,
        recipients: [self],
        content: [{ type: "text", text }],
      }),
  };
}).pipe(Effect.withSpan("makeHarnessFixture"));

function openHarness(environment: FixtureEnvironment, options: OpenOptions) {
  return Effect.gen(function* () {
    const identity = Layer.succeed(Identity, identityFor(environment)).pipe(
      Layer.provideMerge(
        Layer.succeed(SqlClient.SqlClient, environment.database.sql),
      ),
    );
    const services = yield* Layer.build(
      Firewall.layer(
        agentKey,
        options.policy?.source,
        options.contracts,
        options.policy?.provider,
      ).pipe(Layer.provideMerge(identity)),
    );
    const firewall = Context.get(services, Firewall);
    const provided =
      options.decorateFirewall === undefined
        ? services
        : Context.add(services, Firewall, options.decorateFirewall(firewall));
    const context = yield* Layer.build(
      SocialHarness.layer({
        agentKey,
        maximumHistoryRecords: options.maximumHistoryRecords,
        maximumConcurrentChecks: options.maximumConcurrentChecks,
        ...(options.withheldReply === undefined
          ? {}
          : { withheldReply: options.withheldReply }),
      }),
    ).pipe(
      Effect.provide(provided),
      Effect.provideService(Communication, environment.communication),
    );
    const harness = Context.get(context, SocialHarness);
    return {
      harness,
      inbox: yield* makeInbox(agentKey).pipe(Effect.provide(services)),
      firewall,
    };
  });
}

/** Resolves the fixture's peers by name. */
function identityFor(environment: FixtureEnvironment) {
  return Identity.of({
    me: environment.network.self,
    resolve: (names) =>
      Effect.forEach(
        names,
        (agentName) =>
          Effect.suspend(() => {
            const found = environment.cards.get(agentName);
            return found === undefined
              ? Effect.fail(
                  new IdentityError({ reason: "unknown-agent", agentName }),
                )
              : Effect.succeed(found);
          }),
        { concurrency: 1 },
      ),
  });
}

function communicationFor(network: Network) {
  return Communication.of({
    send: (message) =>
      Effect.suspend(() => {
        network.submitted.push(message);
        if (network.sendFailure !== undefined) {
          return Effect.fail(network.sendFailure);
        }
        retain(network, message);
        return Effect.void;
      }),
    consume: (handle) =>
      Queue.take(network.incoming).pipe(
        Effect.flatMap(({ message, accepted }) =>
          handle(message).pipe(
            Effect.zipRight(Deferred.succeed(accepted, undefined)),
          ),
        ),
        Effect.forever,
      ),
    listConversations: Effect.sync(() => [...network.conversations.values()]),
    readHistory: (query) => readHistory(network, query),
  });
}

function readHistory(network: Network, query: HistoryQuery) {
  return Effect.gen(function* () {
    const address = addressOf(network.self, query.members);
    const all = network.messages.filter(
      (message) => addressOf(network.self, membersOf(message)) === address,
    );
    if (query.order === "newest") {
      const before = query.beforeMessage;
      const position =
        before === undefined
          ? all.length
          : all.findIndex(
              (message) =>
                message.author.agentId === before.authorAgentId &&
                message.msgId === before.msgId,
            );
      if (position < 0) {
        return yield* new CommunicationError({ reason: "content-invalid" });
      }
      const available = all.slice(0, position);
      return historyPage(
        available.slice(-query.limit),
        available.length > query.limit,
      );
    }
    const position =
      query.cursor === undefined
        ? { messages: all.slice(), after: 0, address }
        : network.historyPositions.get(query.cursor);
    if (position === undefined || position.address !== address) {
      return yield* new CommunicationError({ reason: "invalid-cursor" });
    }
    const selected = position.messages.slice(
      position.after,
      position.after + 50,
    );
    const after = position.after + selected.length;
    const cursor =
      after < position.messages.length
        ? `history-${++network.cursorId}`
        : undefined;
    if (cursor !== undefined) {
      network.historyPositions.set(cursor, { ...position, after });
    }
    return historyPage(selected, false, cursor);
  });
}

function historyPage(
  selected: readonly Message[],
  earlierMessagesOmitted: boolean,
  nextCursor?: string,
): HistoryPage {
  return {
    entries: selected.map((message) => ({
      message,
      messageBytes: new TextEncoder().encode(JSON.stringify(message))
        .byteLength,
    })),
    earlierMessagesOmitted,
    ...(nextCursor === undefined ? {} : { nextCursor }),
  };
}

function deliver(network: Network, message: Message) {
  return Effect.gen(function* () {
    retain(network, message);
    const accepted = yield* Deferred.make<undefined>();
    yield* Queue.offer(network.incoming, { message, accepted });
    yield* Deferred.await(accepted);
  });
}

function retain(network: Network, message: Message) {
  if (
    !network.messages.some(
      (existing) =>
        existing.author.agentId === message.author.agentId &&
        existing.msgId === message.msgId,
    )
  ) {
    network.messages.push(message);
  }
  const members = membersOf(message);
  network.conversations.set(addressOf(network.self, members), members);
}

function membersOf(
  message: Pick<Message, "author" | "recipients">,
): readonly AgentCard[] {
  return [message.author, ...message.recipients].sort((left, right) =>
    compareAscii(left.agentName, right.agentName),
  );
}

function addressOf(self: AgentCard, members: readonly AgentCard[]) {
  const others = members.filter((member) => member.agentId !== self.agentId);
  return others.length === 1
    ? `agent:${others[0]?.agentName}`
    : `group:${members
        .map((member) => member.agentName)
        .sort(compareAscii)
        .join(",")}`;
}

function addPeer(environment: FixtureEnvironment, name: string) {
  const card = testAgentCard(`agent:${name}`);
  environment.cards.set(card.agentName, card);
  return card;
}
