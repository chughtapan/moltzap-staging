/** @file SocialHarness fixtures combine real policy and SQLite with controllable identity and communication. */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import {
  Context,
  Deferred,
  Effect,
  FiberSet,
  Layer,
  Queue,
  Schedule,
  Schema,
} from "effect";
import { join } from "node:path";
import type {
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
} from "../../firewall/index.js";
import { testAgentCard } from "../../__tests__/agents.js";
import {
  CommunicationError,
  Message,
  ReceivedRequest,
  RequestError,
} from "../../communication/index.js";
import {
  Communication,
  type Conversation,
  type ConversationQuery,
  type HistoryPage,
  type HistoryQuery,
  type Incoming,
  type Outgoing,
} from "../../communication/service.js";
import { migrateDaemon } from "../../database/index.js";
import { compareAscii } from "../../encoding.js";
import { Firewall, type FirewallOptions } from "../../firewall/service.js";
import {
  type AgentCard,
  type AgentName,
  Identity,
  IdentityError,
} from "../../identity.js";
import { type HarnessSettings, SocialHarness } from "../social-harness.js";

type Harness = Context.Tag.Service<typeof SocialHarness>;
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
    provider: "test",
    model: "test",
    raw: "private model explanation",
    usage: {},
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => [
        id,
        {
          choice: compliance,
          probabilities: Object.fromEntries(
            Object.keys(question.criteria).map((choice) => [
              choice,
              choice === compliance
                ? probability
                : (1 - probability) /
                  (Object.keys(question.criteria).length - 1),
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

/**
 * One natural-language rule makes assessment order and retained advice
 * observable. With `action`, the rule replies to or silently withholds the
 * incoming messages it rejects.
 */
export function firewallOptions(
  provider: DecisionProvider,
  action?: "deny" | "reply",
): FirewallOptions {
  return {
    facts: [],
    settings: {
      timeoutMilliseconds: 10_000,
      maximumInputBytes: 1_000_000,
      maximumResultBytes: 100_000,
    },
    text: {
      initialRules: [
        {
          id: "privacy",
          applies: "both",
          text: "Protect the owner's private information.",
          guidance: "Check before sharing private information.",
          feedback: { text: "Keep private information private." },
          ...(action === undefined ? {} : { action }),
        },
      ],
      provider,
      settings: {
        minimumProbability: 0.8,
        maximumRequestBytes: 100_000,
        timeoutMilliseconds: 5_000,
      },
    },
  };
}

/** Await publication through the public inbox while surfacing background failures. */
export function waitForInbox(
  runtime: {
    readonly harness: Harness;
    readonly failed: Effect.Effect<never, unknown>;
  },
  count = 1,
) {
  return runtime.harness.listInbox().pipe(
    Effect.repeat({
      until: (page) => page.items.length >= count,
      schedule: Schedule.spaced("5 millis"),
    }),
    Effect.zipRight(runtime.harness.listInbox()),
    Effect.raceFirst(runtime.failed),
    Effect.timeout("10 seconds"),
  );
}

interface HistoryPosition {
  readonly messages: readonly Message[];
  readonly after: number;
  readonly address: string;
}
interface Intake {
  readonly value: Incoming;
  readonly accepted: Deferred.Deferred<undefined>;
}
interface Network {
  readonly self: AgentCard;
  readonly messages: Message[];
  readonly submitted: Outgoing[];
  readonly requests: Map<string, ReceivedRequest>;
  readonly catalog: Map<string, Conversation>;
  readonly historyPositions: Map<string, HistoryPosition>;
  readonly catalogPositions: Map<string, string>;
  readonly incoming: Queue.Queue<Intake>;
  cursorId: number;
  sendFailure?: CommunicationError;
}
interface OpenOptions extends Omit<HarnessSettings, "agentKey"> {
  readonly policy?: FirewallOptions;
  readonly decorateFirewall?: (firewall: Policy) => Policy;
}
interface FixtureEnvironment {
  readonly filename: string;
  /** The agents `resolve` finds by name. */
  readonly cards: Map<AgentName, AgentCard>;
  readonly network: Network;
  readonly communication: Context.Tag.Service<typeof Communication>;
}

/** Each opened harness shares the fixture's communication history; SQLite keeps the harness's own inbox and decisions. */
export const makeHarnessFixture = Effect.gen(function* () {
  const self = testAgentCard("agent:alice");
  const directory = yield* Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.makeTempDirectoryScoped({ prefix: "sharness-main-" });
  }).pipe(Effect.provide(NodeFileSystem.layer));
  const network: Network = {
    self,
    messages: [],
    submitted: [],
    requests: new Map(),
    catalog: new Map(),
    historyPositions: new Map(),
    catalogPositions: new Map(),
    cursorId: 0,
    incoming: yield* Queue.unbounded<Intake>(),
  };
  const environment: FixtureEnvironment = {
    network,
    filename: join(directory, "harness.sqlite3"),
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
    deliver: (value: Incoming) => deliver(network, value),
    addPeer: (name: string) => Effect.sync(() => addPeer(environment, name)),
    retain: (message: Message) =>
      Effect.sync(() => {
        retain(network, message);
      }),
    addConversation: (members: Conversation) =>
      Effect.sync(() => {
        network.catalog.set(addressOf(self, members), members);
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
    const workers = yield* FiberSet.make();
    const identity = Layer.succeed(Identity, identityFor(environment)).pipe(
      Layer.provideMerge(
        Layer.effectDiscard(migrateDaemon).pipe(
          Layer.provideMerge(
            SqliteClient.layer({ filename: environment.filename }),
          ),
        ),
      ),
    );
    const services = yield* Layer.build(
      Firewall.layer(environment.filename, options.policy).pipe(
        Layer.provideMerge(identity),
      ),
    );
    const firewall = Context.get(services, Firewall);
    const provided =
      options.decorateFirewall === undefined
        ? services
        : Context.add(services, Firewall, options.decorateFirewall(firewall));
    const context = yield* Layer.build(
      SocialHarness.layer(
        {
          agentKey: environment.filename,
          maximumHistoryRecords: options.maximumHistoryRecords,
          maximumConcurrentChecks: options.maximumConcurrentChecks,
          hideGroups: options.hideGroups,
          ...(options.withheldReply === undefined
            ? {}
            : { withheldReply: options.withheldReply }),
        },
        workers,
      ),
    ).pipe(
      Effect.provide(provided),
      Effect.provideService(Communication, environment.communication),
    );
    const harness = Context.get(context, SocialHarness);
    return {
      harness,
      firewall,
      failed: FiberSet.join(workers).pipe(Effect.zipRight(Effect.never)),
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
    getConversation: (names) =>
      Effect.sync(() =>
        [...network.catalog.values()].find(
          (members) =>
            members.length === names.length && sameNames(members, names),
        ),
      ),
    send: (value) =>
      Effect.suspend(() => {
        network.submitted.push(value);
        if (network.sendFailure !== undefined) {
          return Effect.fail(network.sendFailure);
        }
        if (!("kind" in value)) {
          retain(network, value);
        }
        return Effect.void;
      }),
    getRequest: (id) =>
      Effect.suspend(() => {
        const found = network.requests.get(id);
        return found === undefined
          ? Effect.fail(new RequestError({ failure: { kind: "request-none" } }))
          : Effect.succeed(found);
      }),
    consume: (handle) =>
      Queue.take(network.incoming).pipe(
        Effect.flatMap(({ value, accepted }) =>
          handle(value).pipe(
            Effect.zipRight(Deferred.succeed(accepted, undefined)),
          ),
        ),
        Effect.forever,
      ),
    listConversations: (query = {}) => listConversations(network, query),
    readHistory: (query) => readHistory(network, query),
  });
}

function listConversations(network: Network, query: ConversationQuery) {
  return Effect.suspend(() => {
    const after =
      query.cursor === undefined
        ? ""
        : network.catalogPositions.get(query.cursor);
    if (after === undefined) {
      return Effect.fail(new CommunicationError({ reason: "invalid-cursor" }));
    }
    const available = [...network.catalog]
      .filter(([address]) => address > after)
      .sort(([left], [right]) => compareAscii(left, right));
    const selected = available.slice(0, 50);
    const cursor =
      available.length > selected.length
        ? `catalog-${++network.cursorId}`
        : undefined;
    const last = selected.at(-1);
    if (cursor !== undefined && last !== undefined) {
      network.catalogPositions.set(cursor, last[0]);
    }
    return Effect.succeed({
      conversations: selected.map(([, members]) => members),
      ...(cursor === undefined ? {} : { nextCursor: cursor }),
    });
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

function deliver(network: Network, value: Incoming) {
  return Effect.gen(function* () {
    if (value.kind === "message" || value.kind === "request") {
      retain(network, value.message);
    } else if (value.kind === "result") {
      for (const message of [
        ...(value.sources.request === undefined ? [] : [value.sources.request]),
        ...value.sources.answers,
      ]) {
        retain(network, message);
      }
    }
    const accepted = yield* Deferred.make<undefined>();
    yield* Queue.offer(network.incoming, { value, accepted });
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
  network.catalog.set(addressOf(network.self, members), members);
  if (message.operation?.kind === "request") {
    const request = Schema.decodeUnknownSync(ReceivedRequest)(message);
    network.requests.set(request.operation.operationId, request);
  }
}

function membersOf(
  message: Pick<Message, "author" | "recipients">,
): Conversation {
  const [first, second, ...rest] = [message.author, ...message.recipients].sort(
    (left, right) => compareAscii(left.agentName, right.agentName),
  );
  if (first === undefined || second === undefined) {
    throw new Error("A fixture conversation needs two members");
  }
  return [first, second, ...rest];
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

function sameNames(members: Conversation, names: readonly AgentName[]) {
  return members.every((member) => names.includes(member.agentName));
}
