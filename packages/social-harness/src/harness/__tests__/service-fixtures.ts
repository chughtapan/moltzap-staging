/** @file Main-service fixtures combine real identity, policy and SQLite with controllable communication. */

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
  Redacted,
  Schedule,
  Schema,
} from "effect";
import { generateKeyPairSync } from "node:crypto";
import { join } from "node:path";
import type {
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
} from "../../firewall/check.js";
import { testAgentCard } from "../../__tests__/agents.js";
import {
  Communication,
  CommunicationError,
  type Conversation,
  type ConversationQuery,
  type HistoryPage,
  type HistoryQuery,
  type Incoming,
  Message,
  type Outgoing,
  ReceivedRequest,
  RequestError,
} from "../../communication/service.js";
import { compareAscii } from "../../encoding/identifier.js";
import { Firewall, type FirewallOptions } from "../../firewall/service.js";
import {
  AgentIdentityCardIssuedAt,
  type IdentityFixture,
  makeIdentityFixture,
  makeVerifiedCard,
} from "../../identity/__tests__/authentication-fixtures.js";
import {
  type AgentCard,
  agentCardOf,
  type AgentName,
  AgentSigningAuthority,
  type VerifiedAgentIdentityCard,
} from "../../identity/agent-cards.js";
import {
  RegistrationId,
  Registry,
  RegistryError,
} from "../../identity/registry.js";
import { Identity, type RegistrationRequest } from "../../identity/service.js";
import { type HarnessSettings, SocialHarness } from "../social-harness.js";

type Main = Context.Tag.Service<typeof SocialHarness>;
type Policy = Context.Tag.Service<typeof Firewall>;

/** Deterministic judgments still exercise the real question and advice construction. */
export function decisionResponse(
  request: DecisionRequest,
  compliance = "compliant",
  expectation = "none",
): DecisionResponse {
  return {
    provider: "test",
    model: "test",
    raw: "private model explanation",
    usage: {},
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => {
        const choices = Object.keys(question.criteria);
        let preferred = expectation;
        if (question.purpose === "subject") {
          preferred = expectation === "required" ? "relevant" : "irrelevant";
        } else if (request.stage === "compliance") {
          preferred = compliance;
        }
        const selected = choices.includes(preferred) ? preferred : "unresolved";
        return [
          id,
          {
            choice: selected,
            probabilities: Object.fromEntries(
              choices.map((choice) => [choice, Number(choice === selected)]),
            ),
          },
        ];
      }),
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

/** One natural-language rule makes assessment order and retained advice observable. */
export function firewallOptions(provider: DecisionProvider): FirewallOptions {
  return {
    mode: "enforcement",
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
          feedback: {
            text: "Keep private information private.",
            nextAction: "stop",
          },
        },
      ],
      provider,
      settings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 100_000,
        timeoutMilliseconds: 5_000,
      },
    },
  };
}

/** Await publication through the public inbox while surfacing background failures. */
export function waitForInbox(
  runtime: {
    readonly harness: Main;
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
  readonly activated: Deferred.Deferred<undefined>;
  cursorId: number;
  sendFailure?: CommunicationError;
}
interface OpenOptions extends Omit<HarnessSettings<never>, "communication"> {
  readonly registerOnStart?: boolean;
  readonly policy?: FirewallOptions;
  readonly beforeRegistration?: Effect.Effect<undefined>;
  readonly startCommunication?: Effect.Effect<undefined, CommunicationError>;
  readonly decorateFirewall?: (firewall: Policy) => Policy;
}
interface FixtureEnvironment {
  readonly identity: IdentityFixture;
  readonly filename: string;
  readonly cards: Map<AgentName, VerifiedAgentIdentityCard>;
  readonly network: Network;
  readonly communication: Context.Tag.Service<typeof Communication>;
  registryAvailable: boolean;
}

/** Main scopes share completed lower-layer history while SQLite retains Main's own work. */
export const makeHarnessFixture = Effect.gen(function* () {
  const identity = yield* makeIdentityFixture;
  const self = agentCardOf(identity.identityCard);
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
    activated: yield* Deferred.make<undefined>(),
  };
  const environment: FixtureEnvironment = {
    identity,
    network,
    filename: join(directory, "harness.sqlite3"),
    communication: communicationFor(network),
    registryAvailable: true,
    cards: new Map([[self.agentName, identity.identityCard]]),
  };
  const bob = yield* addPeer(environment, "bob");
  const carol = yield* addPeer(environment, "carol");
  const request: RegistrationRequest = {
    operationId: RegistrationId.make("opn_AAAAAAAAAAAAAAAAAAAAAA"),
    principalId: self.principalId,
    agentName: self.agentName,
  };
  return {
    self,
    bob,
    carol,
    request,
    submitted: network.submitted,
    activated: network.activated,
    open: (options: OpenOptions = {}) =>
      openHarness(environment, request, options),
    deliver: (value: Incoming) => deliver(network, value),
    addPeer: (name: string) => addPeer(environment, name),
    retain: (message: Message) =>
      Effect.sync(() => {
        retain(network, message);
      }),
    addConversation: (members: Conversation) =>
      Effect.sync(() => {
        network.catalog.set(addressOf(self, members), members);
      }),
    uncertainSend: () =>
      Effect.sync(() => {
        network.sendFailure = new CommunicationError({
          reason: "outcome-unknown",
        });
      }),
    disconnectRegistry: () =>
      Effect.sync(() => {
        environment.registryAvailable = false;
      }),
    message: (text: string, author: AgentCard = bob) =>
      Message.create({
        author,
        recipients: [self],
        content: [{ type: "text", text }],
      }),
  };
}).pipe(Effect.withSpan("makeHarnessFixture"));

function openHarness(
  environment: FixtureEnvironment,
  request: RegistrationRequest,
  options: OpenOptions,
) {
  return Effect.gen(function* () {
    const workers = yield* FiberSet.make();
    const identity = Identity.layer({
      signingAuthority: environment.identity.signingAuthority,
      registrySignerPublicKey: AgentSigningAuthority.publicKey(
        environment.identity.registryAuthority,
      ),
      admissionCredential: Effect.succeed(Redacted.make("test-admission")),
    }).pipe(
      Layer.provideMerge(registryFor(environment, options)),
      Layer.provideMerge(
        SqliteClient.layer({ filename: environment.filename }),
      ),
    );
    const services = yield* Layer.build(
      Firewall.layer(options.policy).pipe(Layer.provideMerge(identity)),
    );
    const firewall = Context.get(services, Firewall);
    const provided =
      options.decorateFirewall === undefined
        ? services
        : Context.add(services, Firewall, options.decorateFirewall(firewall));
    const context = yield* Layer.build(
      SocialHarness.layer(
        {
          maximumHistoryRecords: options.maximumHistoryRecords,
          maximumConcurrentChecks: options.maximumConcurrentChecks,
          historyExport: options.historyExport,
          hideGroups: options.hideGroups,
          hideCollectives: options.hideCollectives,
          communication: () =>
            Layer.effect(
              Communication,
              (options.startCommunication ?? Effect.void).pipe(
                Effect.zipRight(
                  Deferred.succeed(environment.network.activated, undefined),
                ),
                Effect.as(environment.communication),
              ),
            ),
        },
        workers,
      ),
    ).pipe(Effect.provide(provided));
    const harness = Context.get(context, SocialHarness);
    const local = Context.get(services, Identity);
    if (
      options.registerOnStart !== false &&
      (yield* local.me()).kind === "unregistered"
    ) {
      yield* harness.register(request);
    }
    return {
      harness,
      identity: local,
      firewall,
      failed: FiberSet.join(workers).pipe(Effect.zipRight(Effect.never)),
    };
  });
}

function registryFor(environment: FixtureEnvironment, options: OpenOptions) {
  return Layer.succeed(
    Registry,
    Registry.of({
      register: () =>
        (options.beforeRegistration ?? Effect.void).pipe(
          Effect.as({
            kind: "registered",
            identityCard: environment.identity.identityCard,
          }),
        ),
      lookup: (input) =>
        Effect.suspend(() => {
          if (!environment.registryAvailable) {
            return Effect.fail(new RegistryError({ reason: "connection" }));
          }
          const found =
            "agentName" in input
              ? environment.cards.get(input.agentName)
              : [...environment.cards.values()].find(
                  (card) => card.agentId === input.agentId,
                );
          return Effect.succeed(
            found === undefined
              ? { kind: "not_found" }
              : { kind: "found", identityCard: found },
          );
        }),
    }),
  );
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
        if (!("kind" in value)) {
          retain(network, value);
        }
        return network.sendFailure === undefined
          ? Effect.void
          : Effect.fail(network.sendFailure);
      }),
    getRequest: (id) =>
      Effect.suspend(() => {
        const found = network.requests.get(id);
        return found === undefined
          ? Effect.fail(new RequestError({ failure: { kind: "request-none" } }))
          : Effect.succeed(found);
      }),
    waitForMessage: (reference) =>
      Effect.suspend(() => {
        const found = network.messages.find(
          (message) =>
            message.author.agentId === reference.authorAgentId &&
            message.msgId === reference.msgId,
        );
        return found === undefined
          ? Effect.fail(new CommunicationError({ reason: "unknown-message" }))
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
    if (!network.catalog.has(address) && query.order !== "newest") {
      return yield* new CommunicationError({ reason: "history-gap" });
    }
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
        return yield* new CommunicationError({ reason: "unknown-message" });
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
    pendingOutgoing: false,
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
  return Effect.gen(function* () {
    const { privateKey } = generateKeyPairSync("ed25519");
    const authority = yield* AgentSigningAuthority.fromPkcs8(
      Redacted.make(privateKey.export({ format: "pem", type: "pkcs8" })),
    );
    const signed = yield* makeVerifiedCard({
      ...testAgentCard(`agent:${name}`),
      publicKey: AgentSigningAuthority.publicKey(authority),
      issuedAt: AgentIdentityCardIssuedAt.make(
        environment.identity.identityCard.issuedAt,
      ),
      registrySigningAuthority: environment.identity.registryAuthority,
    });
    environment.cards.set(signed.agentName, signed);
    return agentCardOf(signed);
  });
}

function sameNames(members: Conversation, names: readonly AgentName[]) {
  return members.every((member) => names.includes(member.agentName));
}
