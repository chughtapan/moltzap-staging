/** @file Host history lists messages first, then saved local outcomes within a fixed inbox boundary. */

import { SqlClient } from "@effect/sql";
import { type Context, Data, Effect, Option, Schema } from "effect";
import type {
  Communication,
  CommunicationError,
  ConversationPage,
  HistoryQuery,
} from "../../communication/index.js";
import type { Message } from "../../communication/message.js";
import type {
  ManagementReadConversationRequest,
  ManagementReadConversationResult,
  ManagementSearchConversationsRequest,
  ManagementSearchConversationsResult,
} from "../../host-protocol/daemon-tools.js";
import type { Identity } from "../../identity/local.js";
import { readCollectiveValue } from "../../communication/collectives/index.js";
import {
  AgentAddress,
  GroupAddress,
} from "../../communication/records/values.js";
import { compareAscii } from "../../encoding/identifier.js";
import {
  listLocalResultConversations,
  localResultBoundary,
  readLocalResultPage,
} from "../inbox/history.js";
import { type HistoryView, readHistoryViews } from "../inbox/store.js";

/** History failures name the unavailable view without exposing protocol or database details. */
export class HostHistoryError extends Data.TaggedError("HostHistoryError")<{
  readonly reason:
    | "dependency-unavailable"
    | "history-gap"
    | "incompatible-daemon"
    | "invalid-address"
    | "invalid-continuation"
    | "not-registered"
    | "persistence-failed"
    | "unknown-agent";
}> {}

type Conversation = ConversationPage["conversations"][number];
type Address = ManagementSearchConversationsResult["addresses"][number];
type HistoryRecord = ManagementReadConversationResult["records"][number];
const addressSchema = Schema.Union(AgentAddress, GroupAddress);
const cursor = Schema.Union(
  Schema.Struct({
    phase: Schema.Literal("messages"),
    address: addressSchema,
    communication: Schema.NonEmptyString,
    through: Schema.NonNegativeInt,
  }),
  Schema.Struct({
    phase: Schema.Literal("results"),
    address: addressSchema,
    after: Schema.NonNegativeInt,
    through: Schema.NonNegativeInt,
  }),
);
type Cursor = typeof cursor.Type;
type HistoryState =
  | {
      readonly phase: "messages";
      readonly address: Address;
      readonly query: HistoryQuery;
      readonly through: number;
    }
  | Extract<Cursor, { phase: "results" }>;
const encodeCursor = Schema.encodeSync(Schema.parseJson(cursor));
const decodeCursor = Schema.decodeUnknown(Schema.parseJson(cursor));
const CURSOR_PREFIX = "host-history:";

/** Each traversal retains its communication cursor and local-result sequence independently. */
export function makeHistoryOperations(
  identity: Context.Tag.Service<typeof Identity>,
  communication: Context.Tag.Service<typeof Communication>,
  sql: SqlClient.SqlClient,
) {
  const provideSql = Effect.provideService(SqlClient.SqlClient, sql);
  return {
    listConversations: (request: ManagementSearchConversationsRequest) =>
      listConversations(identity, communication, request).pipe(provideSql),
    readHistory: (request: ManagementReadConversationRequest) =>
      readHistory(identity, communication, request).pipe(provideSql),
  };
}

function listConversations(
  identity: Context.Tag.Service<typeof Identity>,
  communication: Context.Tag.Service<typeof Communication>,
  request: ManagementSearchConversationsRequest,
): Effect.Effect<
  ManagementSearchConversationsResult,
  HostHistoryError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const me = yield* localAgent(identity);
    if (request.afterAddress !== undefined) {
      yield* validateAddress(request.afterAddress, me);
    }
    const conversations = yield* allConversations(communication);
    const addresses = conversations
      .map((conversation) => conversationAddress(conversation, me))
      .sort(compareAscii);
    if (
      addresses.some(
        (address, index) => index > 0 && address === addresses[index - 1],
      )
    ) {
      return yield* new HostHistoryError({ reason: "history-gap" });
    }
    const results = yield* listLocalResultConversations(
      request.afterAddress,
    ).pipe(Effect.catchTag("EndpointStoreError", storeFailure));
    const resultAddresses = yield* Schema.decodeUnknown(
      Schema.Array(addressSchema),
    )(results.addresses).pipe(
      Effect.catchTag(
        "ParseError",
        () => new HostHistoryError({ reason: "history-gap" }),
      ),
    );
    const merged = [...new Set([...addresses, ...resultAddresses])].sort(
      compareAscii,
    );
    const remaining =
      request.afterAddress === undefined
        ? merged
        : merged.filter(
            (address) => compareAscii(address, request.afterAddress ?? "") > 0,
          );
    return {
      kind: "page",
      addresses: remaining.slice(0, 50),
      hasMore: results.hasMore || remaining.length > 50,
    };
  });
}

function readHistory(
  identity: Context.Tag.Service<typeof Identity>,
  communication: Context.Tag.Service<typeof Communication>,
  request: ManagementReadConversationRequest,
): Effect.Effect<
  ManagementReadConversationResult,
  HostHistoryError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const me = yield* localAgent(identity);
    const state = yield* historyState(communication, me, request);
    if (state.phase === "results") {
      return yield* resultsPage(state, [], 50);
    }
    const page = yield* communication
      .readHistory(state.query)
      .pipe(Effect.mapError(communicationFailure));
    const views = yield* readHistoryViews(
      page.messages
        .filter((message) => message.author.agentId !== me.agentId)
        .map((message) => ({
          authorAgentId: message.author.agentId,
          msgId: message.msgId,
        })),
    ).pipe(Effect.catchTag("EndpointStoreError", storeFailure));
    const records = yield* historyRecords(me, page.messages, views);
    if (page.continuation !== null) {
      return {
        kind: "page",
        records,
        continuation: continuation({
          phase: "messages",
          address: state.address,
          communication: page.continuation,
          through: state.through,
        }),
      };
    }
    return yield* resultsPage(
      {
        phase: "results",
        address: state.address,
        after: 0,
        through: state.through,
      },
      records,
      50 - records.length,
    );
  });
}

function resultsPage(
  state: Extract<Cursor, { phase: "results" }>,
  records: readonly HistoryRecord[],
  limit: number,
): Effect.Effect<
  ManagementReadConversationResult,
  HostHistoryError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    if (state.through === 0) {
      return { kind: "page", records, continuation: null };
    }
    if (limit === 0) {
      return { kind: "page", records, continuation: continuation(state) };
    }
    const page = yield* readLocalResultPage({
      conversation: state.address,
      after: state.after,
      through: state.through,
      limit,
    }).pipe(Effect.catchTag("EndpointStoreError", storeFailure));
    const results: HistoryRecord[] = [];
    for (const view of page.items) {
      const { item, guidance } = view;
      if (item.kind !== "collectiveResult" && item.kind !== "operationFailed") {
        return yield* new HostHistoryError({ reason: "history-gap" });
      }
      results.push({
        kind: "result",
        deliveryToken: view.deliveryToken,
        item,
        ...(guidance === undefined ? {} : { guidance }),
      });
    }
    return {
      kind: "page",
      records: [...records, ...results],
      continuation:
        page.nextAfter === undefined
          ? null
          : continuation({ ...state, after: page.nextAfter }),
    };
  });
}

function localAgent(identity: Context.Tag.Service<typeof Identity>) {
  return identity.me().pipe(
    Effect.catchTag(
      "IdentityError",
      (error) => new HostHistoryError({ reason: error.reason }),
    ),
    Effect.flatMap((status) =>
      status.kind === "registered"
        ? Effect.succeed(status.agent)
        : new HostHistoryError({ reason: "not-registered" }),
    ),
  );
}

function allConversations(
  communication: Context.Tag.Service<typeof Communication>,
) {
  return Effect.gen(function* () {
    const conversations: Conversation[] = [];
    let afterConversationId: string | undefined;
    for (;;) {
      const page = yield* communication
        .listConversations(
          afterConversationId === undefined ? {} : { afterConversationId },
        )
        .pipe(Effect.mapError(communicationFailure));
      conversations.push(...page.conversations);
      if (!page.hasMore) {
        return conversations;
      }
      const last = page.conversations.at(-1);
      if (last === undefined) {
        return yield* new HostHistoryError({ reason: "history-gap" });
      }
      afterConversationId = last.conversationId;
    }
  });
}

function conversationAddress(
  conversation: Conversation,
  me: Message["author"],
): Address {
  const peers = conversation.members.filter(
    (member) => member.agentId !== me.agentId,
  );
  const [peer] = peers;
  if (peers.length === 1 && peer !== undefined) {
    return AgentAddress.make(`agent:${peer.agentName}`);
  }
  return GroupAddress.make(
    `group:${conversation.members
      .map((member) => member.agentName)
      .sort(compareAscii)
      .join(",")}`,
  );
}

function historyState(
  communication: Context.Tag.Service<typeof Communication>,
  me: Message["author"],
  request: ManagementReadConversationRequest,
): Effect.Effect<HistoryState, HostHistoryError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    if ("continuation" in request) {
      const saved = yield* parseContinuation(request.continuation);
      return saved.phase === "results"
        ? saved
        : {
            phase: "messages",
            address: saved.address,
            query: { continuation: saved.communication },
            through: saved.through,
          };
    }
    yield* validateAddress(request.address, me);
    const through = yield* localResultBoundary(request.address).pipe(
      Effect.catchTag("EndpointStoreError", storeFailure),
    );
    const conversations = yield* allConversations(communication);
    const matches = conversations.filter(
      (candidate) => conversationAddress(candidate, me) === request.address,
    );
    if (matches.length > 1) {
      return yield* new HostHistoryError({ reason: "history-gap" });
    }
    const conversation = matches[0];
    if (conversation !== undefined) {
      return {
        phase: "messages",
        address: request.address,
        query: { conversationId: conversation.conversationId },
        through,
      };
    }
    if (through > 0) {
      return { phase: "results", address: request.address, after: 0, through };
    }
    return yield* new HostHistoryError({ reason: "invalid-address" });
  });
}

function parseContinuation(value: string) {
  if (!value.startsWith(CURSOR_PREFIX)) {
    return Effect.fail(
      new HostHistoryError({ reason: "invalid-continuation" }),
    );
  }
  return decodeCursor(
    Buffer.from(value.slice(CURSOR_PREFIX.length), "base64url").toString(
      "utf8",
    ),
  ).pipe(
    Effect.catchTag(
      "ParseError",
      () => new HostHistoryError({ reason: "invalid-continuation" }),
    ),
  );
}

function continuation(value: Cursor) {
  return CURSOR_PREFIX + Buffer.from(encodeCursor(value)).toString("base64url");
}

function validateAddress(address: Address, me: Message["author"]) {
  const names = address.slice(address.indexOf(":") + 1).split(",");
  const valid = address.startsWith("agent:")
    ? names[0] !== me.agentName
    : names.includes(me.agentName);
  return valid
    ? Effect.void
    : new HostHistoryError({ reason: "invalid-address" });
}

function historyRecords(
  me: Message["author"],
  messages: readonly Message[],
  views: readonly HistoryView[],
) {
  return Effect.gen(function* () {
    const records: HistoryRecord[] = [];
    const tokens = new Set<string>();
    for (const message of messages) {
      if (message.author.agentId === me.agentId) {
        if (yield* visibleSent(message)) {
          records.push({ kind: "sent", message });
        }
        continue;
      }
      const view = views.find(
        (candidate) =>
          candidate.message.authorAgentId === message.author.agentId &&
          candidate.message.msgId === message.msgId,
      );
      if (view !== undefined && !tokens.has(view.deliveryToken)) {
        tokens.add(view.deliveryToken);
        const { item, guidance } = view;
        if (item.kind === "multicast" || item.kind === "collectiveRequest") {
          records.push({
            kind: "received",
            message: view.message,
            item,
            ...(guidance === undefined ? {} : { guidance }),
          });
        }
      }
    }
    return records;
  });
}

function visibleSent(message: Message) {
  return readCollectiveValue(message.content).pipe(
    Effect.map((value) => Option.isNone(value) || value.value.kind !== "close"),
    Effect.catchTag("CollectivePartInvalidError", () => Effect.succeed(false)),
  );
}

function communicationFailure(error: CommunicationError): HostHistoryError {
  const reason = error.reason;
  if (
    reason === "not-registered" ||
    reason === "invalid-continuation" ||
    reason === "history-gap"
  ) {
    return new HostHistoryError({ reason });
  }
  if (reason === "persistence-failed" || reason === "unknown-agent") {
    return new HostHistoryError({ reason });
  }
  return new HostHistoryError({ reason: "dependency-unavailable" });
}

function storeFailure() {
  return new HostHistoryError({ reason: "persistence-failed" });
}
