/** @file Host messaging checks captured commands and publishes retained incoming views. */

import { SqlClient } from "@effect/sql";
import {
  Context,
  Deferred,
  Effect,
  FiberSet,
  Layer,
  Option,
  PubSub,
  Queue,
  Ref,
  Scope,
  Stream,
} from "effect";
import type { DeliveryOperations } from "../../host-protocol/operations.js";
import type { Ed25519PublicKey } from "../../identity/keys.js";
import type { FirewallEnvironment } from "../inbox/firewall-context.js";
import type { HistoryExportPort } from "../inbox/host-traffic-log.js";
import type { EventStore } from "../inbox/store.js";
import {
  Collectives,
  type ResultDelivery,
} from "../../communication/collectives/index.js";
import { Communication } from "../../communication/index.js";
import { type Message, replyAddress } from "../../communication/message.js";
import {
  type DeliveryToken,
  encodeRuntimeValue,
  EndpointStoreError,
} from "../../communication/store/index.js";
import { Firewall } from "../../firewall/index.js";
import { Identity, LocalIdentity } from "../../identity/local.js";
import { mintLocalDeliveryToken } from "../inbox/inbox.js";
import {
  type AdmissionContext,
  runAdmission,
} from "../inbox/incoming-worker.js";
import { findIncomingMessage, queueIncomingItem } from "../inbox/pending.js";
import { makeHistoryOperations } from "./history.js";
import { makeInboxOperations } from "./inbox.js";
import { type SendContext, sendOperation } from "./send.js";

/** Checks sends and preserves the messages and results the agent may read. */
export class HostMessaging extends Context.Tag("@moltzap/HostMessaging")<
  HostMessaging,
  {
    /** Success retains accepted work for delivery across restart. */
    readonly send: DeliveryOperations["send"];
    /** Reading leaves items pending until acknowledgment. */
    readonly listInbox: DeliveryOperations["readInbox"];
    /** Callback lookups return the saved item even after acknowledgment. */
    readonly getInboxItem: (
      token: DeliveryToken,
    ) => ReturnType<DeliveryOperations["readEvent"]>;
    /** Ends pending delivery while retaining the item for later reads. */
    readonly acknowledgeDelivery: DeliveryOperations["acknowledgeDelivery"];
    /** Includes conversations with saved local collective results. */
    readonly listConversations: ReturnType<
      typeof makeHistoryOperations
    >["listConversations"];
    /** Returns messages followed by saved local results within a fixed traversal. */
    readonly readHistory: ReturnType<
      typeof makeHistoryOperations
    >["readHistory"];
    /** Signals new saved items. Reading the inbox recovers missed signals. */
    readonly inboxChanged: Stream.Stream<undefined>;
  }
>() {}

/** MCP consumer leases and callback progress share the retained host inbox. */
export class McpInbox extends Context.Tag("@moltzap/McpInbox")<
  McpInbox,
  EventStore
>() {}

interface HostMessagingSettings {
  readonly registrySignerPublicKey: Ed25519PublicKey;
  readonly historyExport: HistoryExportPort;
  readonly experiments?: SendContext["experiments"];
}

interface IncomingContext {
  readonly sql: SqlClient.SqlClient;
  readonly collectives: Context.Tag.Service<typeof Collectives>;
  readonly wake: Queue.Queue<undefined>;
}

/**
 * Checks outgoing drafts and saves the incoming content the agent may read.
 *
 * ```mermaid
 * flowchart LR
 *   Agent -->|send| Firewall -->|allowed| Communication
 *   Communication -->|receive| Incoming[Incoming checks]
 *   Incoming --> Inbox --> Agent
 * ```
 */
export function hostMessagingLayer(
  settings: HostMessagingSettings,
  workers: FiberSet.FiberSet<never>,
) {
  return Layer.scopedContext(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const identity = yield* Identity;
      const communication = yield* Communication;
      const collectives = yield* Collectives;
      const { environment, changes } = yield* receiveMessages(
        settings,
        workers,
      );
      const inbox = yield* makeInboxOperations(sql, settings.historyExport);
      const history = makeHistoryOperations(identity, communication, sql);
      const service = HostMessaging.of({
        send: sendOperation({
          sql,
          identity,
          communication,
          collectives,
          firewallEnvironment: environment,
          historyExport: settings.historyExport,
          ...(settings.experiments === undefined
            ? {}
            : { experiments: settings.experiments }),
        }),
        listInbox: inbox.listInbox,
        getInboxItem: inbox.getInboxItem,
        acknowledgeDelivery: inbox.acknowledgeDelivery,
        ...history,
        inboxChanged: Stream.fromPubSub(changes),
      });
      return Context.make(HostMessaging, service).pipe(
        Context.add(McpInbox, inbox.events),
      );
    }).pipe(Effect.withSpan("hostMessagingLayer")),
  );
}

function receiveMessages(
  settings: HostMessagingSettings,
  workers: FiberSet.FiberSet<never>,
) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const collectives = yield* Collectives;
    const communication = yield* Communication;
    const changes = yield* PubSub.sliding<undefined>(1);
    yield* Effect.addFinalizer(() => PubSub.shutdown(changes));
    const wake = yield* Queue.sliding<undefined>(1);
    const environment = yield* firewallEnvironment(
      settings.registrySignerPublicKey,
    );
    const incoming: IncomingContext = { sql, collectives, wake };
    yield* supervise(
      workers,
      communication.consume((message) => receive(incoming, message)),
    );
    yield* supervise(
      workers,
      collectives.consumeResults((result) => receiveResult(incoming, result)),
    );
    const admission: AdmissionContext = {
      sql,
      scope: yield* Scope.Scope,
      firewallEnvironment: environment,
      admissionWake: wake,
      admissionFailure: yield* Deferred.make<never, EndpointStoreError>(),
      activeConversations: new Set(),
    };
    yield* supervise(
      workers,
      runAdmission(
        admission,
        PubSub.publish(changes, undefined).pipe(Effect.asVoid),
      ),
    );
    yield* Queue.offer(wake, undefined);
    return { environment, changes };
  });
}

function firewallEnvironment(registrySignerPublicKey: Ed25519PublicKey) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const current = yield* LocalIdentity;
    const firewall = yield* Firewall;
    return Ref.get(current).pipe(
      Effect.map((card): FirewallEnvironment | undefined =>
        Option.isNone(card) || firewall.configuration === undefined
          ? undefined
          : {
              sql,
              firewall,
              localAgentCard: card.value,
              registrySignerPublicKey,
            },
      ),
    );
  });
}

function receive(context: IncomingContext, message: Message) {
  return Effect.gen(function* () {
    const reference = {
      authorAgentId: message.author.agentId,
      msgId: message.msgId,
    };
    if ((yield* findIncomingMessage(reference)) !== undefined) {
      return;
    }
    const item = yield* context.collectives.receive(message);
    if (Option.isNone(item)) {
      return;
    }
    const deliveryToken = yield* mintLocalDeliveryToken;
    yield* queueIncomingItem({
      deliveryToken,
      message: reference,
      conversation: replyAddress(message),
      canonicalItem: yield* encodeRuntimeValue(item.value),
    });
    yield* Queue.offer(context.wake, undefined);
  }).pipe(Effect.provideService(SqlClient.SqlClient, context.sql));
}

function receiveResult(context: IncomingContext, result: ResultDelivery) {
  return Effect.gen(function* () {
    const { item, deliveryToken } = result;
    if (item.kind !== "collectiveResult" && item.kind !== "operationFailed") {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    const canonicalItem = yield* encodeRuntimeValue(item);
    yield* queueIncomingItem({
      deliveryToken,
      conversation: item.to,
      canonicalItem,
      canonicalSources: yield* encodeRuntimeValue(result.sources),
    });
    yield* Queue.offer(context.wake, undefined);
  }).pipe(Effect.provideService(SqlClient.SqlClient, context.sql));
}

function supervise<E, R>(
  workers: FiberSet.FiberSet<never>,
  worker: Effect.Effect<never, E, R>,
) {
  return Effect.forkScoped(Effect.interruptible(worker)).pipe(
    Effect.flatMap((fiber) => FiberSet.add(workers, fiber)),
  );
}
