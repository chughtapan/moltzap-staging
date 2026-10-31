/** @file Request acknowledgment preserves answer authority while a pending assessment grants none. */

import { scopedLive as it } from "@effect/vitest";
import { Deferred, Effect, Exit, Schedule, Schema, Scope } from "effect";
import { expect } from "vitest";
import { appendCertifiedMessage } from "../../__tests__/certified-history-fixtures.js";
import { collectiveOperationId } from "../../__tests__/collective-id-fixtures.js";
import {
  conversationIdentityFixture,
  seedIncoming,
} from "../../__tests__/conversation-fixtures.js";
import {
  block,
  firewallOptions,
  pass,
  policy,
} from "../../__tests__/daemon-delivery-fixtures.js";
import { makeHostMessagingFixture } from "../../__tests__/host-messaging-fixtures.js";
import {
  encodeCollectiveContent,
  FormModeSchema,
} from "../../communication/collectives/wire.js";
import { AgentAddress } from "../../communication/records/values.js";

const bob = Schema.decodeUnknownSync(AgentAddress)("agent:bob");
const requestFixture = Effect.gen(function* () {
  const subject = yield* conversationIdentityFixture;
  const first = yield* seedIncoming(subject);
  const operationId = collectiveOperationId(subject.remote.card.agentId);
  const requestedSchema = yield* Schema.decodeUnknown(FormModeSchema)({
    type: "object",
    properties: { slot: { type: "string", enum: ["mon", "tue"] } },
    required: ["slot"],
  });
  const content = yield* encodeCollectiveContent(
    {
      kind: "operation",
      op: "gather",
      operationId,
      deadlineAt: Date.now() + 60_000,
      requestedSchema,
    },
    "Which day?",
  );
  yield* subject.run(appendCertifiedMessage(subject, first, content));
  return {
    subject,
    operationId,
    response: {
      input: {
        to: bob,
        collectiveResponse: {
          operationId,
          action: "accept" as const,
          content: { slot: "mon" },
        },
      },
    },
  };
});

function waitForRequest(
  runtime: Effect.Effect.Success<ReturnType<typeof makeHostMessagingFixture>>,
) {
  return runtime.messaging.listInbox({}).pipe(
    Effect.repeat({
      until: (page) =>
        page.items.some((entry) => entry.item.kind === "collectiveRequest"),
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.zipRight(runtime.messaging.listInbox({})),
    Effect.timeout("10 seconds"),
  );
}

it("answers an acknowledged request after restarting its host services", () =>
  Effect.gen(function* () {
    const fixture = yield* requestFixture;
    const scope = yield* Scope.make();
    const original = yield* makeHostMessagingFixture(fixture.subject).pipe(
      Scope.extend(scope),
    );
    const page = yield* waitForRequest(original);
    const request = page.items.find(
      (entry) => entry.item.kind === "collectiveRequest",
    );
    if (request === undefined) {
      return yield* Effect.dieMessage("Expected a delivered request");
    }
    yield* original.messaging.acknowledgeDelivery(request.deliveryToken);
    yield* Scope.close(scope, Exit.void);
    const resumed = yield* makeHostMessagingFixture(fixture.subject);
    expect(
      yield* resumed.messaging.getInboxItem(request.deliveryToken),
    ).toMatchObject({
      item: {
        kind: "collectiveRequest",
        message: { content: { operationId: fixture.operationId } },
      },
    });
    expect(yield* resumed.messaging.send(fixture.response)).toMatchObject({
      kind: "response",
      operationId: fixture.operationId,
    });
  }));

it("keeps an unpresented request unanswerable after restarting its host services", () =>
  Effect.gen(function* () {
    const fixture = yield* requestFixture;
    const entered = yield* Deferred.make<undefined>();
    const scope = yield* Scope.make();
    const held = policy((input) =>
      input.event.message.operation?.kind === "request"
        ? Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Effect.never),
          )
        : Effect.succeed(pass),
    );
    yield* makeHostMessagingFixture(fixture.subject, {
      firewall: firewallOptions(held),
    }).pipe(Scope.extend(scope));
    yield* Deferred.await(entered);
    yield* Scope.close(scope, Exit.void);
    const resumed = yield* makeHostMessagingFixture(fixture.subject, {
      firewall: firewallOptions(policy(() => Effect.succeed(block))),
    });
    expect(
      yield* resumed.messaging.send(fixture.response).pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-none" } });
  }));
