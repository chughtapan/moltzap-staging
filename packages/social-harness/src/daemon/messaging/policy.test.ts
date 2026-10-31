/** @file Host policy governs delivered text and captured outgoing commands through the service graph. */

import { scopedLive as it } from "@effect/vitest";
import {
  type Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Schedule,
  Schema,
  Scope,
} from "effect";
import { expect } from "vitest";
import type { Message } from "../../communication/message.js";
import type { DecisionRequest } from "../../firewall/check.js";
import type { HostMessaging } from "./index.js";
import { appendCertifiedMessage } from "../../__tests__/certified-history-fixtures.js";
import {
  conversationIdentityFixture,
  seedIncoming,
} from "../../__tests__/conversation-fixtures.js";
import {
  decisionResponse,
  decisionState,
  firewallOptions,
  rule,
  waitForChecks,
} from "../../__tests__/daemon-delivery-fixtures.js";
import { makeHostMessagingFixture } from "../../__tests__/host-messaging-fixtures.js";
import { Communication } from "../../communication/index.js";
import { AgentAddress } from "../../communication/records/values.js";

const bob = Schema.decodeUnknownSync(AgentAddress)("agent:bob");
const unavailable = () =>
  Effect.dieMessage("The test does not use this messaging operation");
function recordedCommunication(messages: Message[]) {
  return Communication.of({
    send: (message) =>
      Effect.sync(() => {
        messages.push(message);
      }),
    waitForMessage: unavailable,
    consume: () => Effect.never,
    listConversations: unavailable,
    readHistory: unavailable,
  });
}

// @agent-code-guard/regression-only: Composed-service cases exercise finite policy, cancellation, captured-input, identity, and restart transitions.
it("withholds a peer message from both inbox and conversation history", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    yield* seedIncoming(subject);
    const runtime = yield* makeHostMessagingFixture(subject, {
      firewall: firewallOptions({
        decide: (request) =>
          Effect.succeed(decisionResponse(request, "violation")),
      }),
    });
    yield* waitForChecks(subject);
    expect(yield* runtime.messaging.listInbox({})).toEqual({ items: [] });
    expect(
      (yield* runtime.messaging.readHistory({ address: bob })).records,
    ).toEqual([]);
  }));

it("delivers original peer text with guidance attributed to its author and recipients", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    yield* seedIncoming(subject);
    const runtime = yield* makeHostMessagingFixture(subject, {
      firewall: firewallOptions({
        decide: (request) =>
          Effect.succeed(decisionResponse(request, "compliant", "required")),
      }),
    });
    yield* waitForChecks(subject);
    const [received] = (yield* runtime.messaging.listInbox({})).items;
    expect(received?.item).toMatchObject({
      kind: "multicast",
      message: {
        author: { agentId: subject.remote.card.agentId },
        content: [{ type: "text", text: "certified before restart" }],
      },
    });
    expect(received?.guidance).toMatchObject({
      subjects: [
        {
          sender: { agentId: subject.remote.card.agentId },
          recipients: [{ agentId: subject.local.card.agentId }],
          text: "certified before restart",
        },
      ],
      instructions: [{ instruction: rule.guidance }],
    });
  }));

it("uses the latest bounded history and excludes the candidate being assessed", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const first = yield* seedIncoming(subject);
    const second = yield* subject.run(
      appendCertifiedMessage(subject, first, "An earlier local message", true),
    );
    const third = yield* subject.run(
      appendCertifiedMessage(subject, second, "Recent local message", true),
    );
    yield* subject.run(
      appendCertifiedMessage(subject, third, "Newest local message", true),
    );
    const seen: DecisionRequest[] = [];
    yield* makeHostMessagingFixture(subject, {
      firewall: firewallOptions(
        {
          decide: (request) =>
            Effect.sync(() => {
              if (request.stage === "compliance") {
                seen.push(request);
              }
              return decisionResponse(request);
            }),
        },
        {
          maximumHistoryRecords: 2,
        },
      ),
    });
    yield* waitForChecks(subject);
    const state = decisionState(seen[0]?.state);
    expect(state.history.map((entry) => entry.text)).toEqual([
      "Recent local message",
      "Newest local message",
    ]);
    expect(state.earlierHistoryOmitted).toBe(true);
  }));

it("cancels an unaccepted draft and releases the check permit for another send", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const entered = yield* Deferred.make<undefined>();
    const messages: Message[] = [];
    const provider = pauseFirstCheck(entered);
    const runtime = yield* makeHostMessagingFixture(subject, {
      communication: recordedCommunication(messages),
      firewall: firewallOptions(provider, { maximumConcurrentChecks: 1 }),
    });
    const sending = yield* runtime.messaging
      .send({ input: { to: bob, text: "Cancelled draft" } })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    yield* Fiber.interrupt(sending);
    expect(messages).toEqual([]);
    expect(
      yield* runtime.messaging
        .send({ input: { to: bob, text: "Another draft" } })
        .pipe(Effect.flip),
    ).toMatchObject({ refusal: "rule-violation" });
  }));

it("keeps a captured draft when its caller changes the original input during assessment", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const entered = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const messages: Message[] = [];
    const runtime = yield* makeHostMessagingFixture(subject, {
      communication: recordedCommunication(messages),
      firewall: firewallOptions({
        decide: (request) =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Deferred.await(release)),
            Effect.as(decisionResponse(request)),
          ),
      }),
    });
    const input = { to: bob, text: "The captured draft" };
    const sending = yield* runtime.messaging
      .send({ input })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(entered);
    input.text = "A later caller mutation";
    yield* Deferred.succeed(release, undefined);
    expect(yield* Fiber.join(sending)).toMatchObject({
      kind: "multicast",
      msgId: messages[0]?.msgId,
    });
    expect(messages.map((message) => message.content)).toEqual([
      [{ type: "text", text: "The captured draft" }],
    ]);
  }));

it("assigns a distinct message identity to each repeated host invocation", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const messages: Message[] = [];
    const runtime = yield* makeHostMessagingFixture(subject, {
      communication: recordedCommunication(messages),
    });
    const input = { to: bob, text: "Repeat this intentionally" };
    const first = yield* runtime.messaging.send({ input });
    const second = yield* runtime.messaging.send({ input });
    expect(first).toMatchObject({
      kind: "multicast",
      msgId: messages[0]?.msgId,
    });
    expect(second).toMatchObject({
      kind: "multicast",
      msgId: messages[1]?.msgId,
    });
    expect(messages.map((message) => message.msgId)).toHaveLength(2);
    expect(messages[0]?.msgId).not.toBe(messages[1]?.msgId);
  }));

it("drains saved incoming work after restarting without configured assessment", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    yield* seedIncoming(subject);
    const entered = yield* Deferred.make<undefined>();
    const scope = yield* Scope.make();
    yield* makeHostMessagingFixture(subject, {
      firewall: firewallOptions({
        decide: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.zipRight(Effect.never),
          ),
      }),
    }).pipe(Scope.extend(scope));
    yield* Deferred.await(entered);
    yield* Scope.close(scope, Exit.void);
    const resumed = yield* makeHostMessagingFixture(subject);
    const page = yield* waitForInbox(resumed.messaging);
    expect(page.items).toHaveLength(1);
    expect(page.items[0]?.item).toMatchObject({
      kind: "multicast",
      message: {
        author: { agentId: subject.remote.card.agentId },
        content: [{ type: "text", text: "certified before restart" }],
      },
    });
  }));

function waitForInbox(messaging: Context.Tag.Service<typeof HostMessaging>) {
  return messaging.listInbox({}).pipe(
    Effect.repeat({
      until: (page) => page.items.length > 0,
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.zipRight(messaging.listInbox({})),
    Effect.timeout("10 seconds"),
  );
}

function pauseFirstCheck(entered: Deferred.Deferred<undefined>) {
  let checks = 0;
  return {
    decide: (request: DecisionRequest) =>
      Effect.suspend(() => {
        checks += 1;
        return checks === 1
          ? Deferred.succeed(entered, undefined).pipe(
              Effect.zipRight(Effect.never),
            )
          : Effect.succeed(decisionResponse(request, "violation"));
      }),
  };
}
