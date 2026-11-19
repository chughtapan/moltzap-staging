/** @file An agent's firewall checks direct messages against the contracts it holds, from its own saved checks of the conversation. */

import { scopedLive as it } from "@effect/vitest";
import { type Context, Effect, FastCheck as fc } from "effect";
import { expect } from "vitest";
import type { AgentCard } from "../identity.js";
import type { DecisionProvider, FirewallEvidence } from "./check.js";
import type { Act } from "./contract.js";
import type { Firewall } from "./service.js";
import { Message, type MsgId } from "../communication/index.js";
import { compareAscii } from "../encoding.js";
import { openTestDatabase } from "../test.js";
import {
  certainProvider,
  contractOf,
  firewallLayer,
  form,
  formatAct,
  noFields,
  peer,
  policy,
  valueField,
  withFirewall,
} from "./__tests__/firewall-fixtures.js";

const bob = peer("bob", 2);

/** `open` starts an instance and `close` ends it. */
const exchange = contractOf("exchange", [
  {
    ...form,
    act: "open",
    by: ["Sender"],
    fields: valueField,
    starts: true,
    initiates: ["opened"],
  },
  {
    ...form,
    act: "close",
    by: ["Receiver"],
    fields: noFields,
    when: ["opened"],
    ends: true,
  },
]);

/** The norm of `exchange` under another document: `begin` starts an instance in place of `open`. */
const replacement = contractOf(
  "exchange",
  [
    {
      ...form,
      act: "begin",
      by: ["Sender"],
      fields: noFields,
      starts: true,
      ends: true,
    },
  ],
  { digest: "1".repeat(64) },
);

/** Two one-act instances, with a rule for each act and one for both. */
const duo = contractOf(
  "duo",
  ["ask", "tell"].map((act) => ({
    ...form,
    act,
    by: ["Sender"],
    fields: noFields,
    starts: true,
    ends: true,
  })),
  {
    textRules: [
      { id: "duo-rule-1", act: "ask", applies: "both", text: "Ask politely." },
      { id: "duo-rule-2", act: "tell", applies: "both", text: "Tell briefly." },
      { id: "duo-rule-3", applies: "both", text: "Stay on the subject." },
    ],
  },
);

const brief = { id: "brief", applies: "both", text: "Be brief." } as const;

type FirewallService = Context.Tag.Service<typeof Firewall>;

/** A direct message of `text` from `author` to `recipient`. */
function direct(
  author: AgentCard,
  recipient: AgentCard,
  text: string,
  replyToMsgId?: MsgId,
) {
  return Message.create({
    author,
    recipients: [recipient],
    content: [{ type: "text", text }],
    replyToMsgId,
  });
}

/** Evidence that the conversation so far is exactly `history`. */
function after(history: readonly Message[]): FirewallEvidence {
  return {
    history: history.map((message) => ({ message, visibleToAgent: true })),
    earlierHistoryOmitted: false,
    gaps: [],
    protocolHistory: { messages: history, complete: true },
  };
}

/** Checks a message this agent sends or receives, and saves the check. */
function check(
  firewall: FirewallService,
  self: AgentCard,
  message: Message,
  evidence: FirewallEvidence,
) {
  return Effect.gen(function* () {
    const decision =
      message.author.agentId === self.agentId
        ? yield* firewall.checkOutgoing(message, () => Effect.succeed(evidence))
        : yield* firewall.checkIncoming(message, () =>
            Effect.succeed(evidence),
          );
    yield* decision.save;
    return decision;
  });
}

const actText = (name: string, norm: string, fields: Act["fields"] = {}) =>
  formatAct({ name, norm, fields });

it("refuses every text that is not an act of a held norm", () =>
  withFirewall(
    {},
    (firewall, self) =>
      Effect.forEach(
        fc.sample(fc.string({ minLength: 1 }), 60),
        (text) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* direct(self, bob, text),
              () => Effect.succeed(after([])),
            );
            expect(decision).toMatchObject({
              kind: "refused",
              refusal: {
                refusal: "rule-violation",
                rule: "norm-structural",
                detail: { stage: "structural" },
              },
            });
          }),
        { concurrency: 1, discard: true },
      ),
    (agentKey) => firewallLayer(agentKey, policy(), [exchange]),
  ));

it.prop(
  "refuses an act for unresolved evidence when its conversation's history is incomplete or holds a message the firewall saved no check of",
  [
    fc.constantFrom<(unchecked: Message) => FirewallEvidence>(
      (unchecked) => after([unchecked]),
      () => ({
        ...after([]),
        protocolHistory: { messages: [], complete: false },
      }),
      () => ({ history: [], earlierHistoryOmitted: true, gaps: [] }),
      () => ({ history: [], earlierHistoryOmitted: false, gaps: ["a gap"] }),
    ),
    fc.string(),
  ],
  ([evidenceOf, value]) =>
    Effect.gen(function* () {
      yield* withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const unchecked = yield* direct(bob, self, "Hello");
            const decision = yield* firewall.checkOutgoing(
              yield* direct(self, bob, actText("open", "exchange", { value })),
              () => Effect.succeed(evidenceOf(unchecked)),
            );
            expect(decision).toMatchObject({
              kind: "refused",
              refusal: {
                refusal: "unresolved-evidence",
                detail: { stage: "protocol" },
              },
            });
          }),
        (agentKey) => firewallLayer(agentKey, policy(), [exchange]),
      );
    }),
  { fastCheck: { numRuns: 8 } },
);

it.prop(
  "asks the provider about the owner's rules and an act's own contract rules",
  [fc.constantFrom("ask", "tell"), fc.constantFrom("incoming", "outgoing")],
  ([act, direction]) =>
    Effect.gen(function* () {
      const asked: string[] = [];
      const recording: DecisionProvider = {
        decide: (request) => {
          asked.push(
            ...Object.values(request.questions).map(
              (question) => question.ruleId,
            ),
          );
          return certainProvider.decide(request);
        },
      };
      yield* withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const [author, recipient] =
              direction === "outgoing" ? [self, bob] : [bob, self];
            const decision = yield* check(
              firewall,
              self,
              yield* direct(author, recipient, actText(act, "duo")),
              after([]),
            );
            expect(decision.kind).toBe("allowed");
            const expected = [
              brief.id,
              act === "ask" ? "duo-rule-1" : "duo-rule-2",
              "duo-rule-3",
            ];
            expect([...asked].sort(compareAscii)).toEqual(
              [...expected].sort(compareAscii),
            );
          }),
        (agentKey) =>
          firewallLayer(agentKey, policy([brief], recording), [duo]),
      );
    }),
  { fastCheck: { numRuns: 8 } },
);

it.prop(
  "continues an instance under the contract it opened with after a restart that holds another document of that norm, and opens the next under the new one",
  [fc.string()],
  ([value]) =>
    Effect.gen(function* () {
      const database = yield* openTestDatabase();
      const open = yield* withFirewall(
        { database },
        (firewall, self) =>
          Effect.gen(function* () {
            const opening = yield* direct(
              self,
              bob,
              actText("open", "exchange", { value }),
            );
            const decision = yield* check(firewall, self, opening, after([]));
            expect(decision.kind).toBe("allowed");
            return opening;
          }),
        (agentKey) => firewallLayer(agentKey, policy(), [exchange]),
      );
      yield* withFirewall(
        { database },
        (firewall, self) =>
          Effect.gen(function* () {
            const close = yield* direct(
              bob,
              self,
              actText("close", "exchange"),
              open.msgId,
            );
            const closed = yield* check(firewall, self, close, after([open]));
            expect(closed.kind).toBe("allowed");
            const history = [open, close];
            const again = yield* check(
              firewall,
              self,
              yield* direct(self, bob, actText("open", "exchange", { value })),
              after(history),
            );
            const begun = yield* check(
              firewall,
              self,
              yield* direct(self, bob, actText("begin", "exchange")),
              after(history),
            );
            expect(again.kind).toBe("refused");
            expect(begun.kind).toBe("allowed");
          }),
        (agentKey) => firewallLayer(agentKey, policy(), [replacement]),
      );
    }),
  { fastCheck: { numRuns: 6 } },
);
