/** @file An agent's firewall checks direct messages against the contracts it holds, from its own saved checks of the conversation. */

import { scopedLive as it } from "@effect/vitest";
import { type Context, Effect, FastCheck as fc } from "effect";
import { expect } from "vitest";
import type { AgentCard } from "../identity.js";
import type { Chain, FirewallEvidence } from "./chain.js";
import type { DecisionProvider } from "./check.js";
import { Message, type MsgId } from "../communication/index.js";
import { compareAscii } from "../encoding.js";
import {
  always,
  certainProvider,
  contractOf,
  form,
  noFields,
  peer,
  policy,
  valueField,
  withFirewall,
} from "./__tests__/firewall-fixtures.js";
import { type Act, formatAct, readAct } from "./contract.js";
import { Firewall } from "./service.js";

const bob = peer("bob", 2);
const carol = peer("carol", 3);

const reasonField = {
  type: "object",
  properties: { reason: { type: "string" } },
  required: ["reason"],
};

/** `open` starts an instance, `close` ends it, and `reject` answers a withheld act. */
const exchange = contractOf(
  "exchange",
  [
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
    {
      ...form,
      act: "reject",
      by: ["Sender", "Receiver"],
      fields: reasonField,
      when: ["opened"],
    },
  ],
  { onRefusal: "reject" },
);

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
        ? yield* firewall.checkOutgoing(message, evidence)
        : yield* firewall.checkIncoming(message, evidence);
    yield* decision.save;
    return decision;
  });
}

const actText = (name: string, norm: string, fields: Act["fields"] = {}) =>
  formatAct({ name, norm, fields });

it("refuses every direct text that is not an act of a held norm, and applies no contract to a group message", () =>
  withFirewall(
    {},
    (firewall, self) =>
      Effect.forEach(
        fc.sample(fc.tuple(fc.string({ minLength: 1 }), fc.boolean()), 60),
        ([text, toGroup]) =>
          Effect.gen(function* () {
            const message = yield* Message.create({
              author: self,
              recipients: toGroup ? [bob, carol] : [bob],
              content: [{ type: "text", text }],
            });
            const decision = yield* firewall.checkOutgoing(message, after([]));
            expect(decision).toMatchObject(
              toGroup
                ? { kind: "allowed" }
                : {
                    kind: "refused",
                    refusal: {
                      refusal: "rule-violation",
                      rule: "norm-structural",
                      detail: { stage: "structural" },
                    },
                  },
            );
          }),
        { concurrency: 1, discard: true },
      ),
    (agentKey) => Firewall.layer(agentKey, policy(), undefined, [exchange]),
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
              evidenceOf(unchecked),
            );
            expect(decision).toMatchObject({
              kind: "refused",
              refusal: {
                refusal: "unresolved-evidence",
                detail: { stage: "protocol" },
              },
            });
          }),
        (agentKey) => Firewall.layer(agentKey, policy(), undefined, [exchange]),
      );
    }),
  { fastCheck: { numRuns: 8 } },
);

it.prop(
  "asks the provider about an act's own contract rules even when a chain rule allows the message, and about the owner's rules only when none does",
  [
    fc.boolean(),
    fc.constantFrom("ask", "tell"),
    fc.constantFrom("incoming", "outgoing"),
  ],
  ([chainAllows, act, direction]) =>
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
      const chain: Chain = {
        rules: [
          {
            id: "allow-all",
            applies: "both",
            match: "always",
            action: "allow",
          },
        ],
        matchers: { always },
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
              ...(chainAllows ? [] : [brief.id]),
              act === "ask" ? "duo-rule-1" : "duo-rule-2",
              "duo-rule-3",
            ];
            expect([...asked].sort(compareAscii)).toEqual(
              [...expected].sort(compareAscii),
            );
            expect((yield* firewall.readPolicy()).fixedRules).toMatchObject(
              duo.textRules.map(({ id }) => ({ id })),
            );
          }),
        (agentKey) =>
          Firewall.layer(
            agentKey,
            policy([brief], [], recording),
            chainAllows ? chain : undefined,
            [duo],
          ),
      );
    }),
  { fastCheck: { numRuns: 8 } },
);

it.prop(
  "continues an instance under the contract it opened with after the agent removes or replaces that norm, and opens the next under what it then holds",
  [fc.boolean(), fc.string()],
  ([replaced, value]) =>
    Effect.gen(function* () {
      yield* withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const open = yield* direct(
              self,
              bob,
              actText("open", "exchange", { value }),
            );
            expect((yield* check(firewall, self, open, after([]))).kind).toBe(
              "allowed",
            );
            yield* firewall.updatePolicy({
              remove: [{ norm: "exchange" }],
              add: replaced ? [{ contract: replacement }] : [],
            });
            expect((yield* firewall.readPolicy()).norms).toEqual(
              replaced ? ["exchange"] : [],
            );
            const close = yield* direct(
              bob,
              self,
              actText("close", "exchange"),
              open.msgId,
            );
            expect(
              (yield* check(firewall, self, close, after([open]))).kind,
            ).toBe("allowed");
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
            expect(again.kind).toBe(replaced ? "refused" : "allowed");
            expect(begun.kind).toBe("allowed");
          }),
        (agentKey) => Firewall.layer(agentKey, policy(), undefined, [exchange]),
      );
    }),
  { fastCheck: { numRuns: 6 } },
);

it.prop(
  "proposes the contract's refusal act for a withheld act of an instance, a reply its own check allows only when the withheld act replied into that instance",
  [
    fc.oneof(
      fc.string({ minLength: 1 }),
      fc.constantFrom(
        actText("unknown", "exchange"),
        actText("close", "exchange", { extra: "unasked" }),
        actText("open", "exchange", { value: "again" }),
        actText("close", "other"),
      ),
    ),
    fc.boolean(),
  ],
  ([text, repliesIntoInstance]) =>
    Effect.gen(function* () {
      yield* withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const open = yield* direct(
              bob,
              self,
              actText("open", "exchange", { value: "first" }),
            );
            yield* check(firewall, self, open, after([]));
            const withheld = yield* direct(
              bob,
              self,
              text,
              repliesIntoInstance ? open.msgId : undefined,
            );
            const decision = yield* check(
              firewall,
              self,
              withheld,
              after([open]),
            );
            if (decision.kind !== "withheld" || decision.reply === undefined) {
              return yield* Effect.dieMessage("the act was not answered");
            }
            const reply = yield* direct(
              self,
              bob,
              decision.reply,
              withheld.msgId,
            );
            const act = yield* readAct(reply);
            expect(act).toMatchObject({ name: "reject", norm: "exchange" });
            expect(Object.keys(act.fields)).toEqual(["reason"]);
            const sent = yield* firewall.checkOutgoing(reply, {
              ...after([open]),
              references: [withheld],
            });
            expect(sent.kind).toBe(repliesIntoInstance ? "allowed" : "refused");
          }),
        (agentKey) => Firewall.layer(agentKey, policy(), undefined, [exchange]),
      );
    }),
  { fastCheck: { numRuns: 12 } },
);

/** The place in `add` of the first contract whose norm the agent keeps or the edit already added, or -1. */
function firstDuplicate(
  kept: readonly string[],
  add: ReadonlyArray<{ readonly name: string }>,
) {
  const names = add.map((contract) => contract.name);
  return names.findIndex(
    (name, index) => kept.includes(name) || names.indexOf(name) !== index,
  );
}

it.prop(
  "holds one contract for each norm name: an edit may add a norm it does not hold and remove one it does, and names the place of any other",
  [fc.subarray(["exchange", "duo"]), fc.subarray([exchange, duo, replacement])],
  ([remove, add]) =>
    Effect.gen(function* () {
      yield* withFirewall(
        {},
        (firewall) =>
          Effect.gen(function* () {
            const unknown = remove.indexOf("duo");
            const kept = remove.includes("exchange") ? [] : ["exchange"];
            const duplicate = firstDuplicate(kept, add);
            const edited = yield* Effect.either(
              firewall.updatePolicy({
                remove: remove.map((norm) => ({ norm })),
                add: add.map((contract) => ({ contract })),
              }),
            );
            const held = (yield* firewall.readPolicy()).norms;
            if (unknown >= 0) {
              expect(edited).toMatchObject({
                left: { problem: "unknown-id", path: ["remove", unknown] },
              });
              expect(held).toEqual(["exchange"]);
            } else if (duplicate >= 0) {
              expect(edited).toMatchObject({
                left: { problem: "duplicate-id", path: ["add", duplicate] },
              });
              expect(held).toEqual(["exchange"]);
            } else {
              expect(held).toEqual([
                ...kept,
                ...add.map((contract) => contract.name),
              ]);
            }
          }),
        (agentKey) => Firewall.layer(agentKey, policy(), undefined, [exchange]),
      );
    }),
  { fastCheck: { numRuns: 12 } },
);
