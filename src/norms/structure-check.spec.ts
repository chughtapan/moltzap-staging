/** @file The structure check passes exactly the acts a norm's turn rules allow after a conversation's earlier messages and the firewall's checks of them, and answers a refusal with the norm's refusal act. */

import { scopedLive } from "@effect/vitest";
import { Effect, FastCheck as fc, Option, Schema } from "effect";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import type { EarlierMessage } from "../firewall/index.js";
import { peer } from "../firewall/__tests__/firewall-fixtures.js";
import { type AgentCard, Message, MsgId } from "../messages.js";
import {
  form,
  noFields,
  normOf,
  valueField,
} from "./__tests__/norms-fixtures.js";
import { type Act, formatAct, type Norm } from "./norm.js";
import { structureCheck } from "./structure-check.js";

beforeAll(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
});
afterAll(() => {
  vi.useRealTimers();
});

const alice = peer("alice", 1);
const bob = peer("bob", 2);
const agents = [alice, bob] as const;

/**
 * Offers alternate. `offer` opens an instance. Only the recipient of the
 * current offer may `counter` it or `choose` its value, replying in its
 * chain, and choosing settles it. Either agent may `finish`.
 */
const turns = normOf("turns", [
  {
    ...form,
    act: "offer",
    by: ["Sender"],
    fields: valueField,
    starts: true,
    initiates: ["offered"],
  },
  {
    ...form,
    act: "counter",
    by: { recipient: { fact: "offered" } },
    fields: valueField,
    when: ["offered"],
    initiates: ["offered"],
  },
  {
    ...form,
    act: "choose",
    by: { recipient: { fact: "offered" } },
    fields: valueField,
    when: ["offered"],
    terminates: ["offered"],
    initiates: ["selected"],
    relations: [
      {
        operator: "equals",
        field: "value",
        source: { kind: "field", act: { fact: "offered" }, field: "value" },
      },
    ],
  },
  {
    ...form,
    act: "finish",
    by: ["Sender", "Receiver"],
    fields: noFields,
    ends: true,
  },
]);

/** A message id no generated history holds. */
const unknownId = Schema.decodeUnknownSync(MsgId)(`msg_${"A".repeat(43)}`);

/**
 * Checks `act` from `author` against `history` at `now`, and adds its message
 * to the history as one the firewall allowed when the check passes it. A
 * history that holds every message twice gives the same answer.
 */
function attempt(
  norm: Norm,
  history: EarlierMessage[],
  taken: {
    readonly author: AgentCard;
    readonly act: Act;
    readonly now: number;
    readonly reply: Option.Option<MsgId>;
  },
) {
  const message = actMessage(taken.author, stepId(history.length), taken.act, {
    replyToMsgId: Option.getOrUndefined(taken.reply),
  });
  const { kind } = check(norm, history, message, taken.now);
  expect(check(norm, [...history, ...history], message, taken.now).kind).toBe(
    kind,
  );
  history.push({ message, checkedAt: taken.now, allowed: kind === "passed" });
  return kind === "passed";
}

/** What the structure check of an agent that holds `norm` says of `message` at `now`, after `history`. */
function check(
  norm: Norm,
  history: readonly EarlierMessage[],
  message: Message,
  now: number,
) {
  vi.setSystemTime(now);
  return Effect.runSync(structureCheck([norm])(message, history));
}

/** A direct message from `author` to the other agent that states `act`. */
function actMessage(
  author: AgentCard,
  msgId: MsgId,
  act: Act,
  reference: Pick<Message, "replyToMsgId">,
): Message {
  return Schema.decodeSync(Message)({
    author,
    recipient: author.agentId === alice.agentId ? bob : alice,
    msgId,
    ...reference,
    text: formatAct(act),
  });
}

/** The message id of step `index` of a walk. */
function stepId(index: number): MsgId {
  return Schema.decodeUnknownSync(MsgId)(
    `msg_${Buffer.alloc(32, index + 1).toString("base64url")}`,
  );
}

/**
 * One step of a walk: who sends which act with which value, and the message
 * it replies to. Counters and replies to earlier steps are the likeliest, so
 * walks reach offers several messages deep.
 */
const step = fc.record({
  author: fc.constantFrom(0, 1),
  act: fc.oneof(
    { weight: 2, arbitrary: fc.constant("offer") },
    { weight: 5, arbitrary: fc.constant("counter") },
    { weight: 2, arbitrary: fc.constant("choose") },
    { weight: 1, arbitrary: fc.constant("finish") },
  ),
  value: fc.constantFrom("a", "b"),
  reply: fc.oneof(
    { weight: 2, arbitrary: fc.constant<"none" | "unknown">("none") },
    { weight: 1, arbitrary: fc.constant<"none" | "unknown">("unknown") },
    { weight: 7, arbitrary: fc.nat({ max: 11 }) },
  ),
});

type Step =
  typeof step extends fc.Arbitrary<infer Generated> ? Generated : never;

/**
 * What the `turns` norm's turn rules say of a walk, kept apart from the
 * code under test: whether an instance is open, the current offer, and each
 * message of the instance with the message it replied to.
 */
interface Model {
  open: boolean;
  offer?: {
    readonly id: MsgId;
    readonly author: number;
    readonly value: string;
  };
  readonly members: Map<MsgId, Option.Option<MsgId>>;
  readonly known: Set<MsgId>;
}

/** The message a step replies to: none, one no history holds, or an earlier step's. */
function replyOf(
  history: readonly EarlierMessage[],
  taken: Step,
): Option.Option<MsgId> {
  if (taken.reply === "none") {
    return Option.none();
  }
  if (taken.reply === "unknown") {
    return Option.some(unknownId);
  }
  return Option.fromNullable(
    history[taken.reply % Math.max(history.length, 1)]?.message.msgId,
  );
}

/** Whether the model's rules permit `taken` as a reply to `reply`. */
function modelPermits(
  model: Model,
  taken: Step,
  reply: Option.Option<MsgId>,
): boolean {
  if (!model.open) {
    return (
      taken.act === "offer" &&
      Option.match(reply, {
        onNone: () => true,
        onSome: (id) => model.known.has(id),
      })
    );
  }
  const inInstance = Option.filter(reply, (id) => model.members.has(id));
  if (taken.act === "offer" || Option.isNone(inInstance)) {
    return false;
  }
  return taken.act === "finish" || answersOffer(model, taken, inInstance.value);
}

/** Whether `taken` counters or chooses the current offer: from its recipient, in a reply chain that holds it. */
function answersOffer(model: Model, taken: Step, reply: MsgId): boolean {
  const chain = new Set<MsgId>();
  let current = Option.some(reply);
  while (
    Option.isSome(current) &&
    model.members.has(current.value) &&
    !chain.has(current.value)
  ) {
    chain.add(current.value);
    current = model.members.get(current.value) ?? Option.none();
  }
  const offer = model.offer;
  if (
    offer === undefined ||
    !chain.has(offer.id) ||
    offer.author === taken.author
  ) {
    return false;
  }
  return taken.act === "counter" || taken.value === offer.value;
}

/** Applies a step to the model: its message joins the instance it replies into, and a permitted act takes effect. */
function advanceModel(
  model: Model,
  taken: Step,
  message: { readonly id: MsgId; readonly reply: Option.Option<MsgId> },
  permitted: boolean,
) {
  const { id, reply } = message;
  if (
    model.open &&
    Option.exists(reply, (replied) => model.members.has(replied))
  ) {
    model.members.set(id, reply);
  }
  model.known.add(id);
  if (!permitted) {
    return;
  }
  switch (taken.act) {
    case "finish":
      model.open = false;
      delete model.offer;
      model.members.clear();
      break;
    case "choose":
      delete model.offer;
      break;
    case "offer":
      model.open = true;
      model.members.set(id, reply);
      model.offer = { id, author: taken.author, value: taken.value };
      break;
    default:
      model.offer = { id, author: taken.author, value: taken.value };
  }
}

it("permits exactly the acts the turn rules allow, whoever sends them and whichever message they reply to", () => {
  fc.assert(
    fc.property(fc.array(step, { maxLength: 12 }), (steps) => {
      const history: EarlierMessage[] = [];
      const model: Model = {
        open: false,
        members: new Map(),
        known: new Set(),
      };
      for (const taken of steps) {
        const id = stepId(history.length);
        const reply = replyOf(history, taken);
        const expected = modelPermits(model, taken, reply);
        const permitted = attempt(turns, history, {
          author: agents[taken.author] ?? alice,
          act: {
            name: taken.act,
            norm: "turns",
            fields: taken.act === "finish" ? {} : { value: taken.value },
          },
          now: 0,
          reply,
        });
        expect(permitted).toBe(expected);
        advanceModel(model, taken, { id, reply }, permitted);
      }
    }),
    { numRuns: 500 },
  );
});

it("permits an act with a delay and a limit only once its fact is that old and while it was sent fewer times than the limit", () => {
  fc.assert(
    fc.property(
      fc.record({
        wait: fc.nat({ max: 50 }),
        elapsed: fc.nat({ max: 60_000 }),
        maximum: fc.integer({ min: 1, max: 3 }),
        sent: fc.nat({ max: 4 }),
      }),
      ({ wait, elapsed, maximum, sent }) => {
        const timed = normOf("timed", [
          {
            ...form,
            act: "hold",
            by: ["Sender"],
            fields: [
              { name: "wait", type: "integer", optional: false, minimum: 0 },
            ],
            starts: true,
            initiates: ["held"],
          },
          {
            ...form,
            act: "poll",
            by: ["Sender", "Receiver"],
            fields: noFields,
            when: ["held"],
            maximum,
            delay: { fact: "held", field: "wait" },
          },
        ]);
        const history: EarlierMessage[] = [];
        const start = 1_000;
        const due = start + wait * 1000;
        attempt(timed, history, {
          author: alice,
          act: { name: "hold", norm: "timed", fields: { wait } },
          now: start,
          reply: Option.none(),
        });
        const poll = (now: number) =>
          attempt(timed, history, {
            author: bob,
            act: { name: "poll", norm: "timed", fields: {} },
            now,
            reply: Option.some(stepId(0)),
          });
        for (let count = 0; count < sent; count += 1) {
          expect(poll(due)).toBe(count < maximum);
        }
        expect(poll(start + elapsed)).toBe(
          start + elapsed >= due && sent < maximum,
        );
      },
    ),
  );
});

/** `open` starts an instance, `close` ends it, and `reject` answers a withheld act. */
const exchange = normOf(
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
      fields: [{ name: "reason", type: "string", optional: false }],
      when: ["opened"],
    },
  ],
  { onRefusal: "reject" },
);

/** Two one-act instances, with a rule for each act and one for both. */
const duo = normOf(
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
      { id: "duo-rule-1", act: "ask", text: "Ask politely." },
      { id: "duo-rule-2", act: "tell", text: "Tell briefly." },
      { id: "duo-rule-3", text: "Stay on the subject." },
    ],
  },
);

/** A direct message of `text` from `author` to `recipient`. */
function direct(
  author: AgentCard,
  recipient: AgentCard,
  text: string,
  replyToMsgId?: MsgId,
) {
  return Message.create({
    author,
    recipient: recipient,
    text,
    replyToMsgId,
  });
}

const actText = (name: string, norm: string, fields: Act["fields"] = {}) =>
  formatAct({ name, norm, fields });

scopedLive.prop(
  "refuses every text that is not an act of a held norm",
  [fc.string({ minLength: 1 })],
  ([text]) =>
    Effect.gen(function* () {
      const result = yield* structureCheck([exchange])(
        yield* direct(alice, bob, text),
        [],
      );

      expect(result).toMatchObject({
        kind: "refused",
        reason: "rule-violation",
        ruleId: "norm-structural",
        detail: { stage: "structural" },
      });
    }),
  { fastCheck: { numRuns: 60 } },
);

scopedLive.each(["ask", "tell"])(
  "passes the act %s with the rules its norm gives that act and every act",
  (act) =>
    Effect.gen(function* () {
      const result = yield* structureCheck([duo])(
        yield* direct(alice, bob, actText(act, "duo")),
        [],
      );

      expect(result).toMatchObject({
        kind: "passed",
        textRules: [
          { id: act === "ask" ? "duo-rule-1" : "duo-rule-2", applies: "both" },
          { id: "duo-rule-3", applies: "both" },
        ],
        protocol: { norm: "duo", act },
      });
    }),
);

scopedLive.prop(
  "continues an instance only from an opening act the firewall allowed",
  [
    fc.constantFrom(
      { checkedAt: 1, allowed: true },
      { checkedAt: 1, allowed: false },
      { allowed: false },
    ),
    fc.string(),
  ],
  ([checked, value]) =>
    Effect.gen(function* () {
      const open = yield* direct(
        alice,
        bob,
        actText("open", "exchange", { value }),
      );

      const result = yield* structureCheck([exchange])(
        yield* direct(bob, alice, actText("close", "exchange"), open.msgId),
        [{ message: open, ...checked }],
      );

      expect(result.kind).toBe(checked.allowed ? "passed" : "refused");
    }),
  { fastCheck: { numRuns: 12 } },
);

scopedLive.prop(
  "answers a message withheld in an instance, by its own refusal or a later one, with the norm's refusal act",
  [
    fc.oneof(
      fc.string({ minLength: 1 }),
      fc.constantFrom(
        actText("unknown", "exchange"),
        actText("close", "exchange", { extra: "unasked" }),
        actText("open", "exchange", { value: "again" }),
        actText("close", "other"),
        actText("close", "exchange"),
      ),
    ),
    fc.string({ minLength: 1 }),
  ],
  ([text, feedback]) =>
    Effect.gen(function* () {
      const open = yield* direct(
        alice,
        bob,
        actText("open", "exchange", { value: "first" }),
      );

      const result = yield* structureCheck([exchange])(
        yield* direct(bob, alice, text, open.msgId),
        [{ message: open, checkedAt: 1, allowed: true }],
      );

      expect(
        result.kind === "refused" ? result.reply : result.replyTo?.(feedback),
      ).toBe(
        actText("reject", "exchange", {
          reason: result.kind === "refused" ? result.feedback : feedback,
        }),
      );
    }),
  { fastCheck: { numRuns: 20 } },
);
