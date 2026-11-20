/** @file The structure check passes exactly the acts a norm's turn rules and forms allow after a conversation's earlier messages and the firewall's checks of them, names what is wrong with any other message, and answers a refusal with the norm's refusal act. */

import { Effect, Either, FastCheck as fc, Option, Schema } from "effect";
import { randomBytes } from "node:crypto";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import type { EarlierMessage } from "../firewall.js";
import { AgentName, Message, MsgId } from "../messages.js";
import { compileNorm } from "./compiler.js";
import { structureCheck } from "./structure-check.js";

beforeAll(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
});
afterAll(() => {
  vi.useRealTimers();
});

const alice = AgentName.make("alice");
const bob = AgentName.make("bob");
const agents = [alice, bob] as const;

/** The norm a document declares. */
const normFrom = (document: string) => Either.getOrThrow(compileNorm(document));
type Norm = ReturnType<typeof normFrom>;

/** The text of an act: its performative, its norm and each field, with strings quoted and a selection as a list. */
function actText(
  name: string,
  norm: string,
  fields: Readonly<Record<string, string | number | readonly string[]>> = {},
) {
  const stated = Object.entries(fields).map(
    ([field, value]) =>
      ` :${field} ${
        typeof value === "object"
          ? `(${value.map((choice) => JSON.stringify(choice)).join(" ")})`
          : JSON.stringify(value)
      }`,
  );
  return `(${name} :ontology ${norm}${stated.join("")})`;
}

/**
 * Offers alternate. `offer` opens an instance. Only the recipient of the
 * current offer may `counter` it or `choose` its value, replying in its
 * chain, and choosing settles it. Either agent may `finish`.
 */
const turns = normFrom(`
(norm turns :initiator Sender :participant Receiver)
(content offer :by Sender :starts true :initiates offered
  (field value :type string))
(content counter :by (recipient (fact offered)) :when offered :initiates offered
  (field value :type string))
(content choose :by (recipient (fact offered)) :when offered
  :terminates offered :initiates selected
  (field value :type string)
  (relation equals value (previous (fact offered) value)))
(content finish :by (Sender Receiver) :ends true)
`);

/** A message id no generated history holds. */
const unknownId = Schema.decodeUnknownSync(MsgId)(`msg_${"A".repeat(43)}`);

/**
 * Checks the act `text` from `author` against `history` at `now`, and adds
 * its message to the history as one the firewall allowed when the check
 * passes it. A history that holds every message twice gives the same answer.
 */
function attempt(
  norm: Norm,
  history: EarlierMessage[],
  taken: {
    readonly author: AgentName;
    readonly text: string;
    readonly now: number;
    readonly reply: Option.Option<MsgId>;
  },
) {
  const candidate = message(
    taken.author,
    taken.text,
    { replyToMsgId: Option.getOrUndefined(taken.reply) },
    stepId(history.length),
  );
  const { kind } = check(norm, candidate, history, taken.now);
  expect(check(norm, candidate, [...history, ...history], taken.now).kind).toBe(
    kind,
  );
  history.push({
    message: candidate,
    checkedAt: taken.now,
    allowed: kind === "passed",
  });
  return kind === "passed";
}

/** A message of `text` from `author` to the other agent, under `msgId` or a new id. */
function message(
  author: AgentName,
  text: string,
  reference: Pick<Message, "replyToMsgId"> = {},
  msgId?: MsgId,
): Message {
  return Schema.decodeSync(Message)({
    author,
    recipient: author === alice ? bob : alice,
    msgId: msgId ?? `msg_${randomBytes(32).toString("base64url")}`,
    ...reference,
    text,
  });
}

/** What the structure check of an agent that holds `norm` says of `candidate` at `now`, after `history`. */
function check(
  norm: Norm,
  candidate: Message,
  history: readonly EarlierMessage[] = [],
  now = 0,
) {
  vi.setSystemTime(now);
  return Effect.runSync(structureCheck([norm])(candidate, history));
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
          text: actText(
            taken.act,
            "turns",
            taken.act === "finish" ? {} : { value: taken.value },
          ),
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
        const timed = normFrom(`
(norm timed :initiator Sender :participant Receiver)
(content hold :by Sender :starts true :initiates held
  (field wait :type integer :minimum 0))
(content poll :by (Sender Receiver) :when held :max ${maximum}
  :after held :delay wait :unit seconds)
`);
        const history: EarlierMessage[] = [];
        const start = 1_000;
        const due = start + wait * 1000;
        attempt(timed, history, {
          author: alice,
          text: actText("hold", "timed", { wait }),
          now: start,
          reply: Option.none(),
        });
        const poll = (now: number) =>
          attempt(timed, history, {
            author: bob,
            text: actText("poll", "timed"),
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
const exchange = normFrom(`
(norm exchange :initiator Sender :participant Receiver :on-refusal reject)
(content open :by Sender :starts true :initiates opened
  (field value :type string))
(content close :by Receiver :when opened :ends true)
(content reject :by (Sender Receiver) :when opened
  (field reason :type string))
`);

it("refuses every text that is not an act of a held norm", () => {
  fc.assert(
    fc.property(fc.string({ minLength: 1 }), (text) => {
      expect(check(exchange, message(alice, text))).toMatchObject({
        kind: "refused",
        reason: "rule-violation",
        ruleId: "norm-structural",
        detail: { stage: "structural" },
      });
    }),
    { numRuns: 60 },
  );
});

it.each(["ask", "tell"])(
  "passes the act %s with the rules its norm gives that act and every act",
  (act) => {
    const duo = normFrom(`
(norm duo :initiator Sender :participant Receiver)
(content ask :by Sender :starts true :ends true)
(content tell :by Sender :starts true :ends true)
(rule :act ask "Ask politely.")
(rule :act tell "Tell briefly.")
(rule "Stay on the subject.")
`);

    expect(check(duo, message(alice, actText(act, "duo")))).toMatchObject({
      kind: "passed",
      textRules: [
        { id: act === "ask" ? "duo-rule-1" : "duo-rule-2", applies: "both" },
        { id: "duo-rule-3", applies: "both" },
      ],
      protocol: { norm: "duo", act },
    });
  },
);

it.each([
  { checked: { checkedAt: 1, allowed: true }, continues: "passed" },
  { checked: { checkedAt: 1, allowed: false }, continues: "refused" },
  { checked: { allowed: false }, continues: "refused" },
])(
  "continues an instance only from an opening act the firewall allowed: $checked",
  ({ checked, continues }) => {
    const open = message(alice, actText("open", "exchange", { value: "x" }));

    const result = check(
      exchange,
      message(bob, actText("close", "exchange"), { replyToMsgId: open.msgId }),
      [{ message: open, ...checked }],
    );

    expect(result.kind).toBe(continues);
  },
);

it.each([
  { what: "plain text", text: "hello", stage: "structural" },
  {
    what: "an act the norm lacks",
    text: "(unknown :ontology exchange)",
    stage: "structural",
  },
  {
    what: "a field its form lacks",
    text: '(close :ontology exchange :extra "unasked")',
    stage: "structural",
  },
  {
    what: "a comment after the act",
    text: "(close :ontology exchange) ; note",
    stage: "structural",
  },
  {
    what: "an unclosed act",
    text: "(close :ontology exchange",
    stage: "structural",
  },
  {
    what: "a second opening",
    text: '(open :ontology exchange :value "again")',
    stage: "protocol",
  },
  {
    what: "an act of another norm",
    text: "(close :ontology other)",
    stage: "protocol",
  },
])(
  "refuses $what in an instance under the norm's $stage rule, and answers with the norm's refusal act",
  ({ text, stage }) => {
    const open = message(alice, actText("open", "exchange", { value: "x" }));

    const result = check(
      exchange,
      message(bob, text, { replyToMsgId: open.msgId }),
      [{ message: open, checkedAt: 1, allowed: true }],
    );

    expect(result).toMatchObject({
      kind: "refused",
      ruleId: `exchange-${stage}`,
    });
    expect(result.kind === "refused" && result.reply).toBe(
      `(reject :ontology exchange :reason ${JSON.stringify(
        result.kind === "refused" && result.feedback,
      )})`,
    );
  },
);

it("phrases the refusal of an act it passes as the norm's refusal act", () => {
  const open = message(alice, actText("open", "exchange", { value: "x" }));

  const result = check(
    exchange,
    message(bob, actText("close", "exchange"), { replyToMsgId: open.msgId }),
    [{ message: open, checkedAt: 1, allowed: true }],
  );

  expect(result.kind === "passed" && result.replyTo?.('Say "please".')).toBe(
    '(reject :ontology exchange :reason "Say \\"please\\".")',
  );
});

/** A norm whose one act, `open`, has the declared fields. */
const sampleOf = (fields: readonly string[]) =>
  normFrom(`
(norm sample :initiator Sender :participant Receiver)
(content open :by Sender :starts true :ends true
  ${fields.join("\n  ")})
`);

/** Whether a field declared as `declaration` accepts the value an act states as `value`. */
const accepted = (
  declaration: string,
  value: string | number | readonly string[],
) =>
  check(
    sampleOf([`(field field ${declaration})`]),
    message(alice, actText("open", "sample", { field: value })),
  ).kind === "passed";

it("names exactly the fields an act is missing, adds or gets wrong", () => {
  const names = ["slot", "hours", "note", "constructor", "prototype"];
  const field = fc.record({
    declared: fc.boolean(),
    required: fc.boolean(),
    stated: fc.constantFrom("absent", "valid", "invalid"),
  });
  fc.assert(
    fc.property(fc.tuple(field, field, field, field, field), (fields) => {
      const named = fields.map((state, index) => ({
        ...state,
        name: names[index] ?? "",
      }));
      const result = check(
        sampleOf(
          named
            .filter((state) => state.declared)
            .map(
              (state) =>
                `(field ${state.name} :type string :max-length 3${state.required ? "" : " :optional true"})`,
            ),
        ),
        message(
          alice,
          actText(
            "open",
            "sample",
            Object.fromEntries(
              named
                .filter((state) => state.stated !== "absent")
                .map((state) => [
                  state.name,
                  state.stated === "valid" ? "yes" : "never",
                ]),
            ),
          ),
        ),
      );
      const failureOf = (state: (typeof named)[number]) => {
        if (!state.declared) {
          return state.stated === "absent" ? [] : ["is not a valid field of"];
        }
        if (state.stated === "absent") {
          return state.required ? ["is required for"] : [];
        }
        return state.stated === "valid" ? [] : ["is not valid for"];
      };
      const expected = named.flatMap((state) =>
        failureOf(state).map(
          (failure) => `Error: ${state.name} ${failure} open in sample.`,
        ),
      );
      const stated =
        result.kind === "refused"
          ? result.feedback.split(/(?<=\.) (?=Error: )/u)
          : [];
      expect(new Set(stated)).toEqual(new Set(expected));
      expect(stated).toHaveLength(expected.length);
    }),
  );
});

it("refuses a value another type of field is given", () => {
  const kinds = [
    { declaration: ":type string", value: "a" },
    { declaration: ":type integer", value: 1 },
    { declaration: ":type multi-select :choices (a)", value: ["a"] },
  ];
  for (const asked of kinds) {
    for (const given of kinds) {
      expect(accepted(asked.declaration, given.value)).toBe(asked === given);
    }
  }
});

it("holds an integer to its bounds and to whole values", () => {
  fc.assert(
    fc.property(
      fc.integer({ min: -100, max: 100 }),
      fc.integer({ min: 0, max: 50 }),
      fc.oneof(
        fc.integer({ min: -200, max: 200 }),
        fc.double({ min: -200, max: 200, noNaN: true }),
      ),
      (minimum, span, value) => {
        const maximum = minimum + span;
        expect(
          accepted(
            `:type integer :minimum ${minimum} :maximum ${maximum}`,
            value,
          ),
        ).toBe(Number.isInteger(value) && value >= minimum && value <= maximum);
      },
    ),
  );
});

it("reads any quoted string and counts its length in code points against its bounds", () => {
  fc.assert(
    fc.property(
      fc.string({ unit: "grapheme", maxLength: 8 }),
      fc.integer({ min: 0, max: 4 }),
      fc.integer({ min: 0, max: 4 }),
      (text, minLength, span) => {
        const maxLength = minLength + span;
        const length = Array.from(text).length;
        expect(
          accepted(
            `:type string :min-length ${minLength} :max-length ${maxLength}`,
            text,
          ),
        ).toBe(length >= minLength && length <= maxLength);
      },
    ),
  );
});

it("checks each selected item against the choices and their count against its bounds", () => {
  fc.assert(
    fc.property(
      fc.array(fc.constantFrom("a", "b", "x"), { maxLength: 5 }),
      fc.integer({ min: 0, max: 3 }),
      fc.integer({ min: 0, max: 3 }),
      (selected, minItems, span) => {
        const maxItems = minItems + span;
        expect(
          accepted(
            `:type multi-select :choices (a b) :min-items ${minItems} :max-items ${maxItems}`,
            selected,
          ),
        ).toBe(
          selected.length >= minItems &&
            selected.length <= maxItems &&
            !selected.includes("x"),
        );
      },
    ),
  );
});
