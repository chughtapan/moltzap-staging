/** @file A conversation's protocol permits exactly the acts its contract's turn rules allow, from its earlier messages and the firewall's checks of them. */

import { Effect, Either, FastCheck as fc, Option, Schema } from "effect";
import { expect, it } from "vitest";
import type { EarlierMessage } from "../firewall/index.js";
import type { AgentCard } from "../identity.js";
import { type Message, messageWithId, MsgId } from "../communication/index.js";
import { peer } from "../firewall/__tests__/firewall-fixtures.js";
import {
  contractOf,
  form,
  noFields,
  valueField,
} from "./__tests__/norms-fixtures.js";
import {
  type Act,
  checkStructure,
  type CompiledContract,
  formatAct,
} from "./contract.js";
import { checkProtocol, projectProtocol } from "./protocol.js";

const alice = peer("alice", 1);
const bob = peer("bob", 2);
const agents = [alice, bob] as const;

/**
 * Offers alternate. `offer` opens an instance. Only the recipient of the
 * current offer may `counter` it or `choose` its value, replying in its
 * chain, and choosing settles it. Either agent may `finish`.
 */
const turns = contractOf("turns", [
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
 * to the history as one the firewall allowed when the protocol permits it.
 */
function attempt(
  contract: CompiledContract,
  history: EarlierMessage[],
  taken: {
    readonly author: AgentCard;
    readonly act: Act;
    readonly now: number;
    readonly reply: Option.Option<MsgId>;
  },
) {
  const replyToMsgId = Option.getOrUndefined(taken.reply);
  const view = Effect.runSync(projectProtocol([contract], history));
  const allowed = Effect.runSync(
    Effect.either(
      Effect.flatMap(checkStructure(contract, taken.act), (declared) =>
        checkProtocol(view, contract, declared, {
          act: taken.act,
          author: taken.author.agentId,
          now: taken.now,
          ...(replyToMsgId === undefined ? {} : { replyToMsgId }),
        }),
      ),
    ),
  ).pipe(Either.isRight);
  history.push({
    message: actMessage(taken.author, stepId(history.length), taken.act, {
      replyToMsgId,
    }),
    checkedAt: taken.now,
    allowed,
  });
  return allowed;
}

/** A direct message from `author` to the other agent that states `act`. */
function actMessage(
  author: AgentCard,
  msgId: MsgId,
  act: Act,
  reference: Pick<Message, "replyToMsgId">,
): Message {
  return Effect.runSync(
    messageWithId(
      author,
      agents.filter((agent) => agent.agentId !== author.agentId),
      { msgId, ...reference },
      [{ type: "text", text: formatAct(act) }],
    ),
  );
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
 * What the `turns` contract's turn rules say of a walk, kept apart from the
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
      expect(
        Effect.runSync(projectProtocol([turns], [...history, ...history])),
      ).toEqual(Effect.runSync(projectProtocol([turns], history)));
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
        const timed = contractOf("timed", [
          {
            ...form,
            act: "hold",
            by: ["Sender"],
            fields: {
              type: "object",
              properties: { wait: { type: "integer", minimum: 0 } },
              required: ["wait"],
            },
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
