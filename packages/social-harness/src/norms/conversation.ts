/**
 * @file A conversation's norm state as a fold over its certified history. Each
 * act is judged against the state the acts before it left, and an act that
 * breaks its norm changes nothing, so every member reading the same history
 * reaches the same state and the same verdicts.
 *
 * The conversation, its set of members, identifies the agreement and the norm
 * instance, so acts carry no conversation id. At most one agreement or norm
 * instance is in flight. A unanimous `norm-agreement` registers its norm for
 * the next instance, which any member starts with one of its opening acts and
 * which ends when its protocol or its own turns complete, or a member cancels
 * it. Norm
 * discovery and feedback run beside that instance; a discovery answer names
 * its query with `:in-reply-to`. Chat needs no state.
 */

import { Effect, Either, Option, Struct } from "effect";
import type { Act, ActValue } from "./acts.js";
import {
  type AnswerContent,
  type AnswerFieldFailures,
  type RequestedSchema,
  validateAnswer,
} from "../communication/messages.js";
import { type Norm, readNorm } from "./documents.js";
import { allowedActs, ended, type Instance, openingActs } from "./protocols.js";

/** The norms built into every daemon's handling of acts. */
export const builtIns = {
  agreement: "norm-agreement",
  discovery: "norm-discovery",
  feedback: "feedback",
  chat: "chat",
} as const;

const builtInNames: ReadonlySet<string> = new Set(Object.values(builtIns));

/** One conversation's progress under its norms. */
export interface State {
  /** An open `norm-agreement` proposal and the norm it proposes. */
  readonly agreement?: { readonly norm: string; readonly instance: Instance };
  /** The norm every member agreed to, waiting for its instance to start. */
  readonly agreed?: string;
  /** The norm instance in flight. */
  readonly instance?: Instance;
  /** Open norm-discovery queries, by the id of the message that asked. */
  readonly queries: ReadonlyMap<string, Instance>;
  /** Norms a member sent through norm discovery, which all members can read. */
  readonly fetched: ReadonlyMap<string, Norm>;
}

/** Why an act breaks its norm, what its sender may send instead, and the fields that failed. */
interface Breach {
  readonly reason:
    | "out-of-turn"
    | "no-agreement"
    | "in-flight"
    | "unknown-instance"
    | "invalid-content";
  readonly norm: string;
  readonly allowed?: readonly string[];
  readonly fields?: AnswerFieldFailures;
}

/** An act's verdict, and the state after an act that conforms. */
export type Verdict =
  | { readonly kind: "conforms"; readonly state: State }
  | { readonly kind: "breaks"; readonly breach: Breach };

/** One judgment's inputs: the norms this agent holds and the conversation's members. */
export interface Setting {
  readonly held: ReadonlyMap<string, Norm>;
  readonly members: readonly string[];
}

/** An act, the member that sent it, and the id of the message that carried it. */
export interface SentAct {
  readonly act: Act;
  readonly sender: string;
  readonly msgId: string;
}

/** A conversation with no acts. */
const emptyState: State = { queries: new Map(), fetched: new Map() };

/** The norm an act names, if this member holds it or a member sent it. */
export function normOf(
  setting: Setting,
  state: State,
  name: string,
): Norm | undefined {
  return setting.held.get(name) ?? state.fetched.get(name);
}

/** Folds acts in history order, skipping acts of unknown norms and acts that break theirs. */
export function foldActs(
  setting: Setting,
  acts: readonly SentAct[],
): Effect.Effect<State> {
  return Effect.reduce(acts, emptyState, (state, sent) =>
    normOf(setting, state, sent.act.ontology) === undefined
      ? Effect.succeed(state)
      : judge(setting, state, sent).pipe(
          Effect.map((verdict) =>
            verdict.kind === "conforms" ? verdict.state : state,
          ),
        ),
  );
}

/** Judges one act of a known norm against the state before it. */
export function judge(
  setting: Setting,
  state: State,
  sent: SentAct,
): Effect.Effect<Verdict> {
  const { act, sender } = sent;
  switch (act.ontology) {
    case builtIns.agreement:
      return agreementAct(setting, state, act, sender);
    case builtIns.discovery:
      return discoveryAct(setting, state, sent);
    case builtIns.feedback:
      return standalone(setting, state, act, ["not-understood"]);
    case builtIns.chat:
      return standalone(setting, state, act, ["inform"]);
    default:
      return normAct(setting, state, act, sender);
  }
}

function standalone(
  setting: Setting,
  state: State,
  act: Act,
  performatives: readonly string[],
): Effect.Effect<Verdict> {
  if (!performatives.includes(act.performative)) {
    return Effect.succeed(breaks("out-of-turn", act, performatives));
  }
  return checkContent(setting, state, act).pipe(
    Effect.map((failure) => failure ?? conforms(state)),
  );
}

/** `propose` opens an agreement on `:norm`; every other member accepts or rejects it once. */
function agreementAct(
  setting: Setting,
  state: State,
  act: Act,
  sender: string,
): Effect.Effect<Verdict> {
  return Effect.gen(function* () {
    const failure = yield* checkContent(setting, state, act);
    if (failure !== undefined) {
      return failure;
    }
    const proposed = act.fields.norm;
    if (act.performative === "propose") {
      if (state.agreement !== undefined || state.instance !== undefined) {
        return breaks("in-flight", act, []);
      }
      if (typeof proposed !== "string" || builtInNames.has(proposed)) {
        return breaks("invalid-content", act, ["propose"]);
      }
      const rest = Struct.omit(state, "agreed");
      return conforms({
        ...rest,
        agreement: {
          norm: proposed,
          instance: open(setting, act, sender, "fipa-propose"),
        },
      });
    }
    const agreement = state.agreement;
    if (agreement === undefined) {
      return breaks("unknown-instance", act, []);
    }
    const advanced = advance(agreement.instance, act, sender);
    if (advanced === undefined) {
      return breaks("out-of-turn", act, [
        ...allowedActs(agreement.instance, sender),
        "cancel",
      ]);
    }
    const rest = Struct.omit(state, "agreement");
    if (act.performative === "cancel") {
      return conforms(rest);
    }
    if (!ended(advanced)) {
      return conforms({
        ...state,
        agreement: { ...agreement, instance: advanced },
      });
    }
    const unanimous = advanced.acts.every(
      (sent) =>
        sent.sender === advanced.initiator ||
        sent.performative === "accept-proposal",
    );
    return conforms(unanimous ? { ...rest, agreed: agreement.norm } : rest);
  });
}

/**
 * `query-ref` asks for the names of the norms a member holds, or with `:norm`
 * for one norm's document. Any member may ask at any time. Another member
 * answers with `inform`, whose `:in-reply-to` names the query's message and
 * whose `:content` carries the document.
 */
function discoveryAct(
  setting: Setting,
  state: State,
  { act, sender, msgId }: SentAct,
): Effect.Effect<Verdict> {
  return Effect.gen(function* () {
    const failure = yield* checkContent(setting, state, act);
    if (failure !== undefined) {
      return failure;
    }
    if (act.performative === "query-ref") {
      return conforms({
        ...state,
        queries: new Map([
          ...state.queries,
          [msgId, open(setting, act, sender, "fipa-query")],
        ]),
      });
    }
    const asked = act.inReplyTo;
    const query = asked === undefined ? undefined : state.queries.get(asked);
    if (asked === undefined || query === undefined) {
      return breaks("unknown-instance", act, []);
    }
    const advanced = advance(query, act, sender);
    if (advanced === undefined) {
      return breaks("out-of-turn", act, allowedActs(query, sender));
    }
    const queries = new Map(state.queries);
    if (ended(advanced) || act.performative === "cancel") {
      queries.delete(asked);
    } else {
      queries.set(asked, advanced);
    }
    return conforms({
      ...state,
      queries,
      fetched: received(state.fetched, act),
    });
  });
}

/**
 * Acts of an agreed norm: the opening act starts its instance, and the norm's
 * protocol or its own turns order the rest.
 */
function normAct(
  setting: Setting,
  state: State,
  act: Act,
  sender: string,
): Effect.Effect<Verdict> {
  return Effect.gen(function* () {
    const norm = normOf(setting, state, act.ontology);
    const failure = yield* checkContent(setting, state, act);
    if (norm === undefined || failure !== undefined) {
      return failure ?? breaks("no-agreement", act, []);
    }
    const protocol = norm.protocol ?? norm.turns;
    if (protocol === undefined) {
      return conforms(state);
    }
    const current = state.instance;
    if (current === undefined) {
      if (!openingActs(protocol).includes(act.performative)) {
        return breaks("out-of-turn", act, openingActs(protocol));
      }
      if (state.agreed !== norm.name) {
        return breaks("no-agreement", act, []);
      }
      const rest = Struct.omit(state, "agreed");
      const opened = open(setting, act, sender, protocol);
      return conforms(ended(opened) ? rest : { ...rest, instance: opened });
    }
    if (current.norm !== norm.name) {
      return breaks("in-flight", act, []);
    }
    const advanced = advance(current, act, sender);
    if (advanced === undefined) {
      return breaks("out-of-turn", act, [
        ...allowedActs(current, sender),
        "cancel",
      ]);
    }
    const rest = Struct.omit(state, "instance");
    return conforms(
      act.performative === "cancel" || ended(advanced)
        ? rest
        : { ...state, instance: advanced },
    );
  });
}

/** The instance after `act`, or nothing when the protocol does not allow it now. */
function advance(
  instance: Instance,
  act: Act,
  sender: string,
): Instance | undefined {
  const allowed =
    act.performative === "cancel" ||
    allowedActs(instance, sender).includes(act.performative);
  return allowed
    ? {
        ...instance,
        acts: [...instance.acts, { sender, performative: act.performative }],
      }
    : undefined;
}

function open(
  setting: Setting,
  act: Act,
  sender: string,
  protocol: Instance["protocol"],
): Instance {
  return {
    norm: act.ontology,
    protocol,
    initiator: sender,
    participants: setting.members.filter((member) => member !== sender),
    acts: [{ sender, performative: act.performative }],
  };
}

/** A norm document an `inform` carries is readable in this conversation once it parses. */
function received(
  fetched: ReadonlyMap<string, Norm>,
  act: Act,
): ReadonlyMap<string, Norm> {
  if (act.performative !== "inform" || act.content === undefined) {
    return fetched;
  }
  return Option.match(Either.getRight(readNorm(act.content)), {
    onNone: () => fetched,
    onSome: (norm) =>
      act.fields.norm === norm.name
        ? new Map([...fetched, [norm.name, norm]])
        : fetched,
  });
}

/**
 * Checks an act's fields against the form its norm gives its performative; a
 * performative with no form takes no fields.
 */
function checkContent(
  setting: Setting,
  state: State,
  act: Act,
): Effect.Effect<Verdict | undefined> {
  const norm = normOf(setting, state, act.ontology);
  const schema: RequestedSchema = norm?.contents[act.performative] ?? {
    type: "object",
    properties: {},
  };
  return validateAnswer(schema, answerOf(schema, act.fields)).pipe(
    Effect.as(undefined),
    Effect.catchAll((fields) =>
      Effect.succeed<Verdict>({
        kind: "breaks",
        breach: {
          reason: "invalid-content",
          norm: act.ontology,
          fields,
        },
      }),
    ),
  );
}

/** Reads `true` and `false` as booleans where the form asks for one. */
function answerOf(
  schema: RequestedSchema,
  fields: Readonly<Record<string, ActValue>>,
): AnswerContent {
  return Object.fromEntries(
    Object.entries(fields).map(([field, value]) => {
      const flag = value === "true" || value === "false";
      return [
        field,
        flag && schema.properties[field]?.type === "boolean"
          ? value === "true"
          : value,
      ];
    }),
  );
}

function conforms(state: State): Verdict {
  return { kind: "conforms", state };
}

function breaks(
  reason: Breach["reason"],
  act: Act,
  allowed: readonly string[],
): Verdict {
  return {
    kind: "breaks",
    breach: {
      reason,
      norm: act.ontology,
      ...(allowed.length === 0 ? {} : { allowed }),
    },
  };
}
