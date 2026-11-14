/**
 * @file A conversation's norm state as a fold over its ordered history. Each
 * act is judged against the state the acts before it left. An act that breaks
 * its norm changes nothing, and neither does an act another member answered
 * with `feedback`'s `not-understood`, such as one a member's firewall withheld
 * under a norm's English rule. Every member reading the same history reaches
 * the same state and the same verdicts.
 *
 * The conversation, its set of members, identifies the agreement and the norm
 * instance, so acts carry no conversation id. At most one agreement or norm
 * instance is in flight. A unanimous `norm-agreement` registers its norm for
 * the next instance, which any member starts with one of its opening acts and
 * which ends when its protocol completes or a member cancels it. Norm
 * discovery and feedback run beside that instance; a discovery answer names
 * its query with `:in-reply-to`. Chat needs no state.
 */

import { Effect, Either, Option, Struct } from "effect";
import type { Act, ActValue } from "./acts.js";
import {
  type RequestedSchema,
  type ResponseContent,
  type ResponseFieldFailures,
  validateResponseContent,
} from "../communication/index.js";
import { type Norm, readNorm, standsAlone } from "./documents.js";
import { allowedActs, ended, type Instance, openingActs } from "./protocols.js";

/** The names of the norms the daemon's handling of acts refers to. */
export const normNames = {
  agreement: "norm-agreement",
  discovery: "norm-discovery",
  feedback: "feedback",
  chat: "chat",
} as const;

const codeNamedNormNames: ReadonlySet<string> = new Set(
  Object.values(normNames),
);

/** One conversation's progress under its norms. */
export interface ConversationNorms {
  /** An open `norm-agreement` proposal and the name of the norm it proposes. */
  readonly agreement?: {
    readonly normName: string;
    readonly instance: Instance;
  };
  /** The name of the norm every member agreed to, waiting for its instance to start. */
  readonly agreedNormName?: string;
  /** The norm instance in flight. */
  readonly instance?: Instance;
  /** Open norm-discovery queries, by the id of the message that asked. */
  readonly queries: ReadonlyMap<string, Instance>;
  /** Norms a member sent through norm discovery, which all members can read. */
  readonly fetched: ReadonlyMap<string, Norm>;
}

/** Why an act breaks its norm, and the fields that failed. */
interface Breach {
  readonly reason:
    | "out-of-turn"
    | "no-agreement"
    | "in-flight"
    | "unknown-instance"
    | "invalid-content";
  readonly norm: string;
  readonly fields?: ResponseFieldFailures;
}

/** An act's verdict, and the state after an act that conforms. */
export type Verdict =
  | { readonly kind: "conforms"; readonly state: ConversationNorms }
  | { readonly kind: "breaks"; readonly breach: Breach };

/** One judgment's inputs: the norms this agent holds and the conversation's members. */
export interface Setting {
  readonly held: ReadonlyMap<string, Norm>;
  readonly members: readonly string[];
}

/** An act, the author of the message that carried it, and that message's id. */
export interface SentAct {
  readonly act: Act;
  readonly author: string;
  readonly msgId: string;
}

/** A conversation with no acts. */
const emptyState: ConversationNorms = {
  queries: new Map(),
  fetched: new Map(),
};

/**
 * Folds acts in history order, skipping acts of unknown norms, acts that break
 * theirs, and acts another member answered with `not-understood`.
 */
export function foldActs(
  setting: Setting,
  acts: readonly SentAct[],
): Effect.Effect<ConversationNorms> {
  const answered = notUnderstoodActs(acts);
  return Effect.reduce(acts, emptyState, (state, sent) =>
    answered.has(sent.msgId) ||
    normOf(setting, state, sent.act.ontology) === undefined
      ? Effect.succeed(state)
      : judge(setting, state, sent).pipe(
          Effect.map((verdict) =>
            verdict.kind === "conforms" ? verdict.state : state,
          ),
        ),
  );
}

/**
 * Judges one act of a known norm against the state before it. Agreement and
 * discovery have effects the daemon implements; every other norm, built-in or
 * not, is judged by its document alone.
 */
export function judge(
  setting: Setting,
  state: ConversationNorms,
  sent: SentAct,
): Effect.Effect<Verdict> {
  const norm = normOf(setting, state, sent.act.ontology);
  if (norm === undefined) {
    return Effect.succeed(breaks("no-agreement", sent.act));
  }
  switch (norm.name) {
    case normNames.agreement:
      return agreementAct(setting, state, norm, sent);
    case normNames.discovery:
      return discoveryAct(setting, state, norm, sent);
    default:
      return normAct(setting, state, norm, sent);
  }
}

/** An act a member may send: its performative, its norm and, for a proposal or a document query, the name of the norm it names. */
export interface SendableAct {
  readonly performative: string;
  readonly ontology: string;
  readonly namedNormName?: string;
}

/**
 * The acts `member` may send now: the next acts of the agreement or norm
 * instance in flight, the agreed norm's opening acts or, with nothing in
 * flight, a proposal of each norm held here that has turns and a query for
 * that norm's document. None while another member's turn is next. `cancel`,
 * which any member may send at any time, is not listed.
 */
export function sendableActs(
  setting: Setting,
  state: ConversationNorms,
  member: string,
): readonly SendableAct[] {
  const { agreement, instance, agreedNormName } = state;
  if (agreement !== undefined) {
    return allowedActs(agreement.instance, member).map((performative) => ({
      performative,
      ontology: normNames.agreement,
    }));
  }
  if (instance !== undefined) {
    return allowedActs(instance, member).map((performative) => ({
      performative,
      ontology: instance.normName,
    }));
  }
  if (agreedNormName !== undefined) {
    const norm = normOf(setting, state, agreedNormName);
    const protocol = norm?.protocolName ?? norm?.turns;
    return protocol === undefined
      ? []
      : openingActs(protocol).map((performative) => ({
          performative,
          ontology: agreedNormName,
        }));
  }
  return [...setting.held.values()]
    .filter((norm) => !codeNamedNormNames.has(norm.name) && !standsAlone(norm))
    .flatMap(({ name }) => [
      {
        performative: "propose",
        ontology: normNames.agreement,
        namedNormName: name,
      },
      {
        performative: "query-ref",
        ontology: normNames.discovery,
        namedNormName: name,
      },
    ]);
}

/** The norm an act names, if this member holds it or a member sent it. */
export function normOf(
  setting: Setting,
  state: ConversationNorms,
  normName: string,
): Norm | undefined {
  return setting.held.get(normName) ?? state.fetched.get(normName);
}

/** The ids of the messages whose acts a member other than their author answered with `not-understood`. */
function notUnderstoodActs(acts: readonly SentAct[]): ReadonlySet<string> {
  const authors = new Map(acts.map(({ msgId, author }) => [msgId, author]));
  return new Set(
    acts.flatMap(({ act, author }) => {
      const answered =
        act.ontology === normNames.feedback &&
        act.performative === "not-understood"
          ? act.inReplyTo
          : undefined;
      const answeredAuthor =
        answered === undefined ? undefined : authors.get(answered);
      return answered !== undefined &&
        answeredAuthor !== undefined &&
        answeredAuthor !== author
        ? [answered]
        : [];
    }),
  );
}

/**
 * `propose` opens an agreement on `:norm` under the protocol the
 * norm-agreement document names; every other member accepts or rejects it
 * once.
 */
function agreementAct(
  setting: Setting,
  state: ConversationNorms,
  agreementNorm: Norm,
  { act, author }: SentAct,
): Effect.Effect<Verdict> {
  return Effect.gen(function* () {
    const failure = yield* checkFields(setting, state, act);
    if (failure !== undefined) {
      return failure;
    }
    const protocol = agreementNorm.protocolName;
    if (protocol === undefined) {
      return breaks("invalid-content", act);
    }
    const proposedNormName = act.fields.norm;
    if (act.performative === "propose") {
      if (state.agreement !== undefined || state.instance !== undefined) {
        return breaks("in-flight", act);
      }
      if (typeof proposedNormName !== "string") {
        return breaks("invalid-content", act);
      }
      const rest = Struct.omit(state, "agreedNormName");
      return conforms({
        ...rest,
        agreement: {
          normName: proposedNormName,
          instance: open(setting, act, author, protocol),
        },
      });
    }
    const agreement = state.agreement;
    if (agreement === undefined) {
      return breaks("unknown-instance", act);
    }
    const advanced = advance(agreement.instance, act, author);
    if (advanced === undefined) {
      return breaks("out-of-turn", act);
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
        sent.author === advanced.initiator ||
        sent.performative === "accept-proposal",
    );
    return conforms(
      unanimous ? { ...rest, agreedNormName: agreement.normName } : rest,
    );
  });
}

/**
 * `query-ref` asks for the names of the norms a member holds, or with `:norm`
 * for one norm's document, under the protocol the norm-discovery document
 * names. Any member may ask at any time. Another member answers with
 * `inform`, whose `:in-reply-to` names the query's message and whose
 * `:content` carries the document.
 */
function discoveryAct(
  setting: Setting,
  state: ConversationNorms,
  discoveryNorm: Norm,
  { act, author, msgId }: SentAct,
): Effect.Effect<Verdict> {
  return Effect.gen(function* () {
    const failure = yield* checkFields(setting, state, act);
    if (failure !== undefined) {
      return failure;
    }
    const protocol = discoveryNorm.protocolName;
    if (protocol === undefined) {
      return breaks("invalid-content", act);
    }
    if (act.performative === "query-ref") {
      return conforms({
        ...state,
        queries: new Map([
          ...state.queries,
          [msgId, open(setting, act, author, protocol)],
        ]),
      });
    }
    const queryMsgId = act.inReplyTo;
    const query =
      queryMsgId === undefined ? undefined : state.queries.get(queryMsgId);
    if (queryMsgId === undefined || query === undefined) {
      return breaks("unknown-instance", act);
    }
    const advanced = advance(query, act, author);
    if (advanced === undefined) {
      return breaks("out-of-turn", act);
    }
    const queries = new Map(state.queries);
    if (ended(advanced) || act.performative === "cancel") {
      queries.delete(queryMsgId);
    } else {
      queries.set(queryMsgId, advanced);
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
 * protocol orders the rest.
 */
function normAct(
  setting: Setting,
  state: ConversationNorms,
  norm: Norm,
  { act, author }: SentAct,
): Effect.Effect<Verdict> {
  return Effect.gen(function* () {
    const failure = yield* checkFields(setting, state, act);
    if (failure !== undefined) {
      return failure;
    }
    const protocol = norm.protocolName ?? norm.turns;
    if (protocol === undefined) {
      return Object.hasOwn(norm.forms, act.performative)
        ? conforms(state)
        : breaks("out-of-turn", act);
    }
    const current = state.instance;
    if (current === undefined) {
      if (!openingActs(protocol).includes(act.performative)) {
        return breaks("out-of-turn", act);
      }
      if (state.agreedNormName !== norm.name) {
        return breaks("no-agreement", act);
      }
      const rest = Struct.omit(state, "agreedNormName");
      const opened = open(setting, act, author, protocol);
      return conforms(ended(opened) ? rest : { ...rest, instance: opened });
    }
    if (current.normName !== norm.name) {
      return breaks("in-flight", act);
    }
    const advanced = advance(current, act, author);
    if (advanced === undefined) {
      return breaks("out-of-turn", act);
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
  author: string,
): Instance | undefined {
  const allowed =
    act.performative === "cancel" ||
    allowedActs(instance, author).includes(act.performative);
  return allowed
    ? {
        ...instance,
        acts: [...instance.acts, { author, performative: act.performative }],
      }
    : undefined;
}

function open(
  setting: Setting,
  act: Act,
  author: string,
  protocol: Instance["protocol"],
): Instance {
  return {
    normName: act.ontology,
    protocol,
    initiator: author,
    participants: setting.members.filter((member) => member !== author),
    acts: [{ author, performative: act.performative }],
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
function checkFields(
  setting: Setting,
  state: ConversationNorms,
  act: Act,
): Effect.Effect<Verdict | undefined> {
  const norm = normOf(setting, state, act.ontology);
  const form: RequestedSchema = norm?.forms[act.performative] ?? {
    type: "object",
    properties: {},
  };
  return validateResponseContent(form, answerOf(form, act.fields)).pipe(
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
  form: RequestedSchema,
  fields: Readonly<Record<string, ActValue>>,
): ResponseContent {
  return Object.fromEntries(
    Object.entries(fields).map(([field, value]) => {
      const flag = value === "true" || value === "false";
      return [
        field,
        flag && form.properties[field]?.type === "boolean"
          ? value === "true"
          : value,
      ];
    }),
  );
}

function conforms(state: ConversationNorms): Verdict {
  return { kind: "conforms", state };
}

function breaks(reason: Breach["reason"], act: Act): Verdict {
  return { kind: "breaks", breach: { reason, norm: act.ontology } };
}
