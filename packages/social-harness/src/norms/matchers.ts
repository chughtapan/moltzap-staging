/**
 * @file The norm checks a firewall rule chain names. Each reads the message as
 * an act of a norm this agent holds, and the conversation's norm state from
 * the permitted history, and reports one fact:
 *
 * - `norms.unparsed`: the message is not an act of a norm held here;
 * - `norms.nonconforming`: the act breaks its norm, or the history it needs is
 *   missing;
 * - `norms.query`: a peer's norm-discovery query this agent answers;
 * - `norms.consent`: the agent proposes or accepts a norm, which its owner
 *   approves.
 *
 * The firewall's rules decide what each fact does to the message.
 */

import { Effect, Option } from "effect";
import type { Matcher, MatchInput, MatchResult } from "../firewall/chain.js";
import type { Norm } from "./documents.js";
import {
  type Act,
  formatAct,
  messageText,
  type Reading,
  readText,
} from "./acts.js";
import {
  builtIns,
  foldActs,
  judge,
  normOf,
  type Setting,
  type State,
  type Verdict,
} from "./conversation.js";

/** What one matcher call reads, computed once for every matcher the chain asks. */
interface Analysis {
  readonly reading: Reading;
  readonly setting: Setting;
  readonly state: State;
  readonly complete: boolean;
  readonly verdict?: Verdict;
}

const unmatched = { matched: false } as const;

/** The stateless norms, which judge an act without the acts before it. */
const standalone: ReadonlySet<string> = new Set([
  builtIns.chat,
  builtIns.feedback,
]);

/**
 * The four norm matchers over the norms this agent holds, by the names rule
 * files use.
 */
export function normMatchers(
  norms: readonly Norm[],
): Readonly<Record<string, Matcher>> {
  const held: ReadonlyMap<string, Norm> = new Map(
    norms.map((norm) => [norm.name, norm]),
  );
  const analyses = new WeakMap<MatchInput, Option.Option<Analysis>>();
  const analysis = (input: MatchInput) =>
    Effect.gen(function* () {
      const known = analyses.get(input);
      if (known !== undefined) {
        return known;
      }
      const computed = yield* analyze(held, input);
      analyses.set(input, computed);
      return computed;
    }).pipe(Effect.withSpan("normMatchers"));
  const match =
    (decide: (input: MatchInput, analysis: Analysis) => Matched) =>
    (input: MatchInput) =>
      analysis(input).pipe(
        Effect.map(
          Option.match({
            onNone: () => unmatched,
            onSome: (found) => decide(input, found),
          }),
        ),
      );
  return {
    "norms.unparsed": { match: match(unparsed) },
    "norms.nonconforming": { match: match(nonconforming) },
    "norms.query": { match: match(answerQuery) },
    "norms.consent": { match: match(consent) },
  };
}

type Matched = MatchResult;

function analyze(held: ReadonlyMap<string, Norm>, input: MatchInput) {
  return Effect.gen(function* () {
    const text = messageText(input.message);
    if (text === undefined) {
      return Option.none();
    }
    const { author, recipients } = input.message;
    const setting: Setting = {
      held,
      members: [
        ...new Set([author, ...recipients].map((card) => card.agentId)),
      ],
    };
    const acts = input.evidence.history.flatMap(({ message }) => {
      const body = messageText(message);
      const reading = body === undefined ? undefined : readText(body);
      return reading?.kind === "act"
        ? [{ act: reading.act, sender: message.author.agentId }]
        : [];
    });
    const state = yield* foldActs(setting, acts);
    const reading = readText(text);
    const verdict =
      reading.kind === "act" &&
      normOf(setting, state, reading.act.ontology) !== undefined
        ? yield* judge(setting, state, reading.act, author.agentId)
        : undefined;
    return Option.some<Analysis>({
      reading,
      setting,
      state,
      complete:
        !input.evidence.earlierHistoryOmitted &&
        input.evidence.gaps.length === 0,
      ...(verdict === undefined ? {} : { verdict }),
    });
  });
}

/** Text that is not an act of a held norm; a peer hears `not-understood`, unless it sent one. */
function unparsed(input: MatchInput, analysis: Analysis): Matched {
  const { reading, setting, state } = analysis;
  const accepts = [...setting.held.keys()].join(" ");
  let reason: string;
  if (reading.kind === "invalid") {
    reason = "unparsed";
  } else if (reading.kind === "text") {
    if (setting.held.has(builtIns.chat)) {
      return unmatched;
    }
    reason = "plain-text";
  } else if (normOf(setting, state, reading.act.ontology) === undefined) {
    reason = "unknown-norm";
  } else {
    return unmatched;
  }
  const detail = {
    reason,
    accepts,
    ...(reading.kind === "invalid" ? { error: reading.error } : {}),
  };
  const answerable =
    input.direction === "incoming" &&
    setting.held.has(builtIns.feedback) &&
    !(reading.kind === "act" && reading.act.performative === "not-understood");
  return {
    matched: true,
    detail,
    ...(answerable
      ? {
          reply: formatAct({
            performative: "not-understood",
            ontology: builtIns.feedback,
            inReplyTo: msgIdOf(input),
            fields: { reason, accepts },
          }),
        }
      : {}),
  };
}

/**
 * An act that breaks its norm. Without the full history before it, an act that
 * depends on that history cannot be judged and is held for missing evidence.
 */
function nonconforming(input: MatchInput, analysis: Analysis): Matched {
  const { reading, verdict, state, setting } = analysis;
  if (reading.kind !== "act" || verdict === undefined) {
    return unmatched;
  }
  const { act } = reading;
  if (!analysis.complete && !standalone.has(act.ontology)) {
    return {
      matched: true,
      refusal: "unresolved-evidence",
      detail: { reason: "history-unavailable", norm: act.ontology },
    };
  }
  if (verdict.kind === "breaks") {
    return { matched: true, detail: verdict.breach };
  }
  const accepting =
    input.direction === "outgoing"
      ? actUnder(analysis, builtIns.agreement, ["accept-proposal"])
      : undefined;
  const proposed = state.agreement?.norm;
  if (
    accepting !== undefined &&
    proposed !== undefined &&
    normOf(setting, state, proposed) === undefined
  ) {
    return {
      matched: true,
      detail: { reason: "norm-unknown", norm: proposed },
    };
  }
  return unmatched;
}

/**
 * A peer's norm-discovery query this agent answers: in a direct conversation,
 * any query; in a group, a query for the norm this agent proposed. The answer
 * names the norms held here, or carries the requested norm's document.
 */
function answerQuery(input: MatchInput, analysis: Analysis): Matched {
  const { verdict, setting, state } = analysis;
  const act =
    input.direction === "incoming"
      ? actUnder(analysis, builtIns.discovery, ["query-ref"])
      : undefined;
  if (act === undefined || verdict?.kind !== "conforms") {
    return unmatched;
  }
  const requested = act.fields.norm;
  const direct = setting.members.length === 2;
  const proposer =
    state.agreement?.instance.initiator === input.self.agentId &&
    state.agreement.norm === requested;
  if (!direct && !proposer) {
    return unmatched;
  }
  if (typeof requested !== "string") {
    return direct
      ? answer(act, { norms: [...setting.held.keys()].join(" ") })
      : unmatched;
  }
  const norm = setting.held.get(requested);
  return norm === undefined
    ? unmatched
    : answer(act, { norm: requested }, norm.source);
}

function answer(
  query: Act,
  fields: Readonly<Record<string, string>>,
  content?: string,
): Matched {
  return {
    matched: true,
    reply: formatAct({
      performative: "inform",
      ontology: builtIns.discovery,
      ...(query.conversationId === undefined
        ? {}
        : { conversationId: query.conversationId }),
      ...(content === undefined ? {} : { content }),
      fields,
    }),
  };
}

/** This agent proposing or accepting a norm, which its owner approves. */
function consent(input: MatchInput, analysis: Analysis): Matched {
  const act =
    input.direction === "outgoing"
      ? actUnder(analysis, builtIns.agreement, ["propose", "accept-proposal"])
      : undefined;
  return act !== undefined && analysis.verdict?.kind === "conforms"
    ? { matched: true }
    : unmatched;
}

/** The act the message holds, when it is an act of `ontology` with one of `performatives`. */
function actUnder(
  analysis: Analysis,
  ontology: string,
  performatives: readonly string[],
): Act | undefined {
  const { reading } = analysis;
  return reading.kind === "act" &&
    reading.act.ontology === ontology &&
    performatives.includes(reading.act.performative)
    ? reading.act
    : undefined;
}

function msgIdOf(input: MatchInput): string {
  return "msgId" in input.message ? input.message.msgId : "";
}
