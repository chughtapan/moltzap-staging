/**
 * @file The norm checks a firewall rule chain names. Each reads the message as
 * an act of a norm this agent holds, and the conversation's norm state from
 * the permitted history, and reports one fact:
 *
 * - `norms.unparsed`: the message cannot be read as an act of a norm held
 *   here, including an act whose fields its norm's form refuses;
 * - `norms.nonconforming`: a readable act breaks its norm's turns, or the
 *   history it needs is missing;
 * - `norms.query`: a peer's norm-discovery query this agent answers;
 * - `norms.consent`: the agent proposes or accepts a norm, which its owner
 *   approves.
 *
 * The firewall's rules decide what each fact does to the message. Each norm
 * rule also has a matcher that matches the acts it governs, which scopes the
 * firewall text rule made from it. For a peer's act its reply is the
 * `not-understood` the daemon sends when that rule withholds the act.
 */

import { Effect, Option } from "effect";
import type { Matcher, MatchInput, MatchResult } from "../firewall/index.js";
import {
  type ResponseFieldFailures,
  responseFieldsText,
} from "../communication/index.js";
import {
  type Act,
  formatAct,
  messageText,
  type Reading,
  readText,
} from "./acts.js";
import {
  type ConversationNorms,
  foldActs,
  judge,
  normNames,
  normOf,
  type SendableAct,
  sendableActs,
  type Setting,
  type Verdict,
} from "./conversation.js";
import { type Norm, type NormRule, standsAlone } from "./documents.js";

/** What one matcher call reads, computed once for every matcher the chain asks. */
interface Analysis {
  readonly reading: Reading;
  readonly author: string;
  readonly setting: Setting;
  readonly state: ConversationNorms;
  readonly historyComplete: boolean;
  readonly verdict?: Verdict;
}

const unmatched = { matched: false } as const;

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
    (decide: (input: MatchInput, analysis: Analysis) => MatchResult) =>
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
    ...Object.fromEntries(
      norms.flatMap((norm) =>
        norm.rules.map((rule, index) => [
          ruleMatcher(norm.name, index + 1),
          { match: match(governedBy(norm.name, index + 1, rule)) },
        ]),
      ),
    ),
  };
}

/** The name of the matcher for the acts a norm's `index`th rule governs, counted from 1. */
export function ruleMatcher(normName: string, index: number): string {
  return `norms.rule.${normRuleId(normName, index)}`;
}

/** The id of a norm's `index`th English rule as a firewall text rule, counted from 1. */
export function normRuleId(normName: string, index: number): string {
  return `${normName}-rule-${index}`;
}

/**
 * Matches an act of the norm `normName` whose performative its
 * `ruleNumber`th rule, counted from 1, governs, in either direction. For a
 * peer's act the reply is `not-understood` naming the rule, which voids the
 * act for every member when the rule withholds it.
 */
function governedBy(normName: string, ruleNumber: number, rule: NormRule) {
  return (input: MatchInput, analysis: Analysis): MatchResult => {
    const { reading } = analysis;
    if (
      reading.kind !== "act" ||
      reading.act.ontology !== normName ||
      !rule.acts.includes(reading.act.performative)
    ) {
      return unmatched;
    }
    const reply = notUnderstood(
      input,
      analysis,
      "rule-violation",
      `${normRuleId(normName, ruleNumber)}: ${rule.text}`,
    );
    return { matched: true, ...(reply === undefined ? {} : { reply }) };
  };
}

/** Reads the message and folds the acts in the history before it. */
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
      const earlierText = messageText(message);
      const reading =
        earlierText === undefined ? undefined : readText(earlierText);
      return reading?.kind === "act"
        ? [
            {
              act: reading.act,
              author: message.author.agentId,
              msgId: message.msgId,
            },
          ]
        : [];
    });
    const state = yield* foldActs(setting, acts);
    const reading = readText(text);
    const verdict =
      reading.kind === "act" &&
      normOf(setting, state, reading.act.ontology) !== undefined
        ? yield* judge(setting, state, {
            act: reading.act,
            author: author.agentId,
            msgId: msgIdOf(input),
          })
        : undefined;
    return Option.some<Analysis>({
      reading,
      author: author.agentId,
      setting,
      state,
      historyComplete:
        !input.evidence.earlierHistoryOmitted &&
        input.evidence.gaps.length === 0,
      ...(verdict === undefined ? {} : { verdict }),
    });
  });
}

/**
 * Text that cannot be read as an act of a norm held here, including an act
 * whose fields its form refuses. A peer hears `not-understood` with what failed,
 * unless it sent one.
 */
function unparsed(input: MatchInput, analysis: Analysis): MatchResult {
  const failure = unreadable(analysis);
  if (failure === undefined) {
    return unmatched;
  }
  const accepts = [...analysis.setting.held.keys()].join(" ");
  const reply = notUnderstood(input, analysis, failure.reason, failure.error);
  return {
    matched: true,
    detail: { ...failure, accepts },
    ...(reply === undefined ? {} : { reply }),
  };
}

/**
 * A readable act that breaks its norm's turns or agreement. The detail names
 * the reason and the acts its author may send now, and in a direct
 * conversation the peer hears the same in `not-understood`; in a group every
 * other member's daemon would send the same answer, so none does. Without the full history before it, an act that
 * depends on that history cannot be judged and is held for missing evidence.
 */
function nonconforming(input: MatchInput, analysis: Analysis): MatchResult {
  const { reading, verdict, state, setting } = analysis;
  if (
    reading.kind !== "act" ||
    verdict === undefined ||
    unreadable(analysis) !== undefined
  ) {
    return unmatched;
  }
  const { act } = reading;
  const actNorm = normOf(setting, state, act.ontology);
  if (
    !analysis.historyComplete &&
    (actNorm === undefined || !standsAlone(actNorm))
  ) {
    return {
      matched: true,
      refusal: "unresolved-evidence",
      detail: { reason: "history-unavailable", norm: act.ontology },
    };
  }
  if (verdict.kind === "breaks") {
    const hint = sendableActsHint(analysis);
    const reply =
      setting.members.length === 2
        ? notUnderstood(input, analysis, verdict.breach.reason, hint.error)
        : undefined;
    return {
      matched: true,
      detail: { ...verdict.breach, ...hint },
      ...(reply === undefined ? {} : { reply }),
    };
  }
  const accepting =
    input.direction === "outgoing"
      ? actUnder(analysis, normNames.agreement, ["accept-proposal"])
      : undefined;
  const proposedNormName = state.agreement?.normName;
  if (
    accepting !== undefined &&
    proposedNormName !== undefined &&
    normOf(setting, state, proposedNormName) === undefined
  ) {
    return {
      matched: true,
      detail: { reason: "norm-unknown", norm: proposedNormName },
    };
  }
  return unmatched;
}

/**
 * Why a message cannot be read as an act of a norm held here, if it cannot.
 * Plain text and an act of an unknown norm also carry the acts the author may
 * send now, so that agent can continue in a norm this member reads.
 */
function unreadable(analysis: Analysis):
  | {
      readonly reason: string;
      readonly norm?: string;
      readonly error?: string;
      readonly fields?: ResponseFieldFailures;
    }
  | undefined {
  const { reading, setting, state, verdict } = analysis;
  if (reading.kind === "invalid") {
    return { reason: "unparsed", error: reading.error };
  }
  if (reading.kind === "text") {
    return setting.held.has(normNames.chat)
      ? undefined
      : { reason: "plain-text", ...sendableActsHint(analysis) };
  }
  if (normOf(setting, state, reading.act.ontology) === undefined) {
    return { reason: "unknown-norm", ...sendableActsHint(analysis) };
  }
  const breach = verdict?.kind === "breaks" ? verdict.breach : undefined;
  if (breach?.reason !== "invalid-content") {
    return undefined;
  }
  return {
    reason: "invalid-content",
    norm: breach.norm,
    ...(breach.fields === undefined
      ? {}
      : { error: responseFieldsText(breach.fields), fields: breach.fields }),
  };
}

/** The acts the message's author may send now, as `:error` text, when there are any. */
function sendableActsHint({ author, setting, state }: Analysis): {
  readonly error?: string;
} {
  const acts = sendableActs(setting, state, author).map((act) => skeleton(act));
  return acts.length === 0
    ? {}
    : { error: `acts you may send now: ${acts.join(", ")}` };
}

/** An act's performative, its norm and the norm it names, written as the act's opening. */
function skeleton({
  performative,
  ontology,
  namedNormName,
}: SendableAct): string {
  const normParameter =
    namedNormName === undefined ? "" : ` :norm ${namedNormName}`;
  return `(${performative} :ontology ${ontology}${normParameter})`;
}

/**
 * The `not-understood` this agent's daemon sends back for a peer's message,
 * or nothing for its own send, without `feedback`, or about feedback.
 */
function notUnderstood(
  input: MatchInput,
  { reading, setting }: Analysis,
  reason: string,
  error?: string,
): string | undefined {
  const answerable =
    input.direction === "incoming" &&
    setting.held.has(normNames.feedback) &&
    !(reading.kind === "act" && reading.act.performative === "not-understood");
  return answerable
    ? formatAct({
        performative: "not-understood",
        ontology: normNames.feedback,
        inReplyTo: msgIdOf(input),
        fields: {
          reason,
          ...(error === undefined
            ? {}
            : { error: error.slice(0, errorLimit(setting)) }),
          accepts: [...setting.held.keys()].join(" "),
        },
      })
    : undefined;
}

/** The longest `:error` the held feedback norm's `not-understood` form accepts. */
function errorLimit(setting: Setting): number {
  const field = setting.held.get(normNames.feedback)?.forms["not-understood"]
    ?.properties.error;
  return field !== undefined &&
    "maxLength" in field &&
    field.maxLength !== undefined
    ? field.maxLength
    : Number.POSITIVE_INFINITY;
}

/**
 * A peer's norm-discovery query this agent answers: in a direct conversation,
 * any query; in a group, a query for the norm this agent proposed. The answer
 * names the norms held here, or carries the requested norm's document.
 */
function answerQuery(input: MatchInput, analysis: Analysis): MatchResult {
  const { verdict, setting, state } = analysis;
  const act =
    input.direction === "incoming"
      ? actUnder(analysis, normNames.discovery, ["query-ref"])
      : undefined;
  if (act === undefined || verdict?.kind !== "conforms") {
    return unmatched;
  }
  const requestedNormName = act.fields.norm;
  const direct = setting.members.length === 2;
  const proposer =
    state.agreement?.instance.initiator === input.self.agentId &&
    state.agreement.normName === requestedNormName;
  if (!direct && !proposer) {
    return unmatched;
  }
  if (typeof requestedNormName !== "string") {
    return direct
      ? answer(msgIdOf(input), { norms: [...setting.held.keys()].join(" ") })
      : unmatched;
  }
  const norm = setting.held.get(requestedNormName);
  return norm === undefined
    ? unmatched
    : answer(msgIdOf(input), { norm: requestedNormName }, norm.document);
}

/** An `inform` that answers the query message whose id is `queryMsgId`. */
function answer(
  queryMsgId: string,
  fields: Readonly<Record<string, string>>,
  content?: string,
): MatchResult {
  return {
    matched: true,
    reply: formatAct({
      performative: "inform",
      ontology: normNames.discovery,
      inReplyTo: queryMsgId,
      ...(content === undefined ? {} : { content }),
      fields,
    }),
  };
}

/** This agent proposing or accepting a norm, which its owner approves. */
function consent(input: MatchInput, analysis: Analysis): MatchResult {
  const act =
    input.direction === "outgoing"
      ? actUnder(analysis, normNames.agreement, ["propose", "accept-proposal"])
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
