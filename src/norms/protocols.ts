/**
 * @file Protocols: which performatives start an instance, which act each
 * member may send next, and when the instance ends, for the FIPA interaction
 * protocols and for a norm's own turns. Under a norm's own turns each act
 * starts and ends facts, the facts that hold decide which acts may follow,
 * and the instance ends when no fact holds. Any member may also `cancel` an
 * instance, which ends it.
 */

import type { ProtocolName, Turn, Turns } from "./documents.js";

/** A norm's turn rules, which an instance follows: a FIPA protocol, or the norm's own turns. */
export type Protocol = ProtocolName | Turns;

/** One instance of a norm's protocol: who started it, who takes part, and the acts each has sent. */
export interface Instance {
  readonly normName: string;
  readonly protocol: Protocol;
  readonly initiator: string;
  readonly participants: readonly string[];
  readonly acts: ReadonlyArray<{
    readonly author: string;
    readonly performative: string;
  }>;
}

const answers = {
  query: ["inform", "inform-if", "inform-ref", "refuse", "failure"],
  request: ["inform", "refuse", "failure"],
} as const;

/** The performatives that start an instance of each FIPA protocol. */
const fipaOpeningActs: Readonly<Record<ProtocolName, readonly string[]>> = {
  "fipa-propose": ["propose"],
  "fipa-query": ["query-if", "query-ref"],
  "fipa-request": ["request"],
  "fipa-contract-net": ["cfp"],
};

/**
 * The performatives that start an instance: a FIPA protocol's, or under a
 * norm's own turns the acts that its initiator sends with no `:when` facts.
 */
export function openingActs(protocol: Protocol): readonly string[] {
  return typeof protocol === "string"
    ? fipaOpeningActs[protocol]
    : Object.entries(protocol).flatMap(([performative, turn]) =>
        turn.by === "initiator" && turn.when.length === 0 ? [performative] : [],
      );
}

/**
 * What `member` may send next. Under a norm's own turns, an act is allowed
 * when its role is the member's, its `when` facts hold, its `unless` facts do
 * not, and it has not reached its `max`. The initiator of a Contract Net
 * decides once every participant has proposed or refused; in a Query or
 * Request, one final answer serves every member.
 */
export function allowedActs(
  instance: Instance,
  member: string,
): readonly string[] {
  const own = said(instance, member);
  const isInitiator = member === instance.initiator;
  if (typeof instance.protocol !== "string") {
    const facts = holding(instance.protocol, instance.acts);
    const role = isInitiator ? "initiator" : "participant";
    return Object.entries(instance.protocol).flatMap(([performative, turn]) =>
      turn.by === role &&
      enabled(turn, facts) &&
      belowMax(instance, performative, turn.max)
        ? [performative]
        : [],
    );
  }
  switch (instance.protocol) {
    case "fipa-propose":
      return isInitiator || own.length !== 0
        ? []
        : ["accept-proposal", "reject-proposal"];
    case "fipa-query":
      return participantTurn(isInitiator, own, answers.query);
    case "fipa-request":
      return participantTurn(isInitiator, own, answers.request);
    case "fipa-contract-net":
      if (isInitiator) {
        return everyParticipantAnswered(instance) &&
          said(instance, member).length === 1
          ? ["accept-proposal", "reject-proposal"]
          : [];
      }
      return own.length === 0 ? ["propose", "refuse"] : [];
    default: {
      const exhaustive: never = instance.protocol;
      return exhaustive;
    }
  }
}

/**
 * Whether the acts sent so far complete the instance. Under a norm's own
 * turns, it ends when no fact holds.
 */
export function ended(instance: Instance): boolean {
  if (typeof instance.protocol !== "string") {
    return holding(instance.protocol, instance.acts).size === 0;
  }
  switch (instance.protocol) {
    case "fipa-propose":
      return everyParticipantAnswered(instance);
    case "fipa-query":
      return finalAnswerSent(instance, answers.query);
    case "fipa-request":
      return finalAnswerSent(instance, answers.request);
    case "fipa-contract-net":
      return said(instance, instance.initiator).length > 1;
    default: {
      const exhaustive: never = instance.protocol;
      return exhaustive;
    }
  }
}

/**
 * The facts that hold after `acts`: each act ends its `terminates` facts,
 * then starts its `initiates` facts.
 */
function holding(turns: Turns, acts: Instance["acts"]): ReadonlySet<string> {
  return acts.reduce<ReadonlySet<string>>((facts, { performative }) => {
    const turn = turns[performative];
    return turn === undefined
      ? facts
      : new Set([
          ...[...facts].filter((fact) => !turn.terminates.includes(fact)),
          ...turn.initiates,
        ]);
  }, new Set());
}

/** Whether a turn's `when` facts hold and its `unless` facts do not. */
function enabled(turn: Turn, facts: ReadonlySet<string>): boolean {
  return (
    turn.when.every((fact) => facts.has(fact)) &&
    !turn.unless.some((fact) => facts.has(fact))
  );
}

/** A participant may agree first, then sends one final answer. */
function participantTurn(
  isInitiator: boolean,
  own: readonly string[],
  finals: readonly string[],
): readonly string[] {
  if (isInitiator || own.some((act) => finals.includes(act))) {
    return [];
  }
  return own.includes("agree") ? finals : ["agree", ...finals];
}

function finalAnswerSent(
  instance: Instance,
  finals: readonly string[],
): boolean {
  return instance.acts.some(
    (act) =>
      act.author !== instance.initiator && finals.includes(act.performative),
  );
}

function everyParticipantAnswered(instance: Instance): boolean {
  return instance.participants.every(
    (participant) => said(instance, participant).length !== 0,
  );
}

/** Whether `performative` has occurred fewer than `max` times in the instance. */
function belowMax(
  instance: Instance,
  performative: string,
  max?: number,
): boolean {
  return (
    max === undefined ||
    instance.acts.filter((act) => act.performative === performative).length <
      max
  );
}

function said(instance: Instance, author: string): readonly string[] {
  return instance.acts.flatMap((act) =>
    act.author === author ? [act.performative] : [],
  );
}
