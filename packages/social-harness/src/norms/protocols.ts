/**
 * @file The FIPA interaction protocols as turn rules: which performatives start
 * an instance, which act each party may send next, and when the instance
 * ends. Any member may also `cancel` an instance, which ends it.
 */

import type { ProtocolName } from "./documents.js";

/** One protocol instance: who started it, who takes part, and the acts each has sent. */
export interface Instance {
  readonly norm: string;
  readonly conversationId: string;
  readonly protocol: ProtocolName;
  readonly initiator: string;
  readonly participants: readonly string[];
  readonly acts: ReadonlyArray<{
    readonly sender: string;
    readonly performative: string;
  }>;
}

const answers = {
  query: ["inform", "inform-if", "inform-ref", "refuse", "failure"],
  request: ["inform", "refuse", "failure"],
} as const;

/** The performatives that start an instance of each protocol. */
export const openingActs: Readonly<Record<ProtocolName, readonly string[]>> = {
  "fipa-propose": ["propose"],
  "fipa-query": ["query-if", "query-ref"],
  "fipa-request": ["request"],
  "fipa-contract-net": ["cfp"],
};

/**
 * What `sender` may send next. The initiator of a Contract Net decides once
 * every participant has proposed or refused; in a Query or Request, one final
 * answer serves every member.
 */
export function allowedActs(
  instance: Instance,
  sender: string,
): readonly string[] {
  const own = said(instance, sender);
  const isInitiator = sender === instance.initiator;
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
          said(instance, sender).length === 1
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

/** Whether the acts sent so far complete the instance. */
export function ended(instance: Instance): boolean {
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
      act.sender !== instance.initiator && finals.includes(act.performative),
  );
}

function everyParticipantAnswered(instance: Instance): boolean {
  return instance.participants.every(
    (participant) => said(instance, participant).length !== 0,
  );
}

function said(instance: Instance, sender: string): readonly string[] {
  return instance.acts.flatMap((act) =>
    act.sender === sender ? [act.performative] : [],
  );
}
