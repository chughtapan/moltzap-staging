/** @file Fixtures shared by the tests of the firewall and of what sits on it: an agent's card, a message policy with its provider, and a provider's answers. */

import { Encoding } from "effect";
import type {
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
  LoadedMessagePolicy,
} from "../firewall.js";
import { AgentCard, AgentId, AgentName, PrincipalId } from "../messages.js";

/** A card whose ids repeat `byte`. */
export function peer(name: string, byte: number): AgentCard {
  const id = Encoding.encodeBase64Url(new Uint8Array(16).fill(byte));
  return AgentCard.make({
    agentId: AgentId.make(`agt_${id}`),
    principalId: PrincipalId.make(`prn_${id}`),
    agentName: AgentName.make(name),
  });
}

/** A message policy and the provider that judges its text rules in place of Jev. */
export interface TestPolicy {
  readonly source: LoadedMessagePolicy;
  readonly provider: DecisionProvider;
}

/** One answer exactly as a provider reports it. */
export type Answer = DecisionResponse["answers"][string];

/**
 * A provider's answer to each question of `request`: the choice `choices`
 * gives the question's rule, with certainty unless it is a full answer, and
 * `otherwise` for every other rule.
 */
export function answersTo(
  request: DecisionRequest,
  choices: Readonly<Record<string, string | Answer>> = {},
  otherwise = "compliant",
): DecisionResponse {
  return {
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => {
        const selected = choices[question.ruleId] ?? otherwise;
        return [
          id,
          typeof selected === "string"
            ? {
                choice: selected,
                probabilities: Object.fromEntries(
                  Object.keys(question.criteria).map((choice) => [
                    choice,
                    Number(choice === selected),
                  ]),
                ),
              }
            : selected,
        ];
      }),
    ),
  };
}
