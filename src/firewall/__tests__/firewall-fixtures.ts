/** @file Shared fixtures for firewall tests: an agent's firewall, messages, rules and a test provider. */

import { Effect, Encoding } from "effect";
import type { DecisionProvider } from "../check.js";
import {
  AgentCard,
  AgentId,
  AgentName,
  Message,
  PrincipalId,
} from "../../messages.js";
import {
  decodeMessagePolicy,
  type LoadedMessagePolicy,
  type TextRule,
} from "../rules.js";
import { makeFirewall } from "../service.js";

/** A fixed rule the test provider always finds broken. */
export const privacy = {
  id: "privacy",
  applies: "both",
  text: "Do not reveal the owner's private records.",
  feedback: "Keep private records private.",
} satisfies TextRule;
/** Finds `privacy` broken and every other rule followed, with certainty. */
export const certainProvider: DecisionProvider = {
  decide: (request) =>
    Effect.succeed({
      answers: Object.fromEntries(
        Object.entries(request.questions).map(([id, question]) => {
          const choices = Object.keys(question.criteria);
          const selected =
            question.ruleId === privacy.id ? "violation" : "compliant";
          return [
            id,
            {
              choice: selected,
              probabilities: Object.fromEntries(
                choices.map((choice) => [choice, Number(choice === selected)]),
              ),
            },
          ];
        }),
      ),
    }),
};

/** A draft from `self` to a peer that reveals a private record. */
export function outgoing(self: AgentCard) {
  return Message.create({
    author: self,
    recipient: peer("bob", 2),
    text: "The private record says the owner is away.",
  });
}

/** A peer's plain message to `self`. */
export function incoming(self: AgentCard) {
  return Message.create({
    author: peer("bob", 2),
    recipient: self,
    text: "Hello",
  });
}

/** A peer card whose ids repeat `byte`. */
export function peer(name: string, byte: number): AgentCard {
  return AgentCard.make({
    agentId: AgentId.make(
      `agt_${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`,
    ),
    principalId: PrincipalId.make(
      `prn_${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`,
    ),
    agentName: AgentName.make(name),
  });
}

/** A message policy and the provider that judges its text rules in place of Jev. */
interface TestPolicy {
  readonly source: LoadedMessagePolicy;
  readonly provider: DecisionProvider;
}

/** A message policy with the given text rules, judged by `provider`. */
export function policy(
  rules: readonly TextRule[] = [],
  provider: DecisionProvider = certainProvider,
): TestPolicy {
  return {
    provider,
    source: {
      policy: Effect.runSync(
        decodeMessagePolicy(
          JSON.stringify({
            decisionProvider: { model: "test" },
            rules,
            settings: {
              minimumProbability: 0.5,
              timeoutMilliseconds: 5_000,
            },
          }),
        ),
      ),
    },
  };
}

/** Alice, whose firewall the tests check messages with. */
export const alice = peer("alice", 1);

/** Alice's firewall under `configured`, with whatever `installation` adds. */
export function firewallOf(
  configured: TestPolicy,
  installation: Partial<Parameters<typeof makeFirewall>[0]> = {},
) {
  return makeFirewall({
    source: configured.source,
    me: alice,
    historyLimit: 256,
    provider: configured.provider,
    ...installation,
  });
}
