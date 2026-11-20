/** @file A message check maps each rule's answer to the decision, feedback and guidance its direction gives. */

import { effect as it } from "@effect/vitest";
import { Effect, FastCheck as fc, Fiber, TestClock } from "effect";
import { expect } from "vitest";
import type { ProviderRequestSettings, TextRule } from "./rules.js";
import {
  type AssessmentText,
  checkCapturedMessage,
  type CheckEvidence,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
} from "./check.js";

const repair =
  "Ask for acceptance of the chosen proposal. Do not claim agreement yet.";
const registeredGuidance =
  "After proposing a time, wait for every required participant to accept that proposal.";
const draft: AssessmentText = {
  id: "draft",
  sender: "organizer-agent",
  recipients: ["participant-agent"],
  text: "The meeting is confirmed; everyone agreed.",
};
const evidence: CheckEvidence = {
  agentId: "organizer-agent",
  principal: "organizer",
  participants: [{ agentId: "participant-agent", principal: "participant" }],
  history: [
    {
      id: "availability",
      sender: "participant-agent",
      recipients: ["organizer-agent"],
      text: "I am free Tuesday.",
      visibleToAgent: true,
    },
  ],
  gaps: [],
};
const proposalAcceptance: TextRule = {
  id: "proposal-acceptance",
  applies: "both",
  text: "Availability is not acceptance. Everyone must accept the same concrete proposal before it is finalized.",
  guidance: registeredGuidance,
  feedback: repair,
};
const rules: readonly TextRule[] = [
  {
    id: "privacy",
    applies: "both",
    text: "Do not disclose unrelated private appointments.",
  },
  proposalAcceptance,
];
/** One answer exactly as a provider reports it. */
type Answer = DecisionResponse["answers"][string];
const providerSettings: typeof ProviderRequestSettings.Encoded = {
  minimumProbability: 0.5,
  timeoutMilliseconds: 100,
};
/**
 * A provider's answers: the question for each rule keyed by its id in
 * `choices` takes that choice with certainty, or the full answer given there,
 * and every other rule is compliant with certainty.
 */
function responseFor(
  request: DecisionRequest,
  choices: Readonly<Record<string, string | Answer>> = {},
): typeof DecisionResponse.Encoded {
  return {
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => {
        const selected = choices[question.ruleId] ?? "compliant";
        if (typeof selected !== "string") {
          return [id, selected];
        }
        return [
          id,
          {
            choice: selected,
            probabilities: Object.fromEntries(
              Object.keys(question.criteria).map((choice) => [
                choice,
                choice === selected ? 1 : 0,
              ]),
            ),
          },
        ];
      }),
    ),
  };
}
function provider(
  choices: Readonly<Record<string, string | Answer>> = {},
  requests: DecisionRequest[] = [],
): DecisionProvider {
  return {
    decide: (request) =>
      Effect.sync(() => {
        requests.push(request);
        return responseFor(request, choices);
      }),
  };
}
const defaultProvider = provider();
/** A peer's message to the local agent. */
const peerMessage: AssessmentText = {
  id: "proposal",
  sender: "participant-agent",
  recipients: [evidence.agentId],
  text: "Can we meet Tuesday?",
};
/** Checks one message: the local agent's draft, or a peer's message to it when `incoming`. */
function check(
  decisionProvider: DecisionProvider = defaultProvider,
  options: {
    readonly evidence?: CheckEvidence;
    readonly rules?: readonly TextRule[];
    readonly providerSettings?: ProviderRequestSettings;
    readonly incoming?: boolean;
  } = {},
) {
  return checkCapturedMessage(
    options.incoming ? "incoming" : "outgoing",
    options.incoming ? peerMessage : draft,
    options.evidence ?? evidence,
    {
      textRules: options.rules ?? rules,
      revision: "a".repeat(64),
      settings: options.providerSettings ?? providerSettings,
      provider: decisionProvider,
    },
  );
}
type Choice = "compliant" | "violation";
const choices = fc.constantFrom<Choice>("compliant", "violation");
/** The other answer to the question asked of every rule. */
const rival = { compliant: "violation", violation: "compliant" } as const;
/** An answer that gives its choice this probability and the other option the rest. */
function answer(choice: Choice, probability: number): Answer {
  return {
    choice,
    probabilities: { [choice]: probability, [rival[choice]]: 1 - probability },
  };
}
/** A probability from an even split to certainty, in whole percents so that two of them compare exactly. */
const percent = fc.integer({ min: 50, max: 100 }).map((whole) => whole / 100);
/** Two such probabilities, the lower first. */
const ascending = fc
  .tuple(percent, percent)
  .map(([one, other]) => [Math.min(one, other), Math.max(one, other)] as const);
it.prop(
  "follows an answer whose choice has at least the minimum probability, in either direction: allows a compliant message with no guidance and rejects a violation with its rule's feedback",
  [fc.boolean(), choices, ascending],
  ([incoming, choice, [minimumProbability, probability]]) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({ [proposalAcceptance.id]: answer(choice, probability) }),
        {
          incoming,
          providerSettings: { ...providerSettings, minimumProbability },
        },
      );
      expect(result).toMatchObject(
        choice === "compliant"
          ? {
              reason: "eligible",
              feedback: [],
              instructions: [],
            }
          : {
              reason: "rule-violation",
              feedback: [repair],
              instructions: [],
            },
      );
    }),
);
it.prop(
  "treats a choice below the minimum probability as unsure: holds an outgoing draft with its rule's feedback, and allows an incoming message with its rule's guidance and the provider's choice and probability",
  [
    fc.boolean(),
    choices,
    ascending.filter(([lower, higher]) => lower < higher),
  ],
  ([incoming, choice, [probability, minimumProbability]]) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({ [proposalAcceptance.id]: answer(choice, probability) }),
        {
          incoming,
          providerSettings: { ...providerSettings, minimumProbability },
        },
      );
      expect(result).toMatchObject(
        incoming
          ? {
              reason: "eligible",
              feedback: [],
              instructions: [
                {
                  ruleId: proposalAcceptance.id,
                  instruction: registeredGuidance,
                  uncertain: { choice, probability },
                },
              ],
            }
          : {
              reason: "unresolved-evidence",
              feedback: [repair],
              instructions: [],
            },
      );
    }),
);
it.each<Choice>(["compliant", "violation"])(
  "holds an outgoing draft as unresolved evidence when its %s choice meets the minimum probability but trails the other option",
  (choice) =>
    Effect.gen(function* () {
      const probabilities = { [choice]: 0.5, [rival[choice]]: 0.51 };
      const result = yield* check(
        provider({ [proposalAcceptance.id]: { choice, probabilities } }),
      );
      expect(result).toMatchObject({
        reason: "unresolved-evidence",
        feedback: [repair],
      });
    }),
);
it.each([
  {
    registered: "guidance",
    advice: { guidance: registeredGuidance },
    text: registeredGuidance,
  },
  {
    registered: "feedback",
    advice: { feedback: repair },
    text: repair,
  },
])(
  "advises on an unsure answer from a rule that registers only $registered with that text, as the feedback of a held draft and the guidance of an allowed message",
  ({ advice, text }) =>
    Effect.gen(function* () {
      const rule: TextRule = {
        id: proposalAcceptance.id,
        applies: "both",
        text: proposalAcceptance.text,
        ...advice,
      };
      const unsure = provider({ [rule.id]: answer("compliant", 0.6) });
      const options = {
        rules: [rule],
        providerSettings: { ...providerSettings, minimumProbability: 0.8 },
      };
      const held = yield* check(unsure, options);
      const allowed = yield* check(unsure, { ...options, incoming: true });
      expect(held.feedback).toEqual([text]);
      expect(allowed.instructions).toMatchObject([{ instruction: text }]);
    }),
);
it.prop(
  "rejects a message that violates one rule whether the provider is sure or unsure of another rule, and whichever way it leans",
  [fc.boolean(), choices, percent],
  ([incoming, choice, probability]) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({
          privacy: "violation",
          [proposalAcceptance.id]: answer(choice, probability),
        }),
        {
          incoming,
          providerSettings: { ...providerSettings, minimumProbability: 0.8 },
        },
      );
      expect(result).toMatchObject({
        reason: "rule-violation",
        instructions: [],
      });
    }),
);
it.each([
  {
    failure: "fails",
    failing: {
      decide: () =>
        Effect.fail(new DecisionProviderError({ transient: false })),
    },
  },
  {
    failure: "leaves a question unanswered",
    failing: { decide: () => Effect.succeed({ answers: {} }) },
  },
  {
    failure: "chooses an answer the question does not offer",
    failing: provider({ [proposalAcceptance.id]: "allow-everything" }),
  },
] satisfies ReadonlyArray<{ failure: string; failing: DecisionProvider }>)(
  "holds a message in either direction as an unavailable check, with no feedback or guidance, when the provider $failure",
  ({ failing }) =>
    Effect.gen(function* () {
      for (const incoming of [true, false]) {
        expect(yield* check(failing, { incoming })).toMatchObject({
          reason: "check-unavailable",
          feedback: [],
          instructions: [],
        });
      }
    }),
);
const applicability = fc.constantFrom<TextRule["applies"]>(
  "incoming",
  "outgoing",
  "both",
);
it.prop(
  "asks the provider once about exactly the rules that apply in the message's direction, stating each rule's text once and none of its feedback or guidance",
  [fc.boolean(), fc.array(applicability, { maxLength: 8 })],
  ([incoming, directions]) =>
    Effect.gen(function* () {
      const textRules = directions.map(
        (applies, index): TextRule => ({
          id: `rule-${index}`,
          applies,
          text: `Address open request number ${index} before changing the subject.`,
          guidance: registeredGuidance,
          feedback: repair,
        }),
      );
      const asked = textRules.filter(
        (rule) => rule.applies !== (incoming ? "outgoing" : "incoming"),
      );
      const requests: DecisionRequest[] = [];
      const result = yield* check(provider({}, requests), {
        incoming,
        rules: textRules,
      });
      const stated = requests.map((request) => JSON.stringify(request)).join();
      expect(result.reason).toBe("eligible");
      expect(
        requests.map((request) =>
          Object.values(request.questions).map((question) => question.ruleId),
        ),
      ).toEqual(asked.length === 0 ? [] : [asked.map((rule) => rule.id)]);
      expect(result.checkedRuleIds).toEqual(asked.map((rule) => rule.id));
      for (const rule of textRules) {
        expect(stated.split(rule.text)).toHaveLength(
          asked.includes(rule) ? 2 : 1,
        );
      }
      expect(stated).not.toContain(registeredGuidance);
      expect(stated).not.toContain(repair);
    }),
);
it.prop(
  "with no rules, allows a message in either direction exactly when its evidence states no gap, and holds it as unresolved evidence otherwise",
  [fc.boolean(), fc.array(fc.string({ minLength: 1 }), { maxLength: 2 })],
  ([incoming, gaps]) =>
    Effect.gen(function* () {
      const result = yield* check(defaultProvider, {
        rules: [],
        incoming,
        evidence: { ...evidence, gaps },
      });
      expect(result).toMatchObject(
        gaps.length === 0
          ? { reason: "eligible" }
          : { reason: "unresolved-evidence" },
      );
    }),
);
it.prop(
  "holds a message exactly when the provider answers after the request deadline",
  [
    fc
      .tuple(fc.integer({ min: 1, max: 1000 }), fc.nat({ max: 1000 }))
      .filter(([perRequest, delay]) => delay !== perRequest),
  ],
  ([[perRequest, delay]]) =>
    Effect.gen(function* () {
      const slow: DecisionProvider = {
        decide: (request) =>
          Effect.sleep(delay).pipe(Effect.as(responseFor(request))),
      };
      const running = yield* check(slow, {
        providerSettings: {
          ...providerSettings,
          timeoutMilliseconds: perRequest,
        },
      }).pipe(Effect.fork);
      yield* TestClock.adjust(delay);
      const result = yield* Fiber.join(running);
      expect(result).toMatchObject(
        delay < perRequest
          ? { reason: "eligible", feedback: [] }
          : { reason: "check-unavailable", feedback: [] },
      );
    }),
);
