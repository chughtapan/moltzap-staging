/** @file Message checks enforce configured limits and release only the feedback and guidance their evidence permits. */

import { effect as it } from "@effect/vitest";
import {
  Cause,
  Effect,
  Exit,
  FastCheck as fc,
  Fiber,
  Schema,
  TestClock,
} from "effect";
import { expect } from "vitest";
import type {
  CheckLimits,
  ProviderRequestSettings,
  TextRule,
} from "./rules.js";
import {
  type AssessmentText,
  checkCapturedMessage,
  type CheckEvidence,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
  validateChecks,
} from "./check.js";

const privateEvidence = "PRIVATE: unrelated medical appointment at 14:00";
const repair =
  "Ask for acceptance of the chosen proposal. Do not claim agreement yet.";
const registeredGuidance =
  "After proposing a time, wait for every required participant to accept that proposal.";
const draft: typeof AssessmentText.Encoded = {
  id: "draft",
  sender: "organizer-agent",
  recipients: ["participant-agent"],
  text: "The meeting is confirmed; everyone agreed.",
};
const evidence: typeof CheckEvidence.Encoded = {
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
  facts: [
    {
      id: "private-calendar",
      source: "owner-calendar",
      text: privateEvidence,
      visibility: "assessor",
    },
  ],
  gaps: [],
};
const proposalAcceptance: TextRule = {
  id: "proposal-acceptance",
  applies: "both",
  text: "Availability is not acceptance. Everyone must accept the same concrete proposal before it is finalized.",
  guidance: registeredGuidance,
  feedback: { text: repair },
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
  maximumRequestBytes: 60000,
  timeoutMilliseconds: 100,
};
const checkLimits = {
  timeoutMilliseconds: 2000,
  maximumInputBytes: 300000,
  maximumResultBytes: 1000000,
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
    provider: "test-decisions",
    model: "test-model",
    raw: privateEvidence,
    usage: {},
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
const peerMessage: typeof AssessmentText.Encoded = {
  id: "proposal",
  sender: "participant-agent",
  recipients: [evidence.agentId],
  text: "Can we meet Tuesday?",
};
/**
 * Validates the rules, then checks one message: the local agent's draft, or a
 * peer's message to it when `incoming`, unless `message` gives another.
 */
function check(
  decisionProvider: DecisionProvider = defaultProvider,
  options: {
    readonly message?: AssessmentText;
    readonly evidence?: CheckEvidence;
    readonly rules?: readonly TextRule[];
    readonly providerSettings?: ProviderRequestSettings;
    readonly limits?: CheckLimits;
    readonly incoming?: boolean;
  } = {},
) {
  return Effect.gen(function* () {
    const configuration = {
      textRules: options.rules ?? rules,
      limits: options.limits ?? checkLimits,
      providerSettings: options.providerSettings ?? providerSettings,
      revision: "a".repeat(64),
    };
    yield* validateChecks(configuration);
    return yield* checkCapturedMessage(
      { ...configuration, provider: decisionProvider },
      {
        event: {
          direction: options.incoming ? "incoming" : "outgoing",
          message: options.message ?? (options.incoming ? peerMessage : draft),
        },
        evidence: options.evidence ?? evidence,
      },
    );
  });
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
              judgment: "allow",
              reason: "eligible",
              feedback: [],
              instructions: [],
            }
          : {
              judgment: "reject",
              reason: "rule-violation",
              feedback: [{ text: repair }],
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
              judgment: "allow",
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
              judgment: "hold",
              reason: "unresolved-evidence",
              feedback: [{ text: repair }],
              instructions: [],
            },
      );
    }),
);
it.each<Choice>(["compliant", "violation"])(
  "holds an outgoing draft as unresolved evidence when its %s choice meets the minimum probability but trails the other option within the probability tolerance",
  (choice) =>
    Effect.gen(function* () {
      const probabilities = { [choice]: 0.5, [rival[choice]]: 0.51 };
      const result = yield* check(
        provider({ [proposalAcceptance.id]: { choice, probabilities } }),
      );
      expect(result).toMatchObject({
        reason: "unresolved-evidence",
        feedback: [{ text: repair }],
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
    advice: { feedback: { text: repair } },
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
      expect(held.feedback).toEqual([{ text }]);
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
        judgment: "reject",
        reason: "rule-violation",
        instructions: [],
      });
    }),
);
/**
 * Answers the policy cannot read, each breaking one requirement of a usable
 * answer: a choice the question does not offer, an option left out, an option
 * added, probabilities that sum to more than one beyond the 0.02 tolerance, a
 * choice further than the tolerance below the other option, and a probability
 * outside 0 to 1.
 */
const unusableAnswer: fc.Arbitrary<Answer> = fc.oneof(
  fc.tuple(choices, percent).map(([choice, probability]) => ({
    ...answer(choice, probability),
    choice: "allow-everything",
  })),
  choices.map((choice) => ({ choice, probabilities: { [choice]: 1 } })),
  fc.tuple(choices, percent).map(([choice, probability]) => ({
    choice,
    probabilities: { ...answer(choice, probability).probabilities, abstain: 0 },
  })),
  fc
    .tuple(choices, fc.integer({ min: 52, max: 100 }))
    .map(([choice, each]) => ({
      choice,
      probabilities: { compliant: each / 100, violation: each / 100 },
    })),
  fc
    .tuple(choices, fc.integer({ min: 0, max: 48 }))
    .map(([choice, whole]) => answer(choice, whole / 100)),
  fc
    .tuple(choices, fc.integer({ min: 1, max: 50 }))
    .map(([choice, over]) => answer(choice, 1 + over / 100)),
);
it.prop(
  "holds a message in either direction as an unavailable check, with no guidance, when the provider gives an unusable answer",
  [fc.boolean(), unusableAnswer],
  ([incoming, unusable]) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({ [proposalAcceptance.id]: unusable }),
        { incoming },
      );
      expect(result).toMatchObject({
        judgment: "hold",
        reason: "check-unavailable",
        instructions: [],
      });
    }),
);
it.prop(
  "retains any distribution the provider reports, unrounded, in the check's trace",
  [fc.double({ min: 0.5, max: 1, noNaN: true })],
  ([compliant]) =>
    Effect.gen(function* () {
      const probabilities = { compliant, violation: 1 - compliant };
      const result = yield* check(
        provider({ privacy: { choice: "compliant", probabilities } }),
      );
      expect(result.trace?.data).toContain(JSON.stringify(probabilities));
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
          feedback: { text: repair },
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
      expect(result.judgment).toBe("allow");
      expect(
        requests.map((request) =>
          Object.values(request.questions).map((question) => question.ruleId),
        ),
      ).toEqual(asked.length === 0 ? [] : [asked.map((rule) => rule.id)]);
      expect(
        result.findings.flatMap((finding) =>
          finding.kind === "not-applicable" ? [finding.ruleId] : [],
        ),
      ).toEqual(
        textRules.flatMap((rule) => (asked.includes(rule) ? [] : [rule.id])),
      );
      for (const rule of textRules) {
        expect(stated.split(rule.text)).toHaveLength(
          asked.includes(rule) ? 2 : 1,
        );
      }
      expect(stated).not.toContain(registeredGuidance);
      expect(stated).not.toContain(repair);
    }),
);
/** The parts of a request's state that say what is checked, when it was captured and whether earlier history was left out. */
const statedTimeAndCandidate = Schema.parseJson(
  Schema.Struct({
    candidate: Schema.Unknown,
    now: Schema.optional(Schema.Number),
    earlierHistoryOmitted: Schema.optional(Schema.Boolean),
  }),
);
it.prop(
  "gives the provider the candidate, its collective operation, the capture time and any omission of earlier history as captured, and holds nothing for an omission",
  [
    fc.string(),
    fc.constantFrom<AssessmentText["operation"]>(
      undefined,
      {
        kind: "request",
        op: "gather",
        requestId: "msg_question",
        deadlineAt: 2000,
      },
      { kind: "answer", op: "all_gather", requestId: "msg_question" },
    ),
    fc.option(fc.nat(), { nil: undefined }),
    fc.option(fc.boolean(), { nil: undefined }),
  ],
  ([text, operation, now, earlierHistoryOmitted]) =>
    Effect.gen(function* () {
      const requests: DecisionRequest[] = [];
      const message = { ...draft, text, operation };
      const result = yield* check(provider({}, requests), {
        message,
        evidence: { ...evidence, now, earlierHistoryOmitted },
      });
      expect(result.reason).toBe("eligible");
      expect(
        requests.map((request) =>
          Schema.decodeUnknownSync(statedTimeAndCandidate)(request.state),
        ),
      ).toEqual([{ candidate: message, now, earlierHistoryOmitted }]);
    }),
);
/** Agents named beside the one a case is about. None is the local agent. */
const otherAgents = fc.array(fc.uuid(), { maxLength: 2 });
it.prop(
  "refuses an outgoing draft its agent did not send and an incoming message that does not name its agent as a recipient",
  [fc.boolean(), fc.uuid(), otherAgents],
  ([incoming, stranger, others]) =>
    Effect.gen(function* () {
      const refused = yield* check(defaultProvider, {
        incoming,
        message: incoming
          ? { ...peerMessage, recipients: [stranger, ...others] }
          : { ...draft, sender: stranger },
      }).pipe(Effect.flip);
      expect(refused.reason).toBe("message");
    }),
);
it.prop(
  "refuses a message in either direction that names a recipient twice",
  [fc.boolean(), fc.uuid(), otherAgents],
  ([incoming, repeated, others]) =>
    Effect.gen(function* () {
      const refused = yield* check(defaultProvider, {
        incoming,
        message: incoming
          ? {
              ...peerMessage,
              recipients: [evidence.agentId, repeated, ...others, repeated],
            }
          : { ...draft, recipients: [repeated, ...others, repeated] },
      }).pipe(Effect.flip);
      expect(refused.reason).toBe("message");
    }),
);
/** The references of a candidate, two earlier messages and two owner facts. */
const references = ["candidate", "earlier-1", "earlier-2", "fact-1", "fact-2"];
it.prop(
  "refuses evidence in which any two of the candidate, the earlier messages and the owner facts share a reference",
  [
    fc.boolean(),
    fc.shuffledSubarray(references, { minLength: 2, maxLength: 2 }),
    fc.uuid(),
  ],
  ([incoming, sharing, shared]) =>
    Effect.gen(function* () {
      const held = (reference: string) =>
        sharing.includes(reference) ? shared : reference;
      const refused = yield* check(defaultProvider, {
        incoming,
        message: {
          ...(incoming ? peerMessage : draft),
          id: held("candidate"),
        },
        evidence: {
          ...evidence,
          history: ["earlier-1", "earlier-2"].map((reference) => ({
            ...peerMessage,
            id: held(reference),
            visibleToAgent: true,
          })),
          facts: ["fact-1", "fact-2"].map((reference) => ({
            id: held(reference),
            source: "owner-calendar",
            text: privateEvidence,
            visibility: "assessor",
          })),
        },
      }).pipe(Effect.flip);
      expect(refused.reason).toBe("evidence");
    }),
);
it.prop(
  "refuses evidence that lists a participant twice",
  [fc.boolean(), fc.uuid(), otherAgents],
  ([incoming, repeated, others]) =>
    Effect.gen(function* () {
      const refused = yield* check(defaultProvider, {
        incoming,
        evidence: {
          ...evidence,
          participants: [repeated, ...others, repeated].map((agentId) => ({
            agentId,
            principal: "participant",
          })),
        },
      }).pipe(Effect.flip);
      expect(refused.reason).toBe("evidence");
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
          ? { judgment: "allow", reason: "eligible" }
          : { judgment: "hold", reason: "unresolved-evidence" },
      );
    }),
);
it.each([
  { name: "plain", cancellation: Effect.interrupt },
  {
    name: "cleanup defect",
    cancellation: Effect.interrupt.pipe(
      Effect.ensuring(Effect.die("cleanup failed")),
    ),
  },
  {
    name: "typed failure and interruption",
    cancellation: Effect.fail(
      new DecisionProviderError({ reason: "transport" }),
    ).pipe(Effect.ensuring(Effect.interrupt)),
  },
])("preserves provider interruption with $name", ({ cancellation }) =>
  Effect.gen(function* () {
    const cancelling: DecisionProvider = { decide: () => cancellation };
    const running = yield* check(cancelling).pipe(Effect.fork);
    const exit = yield* Fiber.await(running);
    expect(
      Exit.match(exit, {
        onFailure: Cause.isInterrupted,
        onSuccess: () => false,
      }),
    ).toBe(true);
  }),
);
it.prop(
  "holds a message exactly when the provider answers after the total or the per-request deadline",
  [
    fc
      .tuple(
        fc.integer({ min: 1, max: 1000 }),
        fc.integer({ min: 1, max: 1000 }),
        fc.nat({ max: 1000 }),
      )
      .filter(
        ([total, perRequest, delay]) => delay !== Math.min(total, perRequest),
      ),
  ],
  ([[total, perRequest, delay]]) =>
    Effect.gen(function* () {
      const slow: DecisionProvider = {
        decide: (request) =>
          Effect.sleep(delay).pipe(Effect.as(responseFor(request))),
      };
      const running = yield* check(slow, {
        limits: { ...checkLimits, timeoutMilliseconds: total },
        providerSettings: {
          ...providerSettings,
          timeoutMilliseconds: perRequest,
        },
      }).pipe(Effect.fork);
      yield* TestClock.adjust(delay);
      const result = yield* Fiber.join(running);
      expect(result).toMatchObject(
        delay < Math.min(total, perRequest)
          ? { judgment: "allow", feedback: [] }
          : {
              judgment: "hold",
              reason: "check-unavailable",
              feedback: [{ text: repair }],
            },
      );
    }),
);
function requestBytes(request: DecisionRequest): number {
  return new TextEncoder().encode(JSON.stringify(request)).byteLength;
}
it.prop(
  "submits a request exactly when its UTF-8 bytes fit maximumRequestBytes, and holds the message without asking otherwise",
  [fc.string({ unit: "grapheme" }), fc.integer({ min: -4, max: 4 })],
  ([text, spare]) =>
    Effect.gen(function* () {
      const message = { ...draft, text };
      const captured: DecisionRequest[] = [];
      yield* check(provider({}, captured), {
        message,
        providerSettings: {
          ...providerSettings,
          maximumRequestBytes: Number.MAX_SAFE_INTEGER,
        },
      });
      const bytes = Math.max(...captured.map(requestBytes));
      const submitted: DecisionRequest[] = [];
      const result = yield* check(provider({}, submitted), {
        message,
        providerSettings: {
          ...providerSettings,
          maximumRequestBytes: bytes + spare,
        },
      });
      expect(result.reason).toBe(spare >= 0 ? "eligible" : "check-unavailable");
      expect(submitted.map(requestBytes)).toEqual(spare >= 0 ? [bytes] : []);
    }),
);
it.prop(
  "holds a message exactly when its captured evidence or what the check found is larger than the configured bytes",
  [
    fc.string({ unit: "grapheme" }),
    fc.constantFrom(
      "maximumInputBytes" as const,
      "maximumResultBytes" as const,
    ),
    fc.integer({ min: -4, max: 4 }),
  ],
  ([text, bound, spare]) =>
    Effect.gen(function* () {
      const message = { ...draft, text };
      const bytes = (value: unknown) =>
        new TextEncoder().encode(JSON.stringify(value)).byteLength;
      const unbounded = yield* check(defaultProvider, { message });
      const measured = {
        maximumInputBytes: bytes({
          event: { direction: "outgoing", message },
          evidence,
        }),
        maximumResultBytes: bytes({
          findings: unbounded.findings,
          trace: unbounded.trace,
        }),
      };
      const result = yield* check(defaultProvider, {
        message,
        limits: { ...checkLimits, [bound]: measured[bound] + spare },
      });
      expect(result).toMatchObject(
        spare >= 0
          ? { judgment: "allow", reason: "eligible" }
          : { judgment: "hold", reason: "check-unavailable", instructions: [] },
      );
    }),
);

/** Schema-minimum message and evidence keep this boundary independent of variable evidence size. */
const minimumCaptured = {
  message: {
    id: "m",
    sender: "b",
    recipients: ["a"],
    text: "",
  } satisfies AssessmentText,
  evidence: {
    agentId: "a",
    principal: "p",
    participants: [],
    history: [],
    facts: [],
    gaps: [],
  } satisfies CheckEvidence,
};
function checkMinimumRequests(
  decisionProvider: DecisionProvider,
  maximumRequestBytes: number,
  textRules: readonly TextRule[],
) {
  return Effect.forEach(
    [true, false],
    (incoming) =>
      check(decisionProvider, {
        rules: textRules,
        ...minimumCaptured,
        message: incoming
          ? minimumCaptured.message
          : {
              ...minimumCaptured.message,
              sender: minimumCaptured.evidence.agentId,
              recipients: [minimumCaptured.message.sender],
            },
        incoming,
        providerSettings: { ...providerSettings, maximumRequestBytes },
      }),
    { concurrency: 1 },
  );
}
it.prop(
  "accepts a rule set exactly when the smallest request in each direction fits maximumRequestBytes, and then allows that request",
  [
    fc.array(
      fc.record({
        applies: applicability,
        text: fc.string({ minLength: 1, unit: "grapheme" }),
      }),
      { minLength: 1, maxLength: 3 },
    ),
    fc.integer({ min: -4, max: 4 }),
  ],
  ([stated, spare]) =>
    Effect.gen(function* () {
      const textRules = stated.map(
        (rule, index): TextRule => ({ id: `rule-${index}`, ...rule }),
      );
      const captured: DecisionRequest[] = [];
      yield* checkMinimumRequests(
        provider({}, captured),
        Number.MAX_SAFE_INTEGER,
        textRules,
      );
      const minimumBytes = Math.max(...captured.map(requestBytes));
      const judgments = yield* checkMinimumRequests(
        provider(),
        minimumBytes + spare,
        textRules,
      ).pipe(
        Effect.match({
          onFailure: (rejected) => rejected.reason,
          onSuccess: (results) => results.map((result) => result.judgment),
        }),
      );
      expect(judgments).toEqual(spare >= 0 ? ["allow", "allow"] : "rules");
    }),
);
