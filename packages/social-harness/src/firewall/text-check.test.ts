/** @file Text checks preserve stage independence, provider failures and registered advice. */
import { effect as it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Array as EffectArray,
  Exit,
  FastCheck as fc,
  Fiber,
  Schema,
} from "effect";
import { expect } from "vitest";
import {
  checkMessage,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
  type Message,
  type MessageContext,
  validateChecks,
} from "./check.js";
import {
  decodeMessagePolicy,
  type TextPolicySettings,
  type TextRule,
} from "./config.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Closed judgments, stages and next actions are the public contract under test. */
const privateEvidence = "PRIVATE: unrelated medical appointment at 14:00";
const repair =
  "Ask for acceptance of the chosen proposal. Do not claim agreement yet.";
const reminder =
  "After proposing a time, wait for every required participant to accept that proposal.";
const draft: typeof Message.Encoded = {
  id: "draft",
  sender: "organizer-agent",
  recipients: ["participant-agent"],
  text: "The meeting is confirmed; everyone agreed.",
};
const context: typeof MessageContext.Encoded = {
  agentId: "organizer-agent",
  principal: "organizer",
  participants: [{ agentId: "participant-agent", principal: "participant" }],
  history: [
    {
      id: "availability",
      sender: "participant-agent",
      recipients: ["organizer-agent"],
      text: "I am free Tuesday.",
      visibility: "runtime",
      admission: "admitted",
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
  guidance: reminder,
  feedback: { text: repair, nextAction: "revise" },
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
const settings: typeof TextPolicySettings.Encoded = {
  minimumProbability: 0.5,
  maximumRequestBytes: 60000,
  timeoutMilliseconds: 100,
};
const firewallSettings = {
  timeoutMilliseconds: 2000,
  maximumInputBytes: 300000,
  maximumResultBytes: 1000000,
};
/** Each generated example runs two complete assessments alongside other CI workers. */
const propertyTimeoutMilliseconds = 15000;
/**
 * A provider's answers: each question keyed in `choices` by
 * `stage:ruleId:purpose[:subject]` takes that choice with certainty, or the
 * full answer given there, and every other question its default choice.
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
        const fallback =
          ["compliant", "none", "relevant"].find((choice) =>
            Object.hasOwn(question.criteria, choice),
          ) ?? "unresolved";
        const selected =
          choices[
            `${request.stage}:${question.ruleId}:${question.purpose}:${question.subject ?? ""}`
          ] ??
          choices[`${request.stage}:${question.ruleId}:${question.purpose}`] ??
          fallback;
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
/** Local response expectations supplied to candidate compliance. */
const statedResponseExpectations = Schema.parseJson(
  Schema.Struct({
    responseExpectations: Schema.Array(
      Schema.Struct({ ruleId: Schema.String }),
    ),
  }),
);
function check(
  decisions: DecisionProvider = defaultProvider,
  options: {
    readonly message?: Message;
    readonly context?: MessageContext;
    readonly rules?: readonly TextRule[];
    readonly settings?: TextPolicySettings;
    readonly incoming?: boolean;
  } = {},
) {
  return Effect.gen(function* () {
    const configuration = {
      rules: options.rules ?? rules,
      settings: firewallSettings,
      textSettings: options.settings ?? settings,
      revision: "a".repeat(64),
    };
    yield* validateChecks(configuration);
    return yield* checkMessage({
      ...configuration,
      provider: decisions,
      event: {
        direction: options.incoming ? "incoming" : "outgoing",
        message: options.message ?? draft,
      },
      context: options.context ?? context,
    });
  });
}
it("preserves the receiver of an injected provider method", () =>
  Effect.gen(function* () {
    const stateful = {
      model: "stateful-provider",
      decide(request: DecisionRequest) {
        return Effect.succeed({ ...responseFor(request), model: this.model });
      },
    };
    const result = yield* check(stateful);
    expect(result.judgment).toBe("allow");
    expect(result.trace?.data).toContain(stateful.model);
  }));
it("reports earlier history omissions to the provider without holding or colliding with owner facts", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const result = yield* check(provider({}, requests), {
      context: {
        ...context,
        earlierHistoryOmitted: true,
        facts: [
          ...context.facts,
          {
            id: "client:earlier-history-omitted",
            source: "owner",
            text: "Owner evidence retains its own reference.",
            visibility: "assessor",
          },
        ],
      },
    });
    expect(result.reason).toBe("eligible");
    expect(requests.length).toBeGreaterThan(0);
    expect(
      requests.every((request) =>
        request.state.includes('"earlierHistoryOmitted":true'),
      ),
    ).toBe(true);
  }));
/** The parts of a request's state that say when it was captured and which operation it checks. */
const statedTimeAndCandidate = Schema.parseJson(
  Schema.Struct({
    now: Schema.optional(Schema.Number),
    candidate: Schema.Struct({
      operation: Schema.optional(Schema.Struct({ operationId: Schema.String })),
    }),
  }),
);
it("states the capture time and each message's collective operation to the provider", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const operation = {
      kind: "request",
      op: "gather",
      operationId: "col_question",
      deadlineAt: 2000,
    } as const;
    yield* check(provider({}, requests), {
      message: { ...draft, operation },
      context: { ...context, now: 1000 },
    });
    const states = requests.map((request) =>
      Schema.decodeUnknownSync(statedTimeAndCandidate)(request.state),
    );
    expect(states.length).toBeGreaterThan(0);
    expect(states.every((state) => state.now === 1000)).toBe(true);
    expect(
      states.some(
        (state) =>
          state.candidate.operation?.operationId === operation.operationId,
      ),
    ).toBe(true);
  }));
it.prop(
  "outgoing expectations exclude every unsent draft",
  [fc.string()],
  ([text]) =>
    Effect.gen(function* () {
      const original: DecisionRequest[] = [];
      const changed: DecisionRequest[] = [];
      yield* check(provider({}, original));
      yield* check(provider({}, changed), { message: { ...draft, text } });
      const expected = original.find((entry) => entry.stage === "expectations");
      expect(expected).toBeDefined();
      expect(changed.find((entry) => entry.stage === "expectations")).toEqual(
        expected,
      );
    }),
  { timeout: propertyTimeoutMilliseconds },
);
const malformedAnswers: ReadonlyArray<{
  readonly name: string;
  readonly choice: string;
  readonly probabilities: Readonly<Record<string, number>>;
}> = [
  {
    name: "unknown choice",
    choice: "allow-everything",
    probabilities: { compliant: 1, violation: 0, unresolved: 0 },
  },
  {
    name: "missing option",
    choice: "compliant",
    probabilities: { compliant: 1 },
  },
  {
    name: "negative probability",
    choice: "compliant",
    probabilities: { compliant: 1, violation: -0.1, unresolved: 0.1 },
  },
  {
    name: "wrong sum",
    choice: "compliant",
    probabilities: { compliant: 0.9, violation: 0.9, unresolved: 0.9 },
  },
  {
    name: "nonmaximal selection",
    choice: "compliant",
    probabilities: { compliant: 0.1, violation: 0.8, unresolved: 0.1 },
  },
  {
    name: "selection below the maximum by more than the probability tolerance",
    choice: "compliant",
    probabilities: { compliant: 0.47, violation: 0.5, unresolved: 0.03 },
  },
];
it.each(malformedAnswers)(
  "holds malformed provider answers: $name",
  ({ choice, probabilities }) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({
          "compliance:privacy:assessment": { choice, probabilities },
        }),
      );
      expect(result.reason).toBe("check-unavailable");
      expect(result.guidance).toEqual([]);
    }),
);
/** The failure each stage of a check's trace records, if any. */
const stageFailures = Schema.parseJson(
  Schema.Struct({
    stages: Schema.Array(
      Schema.Struct({
        stage: Schema.String,
        failure: Schema.optional(
          Schema.Struct({
            reason: Schema.String,
            accounting: Schema.Unknown,
          }),
        ),
      }),
    ),
  }),
);
it("keeps an invalid answer's usage in the trace", () =>
  Effect.gen(function* () {
    const usage = { input_tokens: 10, output_tokens: 2 };
    const result = yield* check({
      decide: (request) =>
        Effect.succeed({
          ...responseFor(request, {
            "compliance:privacy:assessment": "unknown",
          }),
          usage,
        }),
    });
    const { stages } = Schema.decodeUnknownSync(stageFailures)(
      result.trace?.data,
    );
    expect(
      stages.find((entry) => entry.stage === "compliance")?.failure,
    ).toEqual({
      reason: "invalid-response",
      accounting: { provider: "test-decisions", model: "test-model", usage },
    });
  }));
it.each([
  {
    name: "a restrictive choice",
    choice: "violation",
    probabilities: { compliant: 0.51, violation: 0.5, unresolved: 0 },
  },
  {
    name: "a permissive choice",
    choice: "compliant",
    probabilities: { compliant: 0.5, violation: 0.51, unresolved: 0 },
  },
])(
  "admits a rule whose stated choice trails another option within the probability tolerance: $name",
  ({ choice, probabilities }) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({
          "compliance:privacy:assessment": { choice, probabilities },
        }),
      );
      expect(result.reason).toBe("eligible");
    }),
);
it("admits a confidently unresolved response expectation as guidance", () =>
  Effect.gen(function* () {
    const result = yield* check(
      provider({
        "expectations:proposal-acceptance:assessment": "unresolved",
      }),
    );
    expect(result.judgment).toBe("allow");
    expect(result.guidance).toContainEqual({
      ruleId: "proposal-acceptance",
      uncertain: { choice: "unresolved", probability: 1 },
      instruction: reminder,
    });
  }));
const replyRule: TextRule = {
  id: "reply-once",
  applies: "both",
  text: "Reply once to a direct question.",
  guidance: "Reply once.",
};
it("admits an unsettled expectation as guidance and assesses compliance against the settled ones", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const result = yield* check(
      provider(
        {
          "expectations:proposal-acceptance:assessment": {
            choice: "required",
            probabilities: {
              required: 0.4,
              permitted: 0.3,
              wait: 0.1,
              none: 0.1,
              unresolved: 0.1,
            },
          },
          "expectations:reply-once:assessment": "required",
        },
        requests,
      ),
      { rules: [...rules, replyRule] },
    );
    const compliance = requests.find((entry) => entry.stage === "compliance");
    expect(result.judgment).toBe("allow");
    expect(result.guidance).toContainEqual({
      ruleId: "proposal-acceptance",
      uncertain: { choice: "required", probability: 0.4 },
      instruction: reminder,
    });
    expect(result.findings).toContainEqual({
      ruleId: "proposal-acceptance",
      outcome: "pass",
      reason: "uncertain",
      choice: "required",
      probability: 0.4,
    });
    expect(
      Schema.decodeUnknownSync(statedResponseExpectations)(compliance?.state)
        .responseExpectations,
    ).toEqual([{ ruleId: "privacy" }, { ruleId: "reply-once" }]);
  }));
it("holds a failed expectations decision as check-unavailable", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const result = yield* check({
      decide: (request) =>
        Effect.suspend(() => {
          requests.push(request);
          return request.stage === "expectations"
            ? Effect.fail(new DecisionProviderError({ reason: "http" }))
            : Effect.succeed(responseFor(request));
        }),
    });
    expect(result.reason).toBe("check-unavailable");
    expect(requests.some((entry) => entry.stage === "compliance")).toBe(false);
  }));
it("captures evidence before asynchronous inference", () =>
  Effect.gen(function* () {
    const message = { ...draft };
    const requests: DecisionRequest[] = [];
    const started = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const decisions: DecisionProvider = {
      decide: (request) =>
        Effect.gen(function* () {
          requests.push(request);
          if (request.stage === "expectations") {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
          }
          return responseFor(request);
        }),
    };
    const running = yield* check(decisions, { message }).pipe(Effect.fork);
    yield* Deferred.await(started);
    message.text = "Replacement after capture";
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(running);
    expect(
      requests.find((entry) => entry.stage === "compliance")?.state,
    ).toContain(draft.text);
  }));
it("returns rule repair with only registered feedback", () =>
  Effect.gen(function* () {
    const result = yield* check(
      provider({
        "compliance:proposal-acceptance:assessment": "violation",
      }),
    );
    expect(result.judgment).toBe("reject");
    expect(result.feedback).toEqual([{ nextAction: "revise", text: repair }]);
    expect(result.guidance).toContainEqual({
      ruleId: proposalAcceptance.id,
      instruction: reminder,
    });
  }));
it("retains a settled violation beside another rule's valid uncertainty", () =>
  Effect.gen(function* () {
    const result = yield* check(
      provider({
        "compliance:privacy:assessment": "violation",
        "compliance:proposal-acceptance:assessment": "unresolved",
      }),
    );
    expect(result.reason).toBe("rule-violation");
    expect(result.findings).toContainEqual({
      ruleId: "privacy",
      outcome: "violation",
    });
    expect(
      JSON.stringify({
        feedback: result.feedback,
        guidance: result.guidance,
      }),
    ).not.toContain(privateEvidence);
  }));
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
    const decisions: DecisionProvider = { decide: () => cancellation };
    const running = yield* check(decisions).pipe(Effect.fork);
    const exit = yield* Fiber.await(running);
    expect(
      Exit.match(exit, {
        onFailure: Cause.isInterrupted,
        onSuccess: () => false,
      }),
    ).toBe(true);
  }),
);
it.each([
  {
    name: "rounded",
    probabilities: { compliant: 0.71, violation: 0.2, unresolved: 0.1 },
  },
  {
    name: "unrounded",
    probabilities: { compliant: 0.7125, violation: 0.1875, unresolved: 0.1 },
  },
])(
  "preserves $name distributions and routes by selected probability",
  ({ probabilities }) =>
    Effect.gen(function* () {
      const decisions = provider({
        "compliance:privacy:assessment": {
          choice: "compliant",
          probabilities,
        },
      });
      const allowed = yield* check(decisions);
      const held = yield* check(decisions, {
        settings: { ...settings, minimumProbability: 0.8 },
      });
      expect(allowed.judgment).toBe("allow");
      expect(allowed.trace?.data).toContain(JSON.stringify(probabilities));
      expect(held.reason).toBe("eligible");
    }),
);
it("keeps renderer templates out of model assessment rules", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    yield* check(provider({}, requests));
    for (const request of requests) {
      expect(request.state).not.toContain(reminder);
      expect(request.state).not.toContain(repair);
      expect(request.state).toContain(proposalAcceptance.text);
    }
  }));
it("projects incoming advice using only admitted visible subjects", () =>
  Effect.gen(function* () {
    const message: typeof Message.Encoded = {
      id: "proposal",
      sender: "participant-agent",
      recipients: [context.agentId],
      text: "Can we meet Tuesday?",
    };
    const requests: DecisionRequest[] = [];
    const result = yield* check(
      provider(
        { "expectations:proposal-acceptance:assessment": "required" },
        requests,
      ),
      {
        incoming: true,
        message,
        context: {
          ...context,
          history: [
            ...context.history,
            {
              ...message,
              id: "private-history",
              visibility: "assessor",
              admission: "admitted",
            },
            {
              ...message,
              id: "withheld-history",
              visibility: "runtime",
              admission: "withheld",
            },
          ],
        },
      },
    );
    expect(result.judgment).toBe("allow");
    expect(result.guidance).toEqual([
      {
        ruleId: "proposal-acceptance",
        expectation: "required",
        subjects: ["availability", "proposal"],
        instruction: reminder,
      },
    ]);
    expect(requests.some((entry) => entry.stage === "compliance")).toBe(true);
    expect(JSON.stringify(result.guidance)).not.toContain(privateEvidence);
  }));
it("leaves an unsettled reference out of an incoming expectation", () =>
  Effect.gen(function* () {
    const message: typeof Message.Encoded = {
      id: "proposal",
      sender: "participant-agent",
      recipients: [context.agentId],
      text: "Can we meet Tuesday?",
    };
    const result = yield* check(
      provider({
        "expectations:proposal-acceptance:assessment": "required",
        "expectations:proposal-acceptance:subject:availability": "unresolved",
      }),
      { incoming: true, message },
    );
    expect(result.judgment).toBe("allow");
    expect(result.guidance).toEqual([
      {
        ruleId: "proposal-acceptance",
        expectation: "required",
        subjects: ["proposal"],
        instruction: reminder,
      },
    ]);
  }));
it("holds a model-selected private subject instead of projecting it", () =>
  Effect.gen(function* () {
    const result = yield* check(
      provider({
        "expectations:proposal-acceptance:subject": "message:private-calendar",
      }),
    );
    expect(result.reason).toBe("check-unavailable");
    expect(result.guidance).toEqual([]);
  }));
it("routes each text rule without consulting the provider for the opposite direction", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const incomingOnly: TextRule = {
      id: proposalAcceptance.id,
      text: proposalAcceptance.text,
      guidance: reminder,
      applies: "incoming",
    };
    const result = yield* check(provider({}, requests), {
      rules: [incomingOnly],
    });
    expect(result.judgment).toBe("allow");
    expect(requests).toEqual([]);
    expect(result.findings).toEqual([
      { ruleId: incomingOnly.id, outcome: "not-applicable" },
    ]);
  }));
it("checks incoming peer content without imposing local response expectations", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const message: typeof Message.Encoded = {
      ...draft,
      sender: "participant-agent",
      recipients: [context.agentId],
    };
    const result = yield* check(
      provider(
        { "expectations:proposal-acceptance:assessment": "required" },
        requests,
      ),
      { incoming: true, message },
    );
    const compliance = requests.find(
      (request) => request.stage === "compliance",
    );
    expect(
      Schema.decodeUnknownSync(statedResponseExpectations)(compliance?.state)
        .responseExpectations,
    ).toEqual([]);
    expect(result.judgment).toBe("allow");
    expect(result.guidance).toContainEqual({
      ruleId: proposalAcceptance.id,
      expectation: "required",
      subjects: ["availability", draft.id],
      instruction: reminder,
    });
  }));
it("submits a complete request at its UTF-8 byte limit and holds one byte below it", () =>
  Effect.gen(function* () {
    const captured: DecisionRequest[] = [];
    const options = {
      context: { ...context, history: [], facts: [] },
      message: { ...draft, text: "Tuesday works. 🗓️" },
      rules: rules
        .filter((rule) => rule.id === "privacy")
        .map(
          (rule): TextRule => ({
            id: rule.id,
            text: rule.text,
            applies: "outgoing",
          }),
        ),
    };
    yield* check(provider({}, captured), {
      ...options,
      settings: { ...settings, maximumRequestBytes: Number.MAX_SAFE_INTEGER },
    });
    const bytes = Math.max(...captured.map(requestBytes));
    const submitted: DecisionRequest[] = [];
    const accepted = yield* check(provider({}, submitted), {
      ...options,
      settings: { ...settings, maximumRequestBytes: bytes },
    });
    expect(accepted.judgment).toBe("allow");
    expect(Math.max(...submitted.map(requestBytes))).toBe(bytes);
    const rejected: DecisionRequest[] = [];
    const unavailable = yield* check(provider({}, rejected), {
      ...options,
      settings: { ...settings, maximumRequestBytes: bytes - 1 },
    });
    expect(unavailable.reason).toBe("check-unavailable");
    expect(rejected.every((request) => requestBytes(request) < bytes)).toBe(
      true,
    );
  }));
const accumulatedRules: readonly TextRule[] = EffectArray.makeBy(
  8,
  (index) => ({
    id: `rule-${index}`,
    applies: "both",
    text: `Address open request number ${index} before changing the subject.`,
  }),
);
const longHistory: MessageContext["history"] = EffectArray.makeBy(
  50,
  (index) => ({
    ...draft,
    id: `visible-${index}`,
    visibility: "runtime",
    admission: "admitted",
  }),
);
it.each([false, true])(
  "assesses every rule over a long window within the default budget (incoming: %s)",
  (incoming) =>
    Effect.gen(function* () {
      const requests: DecisionRequest[] = [];
      const { textSettings } = yield* decodeMessagePolicy(
        JSON.stringify({
          format: "org.social-harness/message-policy/v2",
          mode: "enforcement",
        }),
      );
      const { maximumRequestBytes } = textSettings;
      const result = yield* check(provider({}, requests), {
        incoming,
        message: incoming
          ? {
              ...draft,
              sender: "participant-agent",
              recipients: [context.agentId],
            }
          : draft,
        rules: accumulatedRules,
        context: { ...context, history: longHistory },
        settings: { ...settings, maximumRequestBytes },
      });
      expect(result.judgment).toBe("allow");
      const expectations = requests.find(
        (request) => request.stage === "expectations",
      );
      expect(expectations).toBeDefined();
      expect(
        Object.values(expectations?.questions ?? {}).filter(
          (question) => question.purpose === "subject",
        ),
      ).toHaveLength(
        accumulatedRules.length * (longHistory.length + Number(incoming)),
      );
      for (const request of requests) {
        const serialized = JSON.stringify(request);
        expect(
          new TextEncoder().encode(serialized).byteLength,
        ).toBeLessThanOrEqual(maximumRequestBytes);
        for (const rule of accumulatedRules) {
          expect(serialized.split(rule.text)).toHaveLength(2);
        }
      }
    }),
);
it("bounds large policy-history products without submitting partial requests", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const result = yield* check(provider({}, requests), {
      rules: EffectArray.makeBy(20, (index) => ({
        id: `rule-${index}`,
        applies: "both",
        text: "Speak only about open requests.",
      })),
      context: {
        ...context,
        history: EffectArray.makeBy(100, (index) => ({
          ...draft,
          id: `visible-${index}`,
          text: "Earlier conversation. ".repeat(100),
          visibility: "runtime",
          admission: "admitted",
        })),
      },
    });
    expect(result.reason).toBe("check-unavailable");
    expect(requests).toEqual([]);
    expect(
      new TextEncoder().encode(result.trace?.data).byteLength,
    ).toBeLessThan(settings.maximumRequestBytes);
  }));
it("holds oversized context without truncating it", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const result = yield* check(provider({}, requests), {
      context: {
        ...context,
        facts: [
          {
            id: "large-fact",
            source: "owner",
            text: "x".repeat(settings.maximumRequestBytes * 2),
            visibility: "assessor",
          },
        ],
      },
    });
    expect(result.reason).toBe("check-unavailable");
    expect(requests).toEqual([]);
  }));
function requestBytes(request: DecisionRequest): number {
  return new TextEncoder().encode(JSON.stringify(request)).byteLength;
}

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore assertion checks after public contract cases. */
