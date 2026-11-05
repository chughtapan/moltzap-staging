/** @file Message checks enforce configured limits and release only the advice their evidence permits. */

import { effect as it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Array as EffectArray,
  Exit,
  Fiber,
  Schema,
  TestClock,
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
  type FirewallSettings,
  type TextPolicySettings,
  type TextRule,
} from "./rules.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Closed judgments, reasons and next actions are the public contract under test. */
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
  guidance: reminder,
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
const malformedAnswers: ReadonlyArray<{
  readonly name: string;
  readonly choice: string;
  readonly probabilities: Readonly<Record<string, number>>;
}> = [
  {
    name: "unknown choice",
    choice: "allow-everything",
    probabilities: { compliant: 1, violation: 0 },
  },
  {
    name: "missing option",
    choice: "compliant",
    probabilities: { compliant: 1 },
  },
  {
    name: "negative probability",
    choice: "compliant",
    probabilities: { compliant: 1, violation: -0.01 },
  },
  {
    name: "wrong sum",
    choice: "compliant",
    probabilities: { compliant: 0.9, violation: 0.9 },
  },
  {
    name: "selection below the maximum by more than the probability tolerance",
    choice: "compliant",
    probabilities: { compliant: 0.47, violation: 0.53 },
  },
];
it.each(malformedAnswers)(
  "holds malformed provider answers: $name",
  ({ choice, probabilities }) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({ privacy: { choice, probabilities } }),
      );
      expect(result.reason).toBe("check-unavailable");
      expect(result.guidance).toEqual([]);
    }),
);
/** The failure a check's trace records, if any. */
const traceFailure = Schema.parseJson(
  Schema.Struct({
    trace: Schema.Struct({
      failure: Schema.optional(
        Schema.Struct({
          reason: Schema.String,
          accounting: Schema.Unknown,
        }),
      ),
    }),
  }),
);
it("keeps an invalid answer's usage in the trace", () =>
  Effect.gen(function* () {
    const usage = { input_tokens: 10, output_tokens: 2 };
    const result = yield* check({
      decide: (request) =>
        Effect.succeed({
          ...responseFor(request, { privacy: "unknown" }),
          usage,
        }),
    });
    const { trace } = Schema.decodeUnknownSync(traceFailure)(
      result.trace?.data,
    );
    expect(trace.failure).toEqual({
      reason: "invalid-response",
      accounting: { provider: "test-decisions", model: "test-model", usage },
    });
  }));
it.each([
  {
    name: "a restrictive choice",
    choice: "violation",
    probabilities: { compliant: 0.51, violation: 0.5 },
  },
  {
    name: "a permissive choice",
    choice: "compliant",
    probabilities: { compliant: 0.5, violation: 0.51 },
  },
])(
  "holds an outgoing draft as unresolved evidence when its stated choice trails another option within the probability tolerance: $name",
  ({ choice, probabilities }) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({ [proposalAcceptance.id]: { choice, probabilities } }),
      );
      expect(result).toMatchObject({
        reason: "unresolved-evidence",
        feedback: [{ text: repair }],
      });
    }),
);
it("holds an unsure outgoing draft as unresolved evidence with the rule's registered feedback", () =>
  Effect.gen(function* () {
    const result = yield* check(
      provider({
        [proposalAcceptance.id]: {
          choice: "compliant",
          probabilities: { compliant: 0.6, violation: 0.4 },
        },
      }),
      { settings: { ...settings, minimumProbability: 0.8 } },
    );
    expect(result).toMatchObject({
      judgment: "hold",
      reason: "unresolved-evidence",
      feedback: [{ text: repair }],
    });
  }));
it("refuses an unsure outgoing draft with the guidance of a rule that has no feedback", () =>
  Effect.gen(function* () {
    const guidanceOnly: TextRule = {
      id: proposalAcceptance.id,
      applies: "both",
      text: proposalAcceptance.text,
      guidance: reminder,
    };
    const result = yield* check(
      provider({
        [guidanceOnly.id]: {
          choice: "compliant",
          probabilities: { compliant: 0.6, violation: 0.4 },
        },
      }),
      {
        rules: [guidanceOnly],
        settings: { ...settings, minimumProbability: 0.8 },
      },
    );
    expect(result).toMatchObject({
      reason: "unresolved-evidence",
      feedback: [{ text: reminder }],
    });
  }));
it("captures evidence before asynchronous inference", () =>
  Effect.gen(function* () {
    const gaps: string[] = [];
    const started = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const decisions: DecisionProvider = {
      decide: (request) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(started, undefined);
          yield* Deferred.await(release);
          return responseFor(request);
        }),
    };
    const running = yield* check(decisions, {
      context: { ...context, gaps },
    }).pipe(Effect.fork);
    yield* Deferred.await(started);
    gaps.push("The preceding conversation history is unavailable.");
    yield* Deferred.succeed(release, undefined);
    const result = yield* Fiber.join(running);
    expect(result.reason).toBe("eligible");
  }));
it("returns rule repair with only registered feedback", () =>
  Effect.gen(function* () {
    const result = yield* check(
      provider({ [proposalAcceptance.id]: "violation" }),
    );
    expect(result.judgment).toBe("reject");
    expect(result.feedback).toEqual([{ text: repair }]);
  }));
it("retains a settled violation beside another rule's valid uncertainty", () =>
  Effect.gen(function* () {
    const result = yield* check(
      provider({
        privacy: "violation",
        [proposalAcceptance.id]: {
          choice: "compliant",
          probabilities: { compliant: 0.6, violation: 0.4 },
        },
      }),
      { settings: { ...settings, minimumProbability: 0.8 } },
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
    probabilities: { compliant: 0.71, violation: 0.3 },
  },
  {
    name: "unrounded",
    probabilities: { compliant: 0.7125, violation: 0.2875 },
  },
])(
  "preserves $name distributions and routes by selected probability",
  ({ probabilities }) =>
    Effect.gen(function* () {
      const decisions = provider({
        [proposalAcceptance.id]: { choice: "compliant", probabilities },
      });
      const allowed = yield* check(decisions);
      const held = yield* check(decisions, {
        settings: { ...settings, minimumProbability: 0.8 },
      });
      expect(allowed.judgment).toBe("allow");
      expect(allowed.trace?.data).toContain(JSON.stringify(probabilities));
      expect(held).toMatchObject({
        reason: "unresolved-evidence",
        feedback: [{ text: repair }],
      });
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
/** A peer's message to the local agent. */
const peerMessage: typeof Message.Encoded = {
  id: "proposal",
  sender: "participant-agent",
  recipients: [context.agentId],
  text: "Can we meet Tuesday?",
};
it.each([
  {
    advice: "guidance of its rule",
    rule: proposalAcceptance,
    instruction: reminder,
  },
  {
    advice: "feedback of a rule without guidance",
    rule: {
      id: proposalAcceptance.id,
      applies: "both",
      text: proposalAcceptance.text,
      feedback: { text: repair },
    } satisfies TextRule,
    instruction: repair,
  },
])(
  "admits an unsure incoming message with the provider's choice and the $advice as a reminder",
  ({ rule, instruction }) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({
          [rule.id]: {
            choice: "violation",
            probabilities: { compliant: 0.4, violation: 0.6 },
          },
        }),
        {
          incoming: true,
          message: peerMessage,
          rules: [rule],
          settings: { ...settings, minimumProbability: 0.8 },
        },
      );
      expect(result.reason).toBe("eligible");
      expect(result.guidance).toEqual([
        {
          ruleId: rule.id,
          instruction,
          uncertain: { choice: "violation", probability: 0.6 },
        },
      ]);
    }),
);
it.each([
  { direction: "outgoing", incoming: false, message: draft },
  { direction: "incoming", incoming: true, message: peerMessage },
])(
  "admits a confidently compliant $direction message without guidance",
  ({ incoming, message }) =>
    Effect.gen(function* () {
      const result = yield* check(
        provider({
          [proposalAcceptance.id]: {
            choice: "compliant",
            probabilities: { compliant: 0.9, violation: 0.1 },
          },
        }),
        {
          incoming,
          message,
          settings: { ...settings, minimumProbability: 0.8 },
        },
      );
      expect(result).toMatchObject({
        judgment: "allow",
        reason: "eligible",
        guidance: [],
      });
    }),
);
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
    expect(submitted.map(requestBytes)).toEqual([bytes]);
    const rejected: DecisionRequest[] = [];
    const unavailable = yield* check(provider({}, rejected), {
      ...options,
      settings: { ...settings, maximumRequestBytes: bytes - 1 },
    });
    expect(unavailable.reason).toBe("check-unavailable");
    expect(rejected).toEqual([]);
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
    visibleToAgent: true,
  }),
);
it.each([false, true])(
  "assesses every rule over a long window within the default budget (incoming: %s)",
  (incoming) =>
    Effect.gen(function* () {
      const requests: DecisionRequest[] = [];
      const { textSettings } = yield* decodeMessagePolicy(
        JSON.stringify({ format: "org.social-harness/message-policy/v2" }),
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
      expect(
        requests.map((request) =>
          Object.values(request.questions).map((question) => question.ruleId),
        ),
      ).toEqual([accumulatedRules.map((rule) => rule.id)]);
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
it("bounds a large policy and history without submitting a partial request", () =>
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
          visibleToAgent: true,
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

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Judgments and safe boundary failures are the behavior under test. */
// @agent-code-guard/regression-only: Captured evidence, configured text limits and inference deadlines protect concrete boundary failures.

const boundaryDraft: typeof Message.Encoded = {
  id: "draft",
  sender: "local-agent",
  recipients: ["peer-agent"],
  text: "Tuesday works.",
};
const incoming: typeof Message.Encoded = {
  id: "request",
  sender: "peer-agent",
  recipients: ["local-agent"],
  text: "Does Tuesday work?",
};
const boundaryContext: typeof MessageContext.Encoded = {
  agentId: "local-agent",
  principal: "owner",
  participants: [{ agentId: "peer-agent", principal: "peer-owner" }],
  history: [
    {
      ...incoming,
      id: "earlier",
      visibleToAgent: true,
    },
  ],
  facts: [
    {
      id: "private-fact",
      text: "Owner's private schedule",
      source: "owner",
      visibility: "assessor",
    },
  ],
  gaps: [],
};
const boundarySettings: typeof FirewallSettings.Encoded = {
  timeoutMilliseconds: 1000,
  maximumInputBytes: 100000,
  maximumResultBytes: 100000,
};
it("allows incoming and outgoing messages with no rules", () =>
  Effect.gen(function* () {
    const firewall = {
      rules: [],
      revision: "fixture",
      settings: boundarySettings,
    };
    expect(
      (yield* checkMessage({
        ...firewall,
        event: { direction: "outgoing", message: boundaryDraft },
        context: boundaryContext,
      })).judgment,
    ).toBe("allow");
    expect(
      (yield* checkMessage({
        ...firewall,
        event: { direction: "incoming", message: incoming },
        context: boundaryContext,
      })).judgment,
    ).toBe("allow");
  }));
it("rejects ambiguous references, wrong attribution and duplicate recipients", () =>
  Effect.gen(function* () {
    const firewall = {
      rules: [],
      revision: "fixture",
      settings: boundarySettings,
    };
    const duplicate = yield* checkMessage({
      ...firewall,
      event: { direction: "outgoing", message: boundaryDraft },
      context: {
        ...boundaryContext,
        history: [...boundaryContext.history, ...boundaryContext.history],
      },
    }).pipe(Effect.flip);
    const forged = yield* checkMessage({
      ...firewall,
      event: {
        direction: "outgoing",
        message: { ...boundaryDraft, sender: "owner" },
      },
      context: boundaryContext,
    }).pipe(Effect.flip);
    const otherEndpoint = yield* checkMessage({
      ...firewall,
      event: {
        direction: "incoming",
        message: { ...incoming, recipients: ["elsewhere"] },
      },
      context: boundaryContext,
    }).pipe(Effect.flip);
    const recipients = yield* checkMessage({
      ...firewall,
      event: {
        direction: "outgoing",
        message: { ...boundaryDraft, recipients: ["peer", "peer"] },
      },
      context: boundaryContext,
    }).pipe(Effect.flip);
    expect(duplicate.reason).toBe("context");
    expect(forged.reason).toBe("message");
    expect(otherEndpoint.reason).toBe("message");
    expect(recipients.reason).toBe("message");
  }));
function budgetCheck(
  decisions: DecisionProvider,
  options: {
    message: Message;
    context: MessageContext;
    rules: readonly TextRule[];
    settings: TextPolicySettings;
    incoming: boolean;
  },
) {
  const configuration = {
    rules: options.rules,
    settings: boundarySettings,
    textSettings: options.settings,
    revision: "a".repeat(64),
  };
  return validateChecks(configuration).pipe(
    Effect.zipRight(
      checkMessage({
        ...configuration,
        provider: decisions,
        event: {
          direction: options.incoming ? "incoming" : "outgoing",
          message: options.message,
        },
        context: options.context,
      }),
    ),
  );
}

/** Schema-minimum message and context keep this boundary independent of variable evidence size. */
const minimumInput = {
  message: {
    id: "m",
    sender: "b",
    recipients: ["a"],
    text: "",
  } satisfies Message,
  context: {
    agentId: "a",
    principal: "p",
    participants: [],
    history: [],
    facts: [],
    gaps: [],
  } satisfies MessageContext,
};
function checkMinimumRequests(
  decisions: DecisionProvider,
  maximumRequestBytes: number,
  documents: readonly TextRule[] = rules,
) {
  return Effect.forEach(
    [true, false],
    (incoming) =>
      budgetCheck(decisions, {
        rules: documents,
        ...minimumInput,
        message: incoming
          ? minimumInput.message
          : {
              ...minimumInput.message,
              sender: minimumInput.context.agentId,
              recipients: [minimumInput.message.sender],
            },
        incoming,
        settings: { ...settings, maximumRequestBytes },
      }),
    { concurrency: 1 },
  );
}
it.each([
  rules,
  rules.map(
    (rule): TextRule => ({
      id: rule.id,
      text: rule.text,
      applies: rule.id === "privacy" ? "incoming" : "outgoing",
    }),
  ),
])(
  "requires enough installation budget for reachable minimum requests in each direction",
  (documents) =>
    Effect.gen(function* () {
      const captured: DecisionRequest[] = [];
      yield* checkMinimumRequests(
        provider({}, captured),
        Number.MAX_SAFE_INTEGER,
        documents,
      );
      const minimumBytes = Math.max(...captured.map(requestBytes));
      const results = yield* checkMinimumRequests(
        provider(),
        minimumBytes,
        documents,
      );
      expect(results.map((result) => result.judgment)).toEqual([
        "allow",
        "allow",
      ]);
      const rejected = yield* validateChecks({
        rules: documents,
        textSettings: {
          ...settings,
          maximumRequestBytes: minimumBytes - 1,
        },
        settings: boundarySettings,
        revision: "a".repeat(64),
      }).pipe(Effect.flip);
      expect(rejected.reason).toBe("rules");
    }),
);
it.each([
  {
    content: "documents",
    documents: [
      {
        id: proposalAcceptance.id,
        applies: proposalAcceptance.applies,
        text: "x".repeat(60000),
      },
    ],
    fittingBudget: 70000,
  },
  {
    content: "mandatory questions",
    documents: EffectArray.makeBy(100, (index) => ({
      id: `rule-${index}`,
      applies: proposalAcceptance.applies,
      text: "Follow the timetable.",
    })),
    fittingBudget: 200000,
  },
])(
  "rejects oversized $content during policy validation",
  ({ documents, fittingBudget }) =>
    Effect.gen(function* () {
      const configuration = {
        id: "owner",
        rules: documents,
      };
      yield* validateChecks({
        ...configuration,
        textSettings: { ...settings, maximumRequestBytes: fittingBudget },
        settings: boundarySettings,
        revision: "a".repeat(64),
      });
      const rejected = yield* validateChecks({
        ...configuration,
        textSettings: settings,
        settings: boundarySettings,
        revision: "a".repeat(64),
      }).pipe(Effect.flip);
      expect(rejected.reason).toBe("rules");
    }),
);

it("holds a text assessment that exceeds its total deadline", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<undefined>();
    const provider: DecisionProvider = {
      decide: () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.zipRight(Effect.never),
        ),
    };
    const running = yield* checkMessage({
      rules: rules,
      revision: "fixture",
      provider,
      settings: { ...boundarySettings, timeoutMilliseconds: 10 },
      textSettings: { ...settings, timeoutMilliseconds: 10_000 },
      event: { direction: "outgoing", message: boundaryDraft },
      context: boundaryContext,
    }).pipe(Effect.fork);
    yield* Deferred.await(started);
    yield* TestClock.adjust("10 millis");
    const result = yield* Fiber.join(running);
    expect(result).toMatchObject({
      judgment: "hold",
      reason: "check-unavailable",
    });
    expect(result.findings).toEqual(
      rules.map((rule) => ({
        ruleId: rule.id,
        outcome: "hold",
        reason: "check-unavailable",
      })),
    );
  }));

it.each([{ maximumInputBytes: 1 }, { maximumResultBytes: 1 }])(
  "holds text assessment beyond a configured data bound: %j",
  (bound) =>
    Effect.gen(function* () {
      const result = yield* checkMessage({
        rules: rules,
        revision: "fixture",
        provider: provider(),
        settings: { ...boundarySettings, ...bound },
        textSettings: settings,
        event: { direction: "outgoing", message: boundaryDraft },
        context: boundaryContext,
      });
      expect(result).toMatchObject({
        judgment: "hold",
        reason: "check-unavailable",
        guidance: [],
      });
    }),
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore assertion checks after public contract cases. */
