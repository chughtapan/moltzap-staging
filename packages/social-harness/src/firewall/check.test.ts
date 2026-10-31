/** @file Prepared inputs and registrations preserve composed decisions, budgets and registered advice. */
import { effect as it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Array as EffectArray,
  Exit,
  FastCheck as fc,
  Fiber,
  TestClock,
} from "effect";
import { expect } from "vitest";
import type {
  FirewallSettings,
  RuleDescription,
  TextPolicySettings,
  TextRule,
} from "./config.js";
import {
  checkMessage,
  type DecisionProvider,
  type DecisionRequest,
  type DecisionResponse,
  type Message,
  type MessageContext,
  type MessageMiddleware,
  MiddlewareError,
  type MiddlewareResult,
  validateChecks,
} from "./check.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Public judgments, findings and feedback are the behavior under test. */
type PolicyFinding = MiddlewareResult["findings"][number];
type ResponseExpectation = MiddlewareResult["expectations"][number];
const draft: typeof Message.Encoded = {
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
const context: typeof MessageContext.Encoded = {
  agentId: "local-agent",
  principal: "owner",
  participants: [{ agentId: "peer-agent", principal: "peer-owner" }],
  history: [
    {
      ...incoming,
      id: "earlier",
      visibility: "runtime",
      admission: "admitted",
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
const settings: typeof FirewallSettings.Encoded = {
  timeoutMilliseconds: 1000,
  middlewareTimeoutMilliseconds: 100,
  maximumInputBytes: 100000,
  maximumResultBytes: 100000,
};
/** Generated cases check whole chains alongside the package suite's CI workers. */
const propertyTimeoutMilliseconds = 15000;
const rule: typeof RuleDescription.Encoded = {
  id: "rule",
  guidance: "Respond only to the open request.",
};
const pass: PolicyFinding = { ruleId: rule.id, outcome: "pass" };
const block: PolicyFinding = { ruleId: rule.id, outcome: "violation" };
function policy(
  id: string,
  findings: readonly PolicyFinding[] = [pass],
  expectations: readonly ResponseExpectation[] = [],
): MessageMiddleware {
  return {
    id,
    revision: "v1",
    rules: [rule],
    check: () => Effect.succeed({ findings, expectations }),
  };
}
function codeConfiguration(
  code: readonly MessageMiddleware[],
  overrides: Partial<FirewallSettings> = {},
) {
  return {
    code,
    rules: [],
    revision: "fixture",
    settings: { ...settings, ...overrides },
  };
}
it.prop(
  "a pass never overrides any registered prohibition",
  [fc.array(fc.boolean(), { minLength: 1, maxLength: 8 })],
  ([prohibitions]) =>
    Effect.gen(function* () {
      const firewall = codeConfiguration(
        prohibitions.map((prohibited, index) =>
          policy(`policy-${index}`, [prohibited ? block : pass]),
        ),
      );
      const result = yield* checkMessage({
        ...firewall,
        event: { direction: "outgoing", message: draft },
        context: context,
      });
      expect(result.judgment).toBe(
        prohibitions.some(Boolean) ? "reject" : "allow",
      );
    }),
  { timeout: propertyTimeoutMilliseconds },
);
it("keeps captured evidence immutable between middleware", () =>
  Effect.gen(function* () {
    const tampering: MessageMiddleware = {
      ...policy("tampering"),
      check: (input) =>
        Effect.sync(() => ({
          findings: [
            Reflect.set(input, "event", {
              direction: "outgoing",
              message: draft,
            }) ||
            Reflect.set(input, "context", { ...context, principal: "forged" })
              ? block
              : pass,
          ],
          expectations: [],
        })),
    };
    const firewall = codeConfiguration([tampering, policy("later", [block])]);
    const result = yield* checkMessage({
      ...firewall,
      event: { direction: "incoming", message: incoming },
      context: context,
    });
    expect(result.checks[0]?.findings).toEqual([pass]);
    expect(result.judgment).toBe("reject");
  }));
it("isolates returned findings from mutation while later middleware waits", () =>
  Effect.gen(function* () {
    const result = { findings: [block, pass], expectations: [] };
    const started = yield* Deferred.make<undefined>();
    const release = yield* Deferred.make<undefined>();
    const first: MessageMiddleware = {
      ...policy("first"),
      check: () => Effect.succeed(result),
    };
    const second: MessageMiddleware = {
      ...policy("second"),
      check: () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.zipRight(Deferred.await(release)),
          Effect.as({ findings: [pass], expectations: [] }),
        ),
    };
    const firewall = codeConfiguration([first, second]);
    const running = yield* checkMessage({
      ...firewall,
      event: { direction: "outgoing", message: draft },
      context: context,
    }).pipe(Effect.fork);
    yield* Deferred.await(started);
    result.findings.splice(0, 1, pass);
    yield* Deferred.succeed(release, undefined);
    const checked = yield* Fiber.join(running);
    expect(checked.judgment).toBe("reject");
    expect(checked.checks[0]?.findings).toEqual([block, pass]);
  }));
it("checks incoming and outgoing candidates with their supplied code checks", () =>
  Effect.gen(function* () {
    const receive = codeConfiguration([policy("receive")]);
    const send = codeConfiguration([policy("send", [block])]);
    const received = yield* checkMessage({
      ...receive,
      event: { direction: "incoming", message: incoming },
      context,
    });
    const sent = yield* checkMessage({
      ...send,
      event: { direction: "outgoing", message: draft },
      context,
    });
    expect(received.judgment).toBe("allow");
    expect(sent.judgment).toBe("reject");
    expect(received.checks.map((entry) => entry.middlewareId)).toEqual([
      "receive",
    ]);
    expect(sent.checks.map((entry) => entry.middlewareId)).toEqual(["send"]);
  }));
it.each(["failure", "defect", "timeout"])(
  "retains completed findings through a later middleware %s",
  (fault) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<undefined>();
      const failing: MessageMiddleware = {
        ...policy("failing"),
        check: () => {
          if (fault === "failure") {
            return Effect.fail(new MiddlewareError({ reason: "unavailable" }));
          }
          if (fault === "defect") {
            return Effect.die("private exception");
          }
          return Deferred.succeed(started, undefined).pipe(
            Effect.zipRight(Effect.never),
          );
        },
      };
      const firewall = codeConfiguration([
        policy("earlier", [block]),
        failing,
        policy("later", [pass]),
      ]);
      const running = yield* checkMessage({
        ...firewall,
        event: { direction: "outgoing", message: draft },
        context: context,
      }).pipe(Effect.fork);
      if (fault === "timeout") {
        yield* Deferred.await(started);
        yield* TestClock.adjust(settings.middlewareTimeoutMilliseconds);
      }
      const result = yield* Fiber.join(running);
      expect(result.judgment).toBe("reject");
      expect(result.checks.map((entry) => entry.status)).toEqual([
        "completed",
        "unavailable",
        "completed",
      ]);
      expect(JSON.stringify(result)).not.toContain("private exception");
    }),
);
it("marks unrun entries unavailable after the total deadline", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<undefined>();
    let laterRan = false;
    const waiting: MessageMiddleware = {
      ...policy("waiting"),
      check: () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.zipRight(Effect.never),
        ),
    };
    const later: MessageMiddleware = {
      ...policy("later"),
      check: () =>
        Effect.sync(() => {
          laterRan = true;
          return { findings: [pass], expectations: [] };
        }),
    };
    const firewall = codeConfiguration([waiting, later], {
      timeoutMilliseconds: 50,
    });
    const running = yield* checkMessage({
      ...firewall,
      event: { direction: "outgoing", message: draft },
      context: context,
    }).pipe(Effect.fork);
    yield* Deferred.await(started);
    yield* TestClock.adjust(50);
    const result = yield* Fiber.join(running);
    expect(result.reason).toBe("check-unavailable");
    expect(result.checks.map((entry) => entry.failure?.reason)).toEqual([
      "timeout",
      "timeout",
    ]);
    expect(laterRan).toBe(false);
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
      new MiddlewareError({ reason: "unavailable" }),
    ).pipe(Effect.ensuring(Effect.interrupt)),
  },
])("preserves self-interruption with $name", ({ cancellation }) =>
  Effect.gen(function* () {
    let laterRan = false;
    const first: MessageMiddleware = {
      ...policy("cancel"),
      check: () => cancellation,
    };
    const later: MessageMiddleware = {
      ...policy("later"),
      check: () =>
        Effect.sync(() => {
          laterRan = true;
          return { findings: [pass], expectations: [] };
        }),
    };
    const firewall = codeConfiguration([first, later]);
    const running = yield* checkMessage({
      ...firewall,
      event: { direction: "outgoing", message: draft },
      context: context,
    }).pipe(Effect.fork);
    const exit = yield* Fiber.await(running);
    expect(
      Exit.match(exit, {
        onFailure: Cause.isInterrupted,
        onSuccess: () => false,
      }),
    ).toBe(true);
    expect(laterRan).toBe(false);
  }),
);
it.each(["private-history", "withheld-history", "private-fact", "draft"])(
  "rejects an unsafe code-policy subject: %s",
  (subject) =>
    Effect.gen(function* () {
      const firewall = codeConfiguration([
        policy(
          "code",
          [pass],
          [{ ruleId: rule.id, expectation: "required", subjects: [subject] }],
        ),
      ]);
      const result = yield* checkMessage({
        ...firewall,
        event: { direction: "outgoing", message: draft },
        context: {
          ...context,
          history: [
            ...context.history,
            {
              ...incoming,
              id: "private-history",
              visibility: "assessor",
              admission: "admitted",
            },
            {
              ...incoming,
              id: "withheld-history",
              visibility: "runtime",
              admission: "withheld",
            },
          ],
        },
      });
      expect(result.reason).toBe("check-unavailable");
      expect(result.checks[0]?.failure?.reason).toBe("invalid-result");
      expect(result.guidance).toEqual([]);
    }),
);
it("rejects missing or unknown rule findings and unregistered advice", () =>
  Effect.gen(function* () {
    const extra = {
      findings: [pass],
      expectations: [],
      guidance: "Unregistered text",
    };
    const variants: MessageMiddleware[] = [
      policy("missing", []),
      policy("unknown", [{ ruleId: "unknown", outcome: "pass" }]),
      { ...policy("advice"), check: () => Effect.succeed(extra) },
    ];
    for (const middleware of variants) {
      const firewall = codeConfiguration([middleware]);
      const result = yield* checkMessage({
        ...firewall,
        event: { direction: "outgoing", message: draft },
        context: context,
      });
      expect(result.reason).toBe("check-unavailable");
      expect(result.checks[0]?.failure?.reason).toBe("invalid-result");
    }
  }));
it("admits an unsettled registered judgment with its advice", () =>
  Effect.gen(function* () {
    const unsettled: PolicyFinding = {
      ruleId: rule.id,
      outcome: "pass",
      reason: "uncertain",
      choice: "none",
      probability: 0.3,
    };
    const firewall = codeConfiguration([policy("advice", [unsettled])]);
    const result = yield* checkMessage({
      ...firewall,
      event: { direction: "outgoing", message: draft },
      context: context,
    });
    expect(result.judgment).toBe("allow");
    expect(result.guidance).toContainEqual({
      middlewareId: "advice",
      ruleId: rule.id,
      instruction: rule.guidance,
      uncertain: { choice: "none", probability: 0.3 },
    });
  }));
it("rejects incoming violations with registered local advice", () =>
  Effect.gen(function* () {
    const firewall = codeConfiguration([policy("rule", [block])]);
    const result = yield* checkMessage({
      ...firewall,
      event: { direction: "incoming", message: incoming },
      context: context,
    });
    expect(result.reason).toBe("rule-violation");
    expect(result.guidance).toContainEqual({
      middlewareId: "rule",
      ruleId: rule.id,
      instruction: rule.guidance,
    });
  }));
it("holds opposing expectations about the same subject", () =>
  Effect.gen(function* () {
    const firewall = codeConfiguration([
      policy(
        "respond",
        [pass],
        [
          {
            ruleId: rule.id,
            expectation: "required",
            subjects: [incoming.id],
          },
        ],
      ),
      policy(
        "wait",
        [pass],
        [{ ruleId: rule.id, expectation: "wait", subjects: [incoming.id] }],
      ),
    ]);
    const result = yield* checkMessage({
      ...firewall,
      event: { direction: "incoming", message: incoming },
      context: context,
    });
    expect(result.reason).toBe("unresolved-evidence");
    expect(result.guidance).toEqual([]);
  }));
it("holds missing context and publishes no incoming advice", () =>
  Effect.gen(function* () {
    const firewall = codeConfiguration([
      policy(
        "respond",
        [pass],
        [
          {
            ruleId: rule.id,
            expectation: "required",
            subjects: [incoming.id],
          },
        ],
      ),
    ]);
    const result = yield* checkMessage({
      ...firewall,
      event: { direction: "incoming", message: incoming },
      context: {
        ...context,
        gaps: ["Unknown sender relationship"],
      },
    });
    expect(result.reason).toBe("unresolved-evidence");
    expect(result.guidance).toEqual([]);
  }));
it("projects registered seek-approval feedback", () =>
  Effect.gen(function* () {
    const feedback = {
      text: "Ask your owner with ask_user before committing.",
      nextAction: "seek-approval" as const,
    };
    const firewall = codeConfiguration([
      {
        ...policy("owner-consent", [{ ruleId: rule.id, outcome: "violation" }]),
        rules: [{ ...rule, feedback }],
      },
    ]);
    const result = yield* checkMessage({
      ...firewall,
      event: { direction: "outgoing", message: draft },
      context: context,
    });
    expect(result.judgment).toBe("reject");
    expect(result.feedback).toEqual([feedback]);
  }));
it("bounds input and results without converting skipped checks into passes", () =>
  Effect.gen(function* () {
    let ran = false;
    const code: MessageMiddleware = {
      ...policy("code"),
      check: () =>
        Effect.sync(() => {
          ran = true;
          return { findings: [pass], expectations: [] };
        }),
    };
    const smallInput = codeConfiguration([code], { maximumInputBytes: 1 });
    expect(
      (yield* checkMessage({
        ...smallInput,
        event: { direction: "outgoing", message: draft },
        context: context,
      })).reason,
    ).toBe("check-unavailable");
    expect(ran).toBe(false);
    const smallResult = codeConfiguration([code], { maximumResultBytes: 1 });
    expect(
      (yield* checkMessage({
        ...smallResult,
        event: { direction: "outgoing", message: draft },
        context: context,
      })).reason,
    ).toBe("check-unavailable");
  }));
it("allows an empty chain while retaining input and evidence validation", () =>
  Effect.gen(function* () {
    const firewall = codeConfiguration([]);
    expect(
      (yield* checkMessage({
        ...firewall,
        event: { direction: "outgoing", message: draft },
        context: context,
      })).judgment,
    ).toBe("allow");
    expect(
      (yield* checkMessage({
        ...firewall,
        event: { direction: "incoming", message: incoming },
        context: context,
      })).judgment,
    ).toBe("allow");
    expect(
      (yield* checkMessage({
        ...firewall,
        event: {
          direction: "outgoing",
          message: { ...draft, sender: "other" },
        },
        context: context,
      }).pipe(Effect.flip)).reason,
    ).toBe("message");
  }));
it("rejects ambiguous registrations", () =>
  Effect.gen(function* () {
    for (const chain of [
      [policy("same"), policy("same")],
      [{ ...policy("rules"), rules: [rule, rule] }],
    ]) {
      expect(
        (yield* validateChecks({
          ...codeConfiguration(chain),
          incoming: chain,
          outgoing: chain,
        }).pipe(Effect.flip)).reason,
      ).toBe("registration");
    }
  }));
it("rejects ambiguous references, wrong attribution and duplicate recipients", () =>
  Effect.gen(function* () {
    const firewall = codeConfiguration([policy("code")]);
    const duplicate = yield* checkMessage({
      ...firewall,
      event: { direction: "outgoing", message: draft },
      context: {
        ...context,
        history: [...context.history, ...context.history],
      },
    }).pipe(Effect.flip);
    const forged = yield* checkMessage({
      ...firewall,
      event: {
        direction: "outgoing",
        message: { ...draft, sender: "owner" },
      },
      context: context,
    }).pipe(Effect.flip);
    const otherEndpoint = yield* checkMessage({
      ...firewall,
      event: {
        direction: "incoming",
        message: { ...incoming, recipients: ["elsewhere"] },
      },
      context: context,
    }).pipe(Effect.flip);
    const recipients = yield* checkMessage({
      ...firewall,
      event: {
        direction: "outgoing",
        message: { ...draft, recipients: ["peer", "peer"] },
      },
      context: context,
    }).pipe(Effect.flip);
    expect(duplicate.reason).toBe("context");
    expect(forged.reason).toBe("message");
    expect(otherEndpoint.reason).toBe("message");
    expect(recipients.reason).toBe("message");
  }));

const budgetEvidence = "PRIVATE: unrelated medical appointment at 14:00";
type Answer = DecisionResponse["answers"][string];
const budgetAcceptance: TextRule = {
  id: "proposal-acceptance",
  applies: "both",
  text: "Availability is not acceptance. Everyone must accept the same concrete proposal before it is finalized.",
  guidance:
    "After proposing a time, wait for every required participant to accept that proposal.",
  feedback: {
    text: "Ask for acceptance of the chosen proposal. Do not claim agreement yet.",
    nextAction: "revise",
  },
};
const budgetRules: readonly TextRule[] = [
  {
    id: "privacy",
    applies: "both",
    text: "Do not disclose unrelated private appointments.",
  },
  budgetAcceptance,
];
const budgetSettings: typeof TextPolicySettings.Encoded = {
  minimumProbability: 0.5,
  maximumRequestBytes: 60000,
  timeoutMilliseconds: 100,
};
/**
 * A provider's answers: each question keyed in `choices` by
 * `stage:ruleId:purpose[:subject]` takes that choice with certainty, or the
 * full answer given there, and every other question its default choice.
 */
function budgetResponse(
  request: DecisionRequest,
  choices: Readonly<Record<string, string | Answer>> = {},
): typeof DecisionResponse.Encoded {
  return {
    provider: "test-decisions",
    model: "test-model",
    raw: budgetEvidence,
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

function budgetProvider(
  choices: Readonly<Record<string, string | Answer>> = {},
  requests: DecisionRequest[] = [],
): DecisionProvider {
  return {
    decide: (request) =>
      Effect.sync(() => {
        requests.push(request);
        return budgetResponse(request, choices);
      }),
  };
}
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
    settings,
    textSettings: options.settings,
    revision: "a".repeat(64),
  };
  return validateChecks({ ...configuration, incoming: [], outgoing: [] }).pipe(
    Effect.zipRight(
      checkMessage({
        ...configuration,
        provider: decisions,
        code: [],
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
  documents: readonly TextRule[] = budgetRules,
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
        settings: { ...budgetSettings, maximumRequestBytes },
      }),
    { concurrency: 1 },
  );
}
function requestBytes(request: DecisionRequest): number {
  return new TextEncoder().encode(JSON.stringify(request)).byteLength;
}
it.each([
  budgetRules,
  budgetRules.map(
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
      const choices = {
        [`expectations:${budgetAcceptance.id}:assessment`]: "unresolved",
      };
      const captured: DecisionRequest[] = [];
      yield* checkMinimumRequests(
        budgetProvider(choices, captured),
        Number.MAX_SAFE_INTEGER,
        documents,
      );
      const minimumBytes = Math.max(...captured.map(requestBytes));
      const results = yield* checkMinimumRequests(
        budgetProvider(choices),
        minimumBytes,
        documents,
      );
      expect(results.map((result) => result.judgment)).toEqual([
        "allow",
        "allow",
      ]);
      const requests: DecisionRequest[] = [];
      const rejected = yield* validateChecks({
        rules: documents,
        textSettings: {
          ...budgetSettings,
          maximumRequestBytes: minimumBytes - 1,
        },
        settings: settings,
        revision: "a".repeat(64),
        incoming: [],
        outgoing: [],
      }).pipe(Effect.flip);
      expect(rejected.reason).toBe("registration");
      expect(requests).toEqual([]);
    }),
);
it.each([
  {
    content: "documents",
    documents: [
      {
        id: budgetAcceptance.id,
        applies: budgetAcceptance.applies,
        text: "x".repeat(60000),
      },
    ],
    fittingBudget: 70000,
  },
  {
    content: "mandatory questions",
    documents: EffectArray.makeBy(100, (index) => ({
      id: `rule-${index}`,
      applies: budgetAcceptance.applies,
      text: "Follow the timetable.",
    })),
    fittingBudget: 200000,
  },
])(
  "rejects oversized $content before calling the provider",
  ({ documents, fittingBudget }) =>
    Effect.gen(function* () {
      const requests: DecisionRequest[] = [];
      const registration = {
        id: "owner",
        rules: documents,
        provider: budgetProvider({}, requests),
      };
      yield* validateChecks({
        ...registration,
        textSettings: { ...budgetSettings, maximumRequestBytes: fittingBudget },
        settings: settings,
        revision: "a".repeat(64),
        incoming: [],
        outgoing: [],
      });
      const rejected = yield* validateChecks({
        ...registration,
        textSettings: budgetSettings,
        settings: settings,
        revision: "a".repeat(64),
        incoming: [],
        outgoing: [],
      }).pipe(Effect.flip);
      expect(rejected.reason).toBe("registration");
      expect(requests).toEqual([]);
    }),
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore assertion checks after public contract cases. */
