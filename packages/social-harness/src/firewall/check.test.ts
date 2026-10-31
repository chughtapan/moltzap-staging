/** @file Captured evidence and text assessment budgets preserve safe boundary failures. */
import { effect as it } from "@effect/vitest";
import {
  Deferred,
  Effect,
  Array as EffectArray,
  Fiber,
  TestClock,
} from "effect";
import { expect } from "vitest";
import type {
  FirewallSettings,
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
  validateChecks,
} from "./check.js";
/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Judgments and safe boundary failures are the behavior under test. */
// @agent-code-guard/regression-only: Captured evidence, configured text limits and inference deadlines protect concrete boundary failures.

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
  maximumInputBytes: 100000,
  maximumResultBytes: 100000,
};
it("allows an empty rule set while retaining input and evidence validation", () =>
  Effect.gen(function* () {
    const firewall = { rules: [], revision: "fixture", settings };
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
it("rejects ambiguous references, wrong attribution and duplicate recipients", () =>
  Effect.gen(function* () {
    const firewall = { rules: [], revision: "fixture", settings };
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
      }).pipe(Effect.flip);
      expect(rejected.reason).toBe("rules");
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
      const configuration = {
        id: "owner",
        rules: documents,
        provider: budgetProvider({}, requests),
      };
      yield* validateChecks({
        ...configuration,
        textSettings: { ...budgetSettings, maximumRequestBytes: fittingBudget },
        settings: settings,
        revision: "a".repeat(64),
      });
      const rejected = yield* validateChecks({
        ...configuration,
        textSettings: budgetSettings,
        settings: settings,
        revision: "a".repeat(64),
      }).pipe(Effect.flip);
      expect(rejected.reason).toBe("rules");
      expect(requests).toEqual([]);
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
      rules: budgetRules,
      revision: "fixture",
      provider,
      settings: { ...settings, timeoutMilliseconds: 10 },
      textSettings: { ...budgetSettings, timeoutMilliseconds: 10_000 },
      event: { direction: "outgoing", message: draft },
      context,
    }).pipe(Effect.fork);
    yield* Deferred.await(started);
    yield* TestClock.adjust("10 millis");
    const result = yield* Fiber.join(running);
    expect(result).toMatchObject({
      judgment: "hold",
      reason: "check-unavailable",
    });
    expect(result.findings).toEqual(
      budgetRules.map((rule) => ({
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
        rules: budgetRules,
        revision: "fixture",
        provider: budgetProvider(),
        settings: { ...settings, ...bound },
        textSettings: budgetSettings,
        event: { direction: "outgoing", message: draft },
        context,
      });
      expect(result).toMatchObject({
        judgment: "hold",
        reason: "check-unavailable",
        guidance: [],
      });
    }),
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore assertion checks after public contract cases. */
