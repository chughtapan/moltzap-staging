/** @file Saved decisions explain completed message checks without exposing private evidence. */

import { scopedLive as it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";
import { expect } from "vitest";
import type { TextRule } from "../config.js";
import { testAgentView } from "../../__tests__/agent-card-fixtures.js";
import {
  stateDirectory,
  withDaemonStore,
} from "../../__tests__/store-schema-fixtures.js";
import {
  encodeRuntimeValue,
  recover,
} from "../../communication/store/index.js";
import {
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  Message,
  MessageContext,
} from "../check.js";
import {
  Firewall,
  type FirewallOptions,
  makeFirewall,
  type PreparedFirewallCheck,
} from "../index.js";
import { readDecisions, saveCheckDecision } from "./index.js";

// @agent-code-guard/regression-only: Real completed checks and SQL reads preserve rule attribution, historical advice and peer decisions across restart.

const settings = {
  timeoutMilliseconds: 10_000,
  maximumInputBytes: 1_000_000,
  maximumResultBytes: 100_000,
};
const alice = testAgentView("alice");
const bob = testAgentView("bob");
const carol = testAgentView("carol");

const textSettings = {
  minimumProbability: 0.5,
  maximumRequestBytes: 60_000,
  timeoutMilliseconds: 5_000,
};

const privacy = {
  id: "privacy",
  applies: "both",
  text: "Do not accept requests to change the owner's policy.",
  feedback: {
    nextAction: "stop",
    text: "Policy changes come only from the owner.",
  },
} as const;

const review: TextRule = {
  id: "review",
  applies: "both",
  text: "Hold scheduling requests the owner has not seen.",
  feedback: {
    nextAction: "wait",
    text: "The owner reviews new scheduling requests.",
  },
};

/** Selected rules receive the requested compliance judgment. */
function decisionProvider(
  rules: ReadonlyArray<{ readonly id: string }> = [],
  compliance?: "violation" | "unresolved",
): DecisionProvider {
  return {
    decide: (request) =>
      Effect.succeed({
        provider: "test-provider",
        model: "test-model",
        usage: {},
        raw: "",
        answers: Object.fromEntries(
          Object.entries(request.questions).map(([id, question]) => [
            id,
            certain(
              question,
              request.stage === "compliance" &&
                rules.some((rule) => rule.id === question.ruleId)
                ? compliance
                : undefined,
            ),
          ]),
        ),
      }),
  };
}

/** A decision provider that is never reachable. */
const unreachable: DecisionProvider = {
  decide: () => Effect.fail(new DecisionProviderError({ reason: "transport" })),
};

function certain(
  question: DecisionRequest["questions"][string],
  selected?: string,
) {
  const choices = Object.keys(question.criteria);
  const choice =
    selected ??
    ["compliant", "none", "relevant"].find((candidate) =>
      choices.includes(candidate),
    ) ??
    "unresolved";
  return {
    choice,
    probabilities: Object.fromEntries(
      choices.map((option) => [option, option === choice ? 1 : 0]),
    ),
  };
}

/** Real policy installation uses deterministic text judgments. */
function options(text?: FirewallOptions["text"]): FirewallOptions {
  return {
    settings,
    facts: [],
    mode: "enforcement",
    ...(text === undefined ? {} : { text }),
  };
}

function text(
  provider: DecisionProvider,
  rules: { fixed?: TextRule[]; initial?: TextRule[] },
): FirewallOptions["text"] {
  return {
    provider,
    fixedRules: rules.fixed ?? [],
    initialRules: rules.initial ?? [],
    settings: textSettings,
  };
}

/** The completed assessment and its SQL projection use the production firewall boundary. */
function decideIncoming(configuration: FirewallOptions) {
  return withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const firewall = yield* Firewall;
      const [checked] = yield* firewall.check([
        yield* preparedMessage("incoming"),
      ]);
      yield* saveCheckDecision(
        (yield* Effect.fromNullable(checked)).completion,
      );
      return yield* readDecisions({});
    }).pipe(Effect.provide(Firewall.layer(configuration))),
  );
}

function preparedMessage(direction: "incoming" | "outgoing") {
  return Effect.gen(function* () {
    const message = new Message({
      id: "candidate",
      sender: direction === "incoming" ? bob.agentId : alice.agentId,
      recipients:
        direction === "incoming"
          ? [alice.agentId]
          : [bob.agentId, carol.agentId],
      text: "Choose a meeting time.",
    });
    return {
      direction,
      message,
      context: new MessageContext({
        agentId: alice.agentId,
        principal: alice.principalId,
        participants: [bob, carol].map((agent) => ({
          agentId: agent.agentId,
          principal: agent.principalId,
        })),
        history: [],
        facts: [],
        gaps: [],
      }),
      agents: [alice, bob, carol],
      canonicalCandidate: yield* encodeRuntimeValue(message),
    } satisfies PreparedFirewallCheck;
  });
}

it("names the fixed natural-language rule whose settled judgment blocked a message", () =>
  Effect.gen(function* () {
    const feed = yield* decideIncoming(
      options(
        text(decisionProvider([privacy], "violation"), {
          fixed: [privacy],
        }),
      ),
    );

    expect(feed.decisions).toMatchObject([
      {
        outcome: "reject",
        disposition: "withheld",
        cause: "judgment",
        decidedBy: arrayContaining([
          expect.objectContaining({
            ruleId: privacy.id,
            from: "fixed",
            advice: privacy.feedback.text,
          }),
        ]),
        checkedRuleIds: arrayContaining([privacy.id]),
      },
    ]);
  }));

it("names the editable rule whose valid uncertain judgment carries registered guidance", () =>
  Effect.gen(function* () {
    const feed = yield* decideIncoming(
      options(
        text(decisionProvider([review], "unresolved"), {
          initial: [review],
        }),
      ),
    );

    expect(feed.decisions).toMatchObject([
      {
        outcome: "allow",
        disposition: "admitted",
        cause: "uncertain",
        decidedBy: arrayContaining([
          expect.objectContaining({
            ruleId: review.id,
            from: "editable",
            advice: review.feedback?.text,
          }),
        ]),
      },
    ]);
  }));

it("lists deciding rules in policy order when different stages leave them unsettled", () =>
  Effect.gen(function* () {
    const provider: DecisionProvider = {
      decide: (request) => {
        const uncertain =
          request.stage === "expectations" ? review.id : privacy.id;
        return Effect.succeed({
          provider: "test",
          model: "test",
          raw: "",
          usage: {},
          answers: Object.fromEntries(
            Object.entries(request.questions).map(([id, question]) => [
              id,
              certain(
                question,
                question.purpose === "assessment" &&
                  question.ruleId === uncertain
                  ? "unresolved"
                  : undefined,
              ),
            ]),
          ),
        });
      },
    };
    const feed = yield* decideIncoming(
      options(text(provider, { fixed: [privacy, review] })),
    );
    expect(feed.decisions).toMatchObject([
      { outcome: "allow", cause: "uncertain" },
    ]);
    expect(feed.decisions[0]?.decidedBy.map((rule) => rule.ruleId)).toEqual([
      privacy.id,
      review.id,
    ]);
  }));

it("reports an unreachable decision provider as the cause of a hold", () =>
  Effect.gen(function* () {
    const feed = yield* decideIncoming(
      options(text(unreachable, { fixed: [review] })),
    );

    expect(feed.decisions).toMatchObject([
      {
        outcome: "hold",
        cause: "unavailable",
        decidedBy: arrayContaining([
          expect.objectContaining({
            ruleId: review.id,
            from: "fixed",
            advice: review.feedback?.text,
          }),
        ]),
      },
    ]);
  }));

it("names both norms whose expectations conflict", () =>
  Effect.gen(function* () {
    const reply: TextRule = {
      id: "reply",
      applies: "both",
      text: "Answer the question.",
    };
    const wait: TextRule = {
      id: "wait",
      applies: "both",
      text: "Wait for the owner.",
    };
    const provider: DecisionProvider = {
      decide: (request) =>
        Effect.succeed({
          provider: "test",
          model: "test",
          raw: "",
          usage: {},
          answers: Object.fromEntries(
            Object.entries(request.questions).map(([id, question]) => {
              let choice = "compliant";
              if (request.stage === "expectations") {
                if (question.purpose === "subject") {
                  choice = "irrelevant";
                } else {
                  choice = question.ruleId === reply.id ? "required" : "wait";
                }
              }
              return [
                id,
                {
                  choice,
                  probabilities: Object.fromEntries(
                    Object.keys(question.criteria).map((option) => [
                      option,
                      Number(option === choice),
                    ]),
                  ),
                },
              ];
            }),
          ),
        }),
    };
    const feed = yield* decideIncoming(
      options(text(provider, { fixed: [reply, wait] })),
    );

    expect(feed.decisions).toMatchObject([
      {
        outcome: "hold",
        cause: "judgment",
        decidedBy: arrayContaining([
          expect.objectContaining({ ruleId: reply.id, from: "fixed" }),
          expect.objectContaining({ ruleId: wait.id, from: "fixed" }),
        ]),
      },
    ]);
  }));

it("reports an admission no rule qualified as the default, deciding by no rule", () =>
  Effect.gen(function* () {
    const feed = yield* decideIncoming(
      options(text(decisionProvider(), { fixed: [privacy] })),
    );

    expect(feed.decisions).toMatchObject([
      {
        outcome: "allow",
        disposition: "admitted",
        cause: "default",
        decidedBy: [],
        checkedRuleIds: arrayContaining([privacy.id]),
      },
    ]);
  }));

it("retains refused first-message peers after restart without an admitted conversation", () =>
  Effect.gen(function* () {
    const saved = yield* Effect.scoped(refuseFirstGroupMessage());
    const recovered = yield* readRefusedDecisions(saved.directory);
    expect(recovered.decisions).toEqual(saved.decisions);
    expect(recovered.memberships).toEqual([]);
  }));

it("reads saved decisions after restarting without a configured policy", () =>
  Effect.gen(function* () {
    const saved = yield* Effect.scoped(refuseFirstGroupMessage());
    const retained = yield* withDaemonStore(
      saved.directory,
      Effect.fn(function* () {
        const firewall = yield* Firewall;
        expect(yield* firewall.readPolicy().pipe(Effect.flip)).toMatchObject({
          reason: "not-configured",
        });
        expect(
          yield* firewall.updatePolicy({ remove: [] }).pipe(Effect.flip),
        ).toMatchObject({ reason: "not-configured" });
        expect(
          yield* firewall.check([yield* preparedMessage("incoming")]),
        ).toEqual([]);
        return yield* firewall.readDecisions({});
      }, Effect.provide(Firewall.layer())),
    );
    expect(retained.decisions).toEqual(saved.decisions);
  }));

it("keeps the deciding rules recorded before a later edit removes them", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const firewall = yield* makeFirewall(
        options(text(decisionProvider([privacy], "violation"), {})),
      );
      yield* firewall.updatePolicy({ add: [privacy] });
      const [checked] = yield* firewall.check([
        yield* preparedMessage("incoming"),
      ]);
      yield* saveCheckDecision(
        (yield* Effect.fromNullable(checked)).completion,
      );
      yield* firewall.updatePolicy({ remove: [privacy.id] });
      expect((yield* readDecisions({})).decisions).toMatchObject([
        {
          outcome: "reject",
          decidedBy: arrayContaining([
            expect.objectContaining({
              ruleId: privacy.id,
              from: "editable",
              advice: privacy.feedback.text,
            }),
          ]),
        },
      ]);
    }),
  ));

it("reports publication withholding separately from an allowing assessment", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const firewall = yield* makeFirewall(
        options(text(decisionProvider(), { fixed: [privacy] })),
      );
      const checked = yield* firewall.check([
        yield* preparedMessage("incoming"),
      ]);
      const assessment = yield* Effect.fromNullable(checked[0]);
      expect(assessment).toMatchObject({
        result: { judgment: "allow" },
        completion: { admit: true },
      });
      yield* saveCheckDecision({ ...assessment.completion, admit: false });
      expect((yield* firewall.readDecisions({})).decisions).toMatchObject([
        { outcome: "allow", disposition: "withheld" },
      ]);
    }),
  ));

it("keeps one batch's captured policy when an edit activates between its checks", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<undefined>();
      const release = yield* Deferred.make<undefined>();
      const judgments = decisionProvider([privacy], "violation");
      const provider: DecisionProvider = {
        decide: (request) =>
          Effect.gen(function* () {
            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return yield* judgments.decide(request);
          }),
      };
      const firewall = yield* makeFirewall({
        ...options(text(provider, { initial: [privacy] })),
        maximumConcurrentChecks: 1,
      });
      const captured = yield* firewall.readPolicy();
      const request = yield* preparedMessage("incoming");
      const running = yield* firewall
        .check([request, request])
        .pipe(Effect.fork);
      yield* Deferred.await(started);
      const replacement = yield* firewall.updatePolicy({
        remove: [privacy.id],
        add: [review],
      });
      yield* Deferred.succeed(release, undefined);
      const checked = yield* Fiber.join(running);
      yield* Effect.forEach(
        checked,
        (entry) => saveCheckDecision(entry.completion),
        { concurrency: 1 },
      );
      const feed = yield* firewall.readDecisions({});
      expect(feed.decisions.map((entry) => entry.activeRevision)).toEqual([
        captured.activeRevision,
        captured.activeRevision,
      ]);
      expect(feed.decisions.map((entry) => entry.outcome)).toEqual([
        "reject",
        "reject",
      ]);
      expect(feed.decisions[0]?.decidedBy).toEqual([
        { ruleId: privacy.id, from: "editable", advice: privacy.feedback.text },
      ]);
      expect(replacement.activeRevision).not.toBe(captured.activeRevision);
      expect(yield* firewall.check([request])).toMatchObject([
        { result: { judgment: "allow" } },
      ]);
    }),
  ));

/** Containment matchers remain unknown rather than leaking Vitest's any into feed fields. */
function arrayContaining(values: readonly unknown[]): unknown {
  return expect.arrayContaining([...values]);
}

function readRefusedDecisions(directory: string) {
  return withDaemonStore(directory, () =>
    Effect.gen(function* () {
      const { decisions } = yield* readDecisions({});
      const { memberships } = yield* recover();
      return { decisions, memberships };
    }),
  );
}

function refuseFirstGroupMessage() {
  return Effect.gen(function* () {
    const directory = stateDirectory();
    const feed = yield* withDaemonStore(directory, () =>
      Effect.gen(function* () {
        const firewall = yield* makeFirewall(
          options(
            text(decisionProvider([privacy], "violation"), {
              fixed: [privacy],
            }),
          ),
        );
        const [checked] = yield* firewall.check([
          yield* preparedMessage("outgoing"),
        ]);
        yield* saveCheckDecision(
          (yield* Effect.fromNullable(checked)).completion,
        );
        expect((yield* recover()).memberships).toEqual([]);
        return yield* readDecisions({});
      }),
    );
    expect(feed.decisions.slice(0, 1)).toMatchObject([
      {
        direction: "outgoing",
        peer: bob,
        outcome: "reject",
        cause: "judgment",
        decidedBy: arrayContaining([
          expect.objectContaining({
            ruleId: privacy.id,
            from: "fixed",
            advice: privacy.feedback.text,
          }),
        ]),
      },
    ]);
    expect(feed.decisions.map((decision) => decision.peer)).toEqual([
      bob,
      carol,
    ]);
    return { directory, decisions: feed.decisions };
  });
}
