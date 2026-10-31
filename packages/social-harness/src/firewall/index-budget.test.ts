/** @file Installation budgets preserve usable policy and durable state across edits and startup. */

import { scopedLive as it } from "@effect/vitest";
import { Effect, Array as EffectArray, Option } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Configuration regressions compare exact owner-file bytes before and after installation.
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { expect } from "vitest";
import type {
  DecisionProvider,
  DecisionRequest,
  DecisionResponse,
  Message,
} from "./check.js";
import { testAgentView } from "../__tests__/agent-card-fixtures.js";
import {
  stateDirectory,
  withDaemonStore,
} from "../__tests__/store-schema-fixtures.js";
import { encodeRuntimeValue } from "../communication/store/runtime-codec.js";
import { readCredential } from "../daemon/credentials.js";
import {
  decodeMessagePolicy,
  loadMessagePolicy,
  type TextRule,
} from "./config.js";
import {
  type FirewallOperations,
  type FirewallOptions,
  makeFirewall,
  type PreparedFirewallCheck,
} from "./index.js";
import { readFirewallActivations, readFirewallPolicy } from "./store/index.js";

// @agent-code-guard/regression-only: Managed policy edits and checks use real SQLite and loaded documents with deterministic provider judgments.

const ownerSafety: TextRule = {
  id: "owner-privacy",
  applies: "both",
  text: "Protect the owner's private information.",
};
const document = {
  format: "org.social-harness/message-policy/v2",
  mode: "enforcement",
  decisionProvider: { kind: "unavailable" },
  text: { fixedRules: [ownerSafety] },
};
const buildFailure = "build-failed";
const editDocs =
  "Docs: https://docs.moltzap.xyz/reference/spec/firewall/message-policy#editable-text-policy";
const body = "Follow the agreed timetable. " + "n".repeat(60_000);

/** Only the provider is stubbed; question construction and admission remain real. */
function provider(requests: DecisionRequest[]): DecisionProvider {
  return {
    decide: (request) =>
      Effect.sync(() => {
        requests.push(request);
        return {
          provider: "test",
          model: "test",
          raw: "",
          usage: {},
          answers: Object.fromEntries(
            Object.entries(request.questions).map(answer),
          ),
        };
      }),
  };
}

function answer([id, question]: [
  string,
  DecisionRequest["questions"][string],
]): [string, DecisionResponse["answers"][string]] {
  const choice =
    ["compliant", "none", "irrelevant"].find((option) =>
      Object.hasOwn(question.criteria, option),
    ) ?? "unresolved";
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
}

function options(requests: DecisionRequest[], maximumRequestBytes?: number) {
  return Effect.gen(function* () {
    const policy = yield* decodeMessagePolicy(JSON.stringify(document));
    const text = yield* Effect.fromNullable(policy.text);
    return {
      mode: policy.mode,
      facts: policy.facts,
      settings: policy.settings,
      text: {
        fixedRules: text.fixedRules,
        initialRules: text.initialRules,
        settings: {
          ...policy.textSettings,
          maximumRequestBytes:
            maximumRequestBytes ?? policy.textSettings.maximumRequestBytes,
        },
        provider: provider(requests),
      },
    } satisfies FirewallOptions;
  });
}

function behavior(index: number) {
  return `---\nname: shared-${index}\ndescription: The agreed timetable.\nmetadata:\n  social-harness:\n    applies: both\n    guidance: Follow the timetable.\n---\n${body}\n`;
}

function exerciseRuntimeEdit(
  configuration: FirewallOptions,
  requests: DecisionRequest[],
) {
  const directory = stateDirectory();
  return withDaemonStore(directory, () =>
    rejectOversizedEdit(configuration, requests).pipe(
      Effect.map((retained) => ({ directory, ...retained })),
    ),
  );
}

function rejectOversizedEdit(
  configuration: FirewallOptions,
  requests: DecisionRequest[],
) {
  return Effect.gen(function* () {
    const installed = yield* makeFirewall(configuration);
    yield* Effect.forEach(
      EffectArray.range(0, 5),
      (index) =>
        installed.updatePolicy({ add: [{ behavior: behavior(index) }] }),
      { concurrency: 1 },
    );
    const before = yield* installed.readPolicy();
    const bytes = yield* readFirewallPolicy();
    const activations = yield* readFirewallActivations({});

    const rejected = yield* installed
      .updatePolicy({ add: [{ behavior: behavior(6) }] })
      .pipe(Effect.flip);

    expect(rejected.reason).toBe(buildFailure);
    expect(rejected.message).toContain(editDocs);
    expect(yield* installed.readPolicy()).toEqual(before);
    expect(yield* readFirewallPolicy()).toEqual(bytes);
    expect(yield* readFirewallActivations({})).toEqual(activations);
    expect(requests).toEqual([]);
    yield* expectChecks(installed, requests);
    return { before, bytes, activations };
  });
}

function expectChecks(
  installed: FirewallOperations,
  requests: readonly DecisionRequest[],
) {
  return Effect.gen(function* () {
    const first = yield* incomingMessage("first");
    const second = yield* incomingMessage("second");
    const checked = yield* installed.check([first, second]);
    expect(checked.map(({ completion }) => completion.admit)).toEqual([
      true,
      true,
    ]);
    expect(requests.some(hasOwnerSafetyQuestion)).toBe(true);
  });
}

function incomingMessage(id: string) {
  const local = testAgentView("agent:alice");
  const remote = testAgentView("agent:bob");
  const agents = [local, remote];
  const message: Message = {
    id,
    sender: remote.agentId,
    recipients: [local.agentId],
    text: "Does Tuesday work?",
  };
  return encodeRuntimeValue(message).pipe(
    Effect.map(
      (canonicalCandidate) =>
        ({
          direction: "incoming",
          canonicalCandidate,
          message,
          context: {
            agentId: local.agentId,
            principal: local.principalId,
            participants: agents.map((agent) => ({
              agentId: agent.agentId,
              principal: agent.principalId,
            })),
            history: [],
            facts: [],
            gaps: [],
          },
          agents,
        }) satisfies PreparedFirewallCheck,
    ),
  );
}

function hasOwnerSafetyQuestion(request: DecisionRequest): boolean {
  return (
    request.stage === "compliance" &&
    Object.values(request.questions).some(
      (question) => question.ruleId === ownerSafety.id,
    )
  );
}

function expectReopen(
  retained: Effect.Effect.Success<ReturnType<typeof exerciseRuntimeEdit>>,
  configuration: FirewallOptions,
  smallerBudget: FirewallOptions,
) {
  return withDaemonStore(retained.directory, () =>
    Effect.gen(function* () {
      const rejected = yield* makeFirewall(smallerBudget).pipe(Effect.flip);
      expect(rejected.reason).toBe(buildFailure);
      expect(yield* readFirewallPolicy()).toEqual(retained.bytes);
      expect(yield* readFirewallActivations({})).toEqual(retained.activations);
      const reopened = yield* makeFirewall(configuration);
      expect(yield* reopened.readPolicy()).toEqual(retained.before);
    }),
  );
}

it("rejects an oversized edit while prior policy still checks messages and reopens", () =>
  Effect.gen(function* () {
    const requests: DecisionRequest[] = [];
    const configuration = yield* options(requests);
    const retained = yield* exerciseRuntimeEdit(configuration, requests);
    const smallerBudget = yield* options(requests, 60_000);
    yield* expectReopen(retained, configuration, smallerBudget);
  }));

it.each(["fixedRules", "initialRules"] as const)(
  "rejects an intrinsically oversized file-loaded %s rule set without changing file or store",
  rejectRulesFromFile,
);

function rejectRulesFromFile(field: "fixedRules" | "initialRules") {
  return Effect.gen(function* () {
    const path = join(stateDirectory(), "policy.json");
    const contents = JSON.stringify({
      ...document,
      text: {
        [field]: EffectArray.makeBy(7, (index) => ({
          id: `shared-${index}`,
          applies: "both",
          text: body,
          guidance: "Follow the timetable.",
        })),
      },
    });
    writeFileSync(path, contents);
    const loaded = yield* loadMessagePolicy(
      { messagePolicyFile: path },
      readCredential,
    );
    const configuration = yield* Option.match(loaded, {
      onNone: () => Effect.dieMessage("expected loaded policy options"),
      onSome: Effect.succeed,
    });
    yield* withDaemonStore(stateDirectory(), () =>
      Effect.gen(function* () {
        const error = yield* makeFirewall(configuration).pipe(Effect.flip);
        expect(error.reason).toBe(buildFailure);
        expect(yield* readFirewallPolicy()).toBeUndefined();
        expect(readFileSync(path, "utf8")).toBe(contents);
      }),
    );
  });
}
