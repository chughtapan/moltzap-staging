/** @file Editable policies retain fixed-rule protection and complete state across restart. */
import { live as it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Tests create exact operator configuration and rule files around real persistent edits.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { expect } from "vitest";
import { testAgentView } from "../__tests__/agent-card-fixtures.js";
import {
  stateDirectory,
  withDaemonStore,
} from "../__tests__/store-schema-fixtures.js";
import {
  decodeRuntimeValue,
  encodeRuntimeValue,
} from "../communication/store/index.js";
import { readCredential } from "../daemon/credentials.js";
import { Message, MessageContext, type MessageMiddleware } from "./check.js";
import {
  loadMessagePolicy,
  MAXIMUM_BEHAVIOR_BYTES,
  policyInstallationError,
} from "./config.js";
import {
  type FirewallOptions,
  makeFirewall,
  type PreparedFirewallCheck,
} from "./index.js";
import {
  readFirewallPolicy,
  saveCheckDecision,
  writeFirewallPolicy,
} from "./store/index.js";
/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Indexed safe errors and atomic policy state are the public edit contract. */

// @agent-code-guard/regression-only: Construction and restart regressions protect fixed rules, immutable provenance and retained-result privacy.

const FORMAT = "org.social-harness/message-policy/v2";

const BODY = "Start conversations during working hours.";

const FIXED = {
  id: "inline-fixed",
  applies: "both",
  text: "Protect owner data.",
} as const;

const INITIAL = {
  id: "inline-initial",
  applies: "both",
  text: "Be brief.",
  guidance: "Keep it brief.",
} as const;

const BEHAVIOR = `---
name: working-hours
description: When the agent may start conversations.
license: MIT
metadata:
  another-consumer: ignored
  social-harness:
    applies: both
    feedback:
      text: Wait until working hours.
      nextAction: wait
---

${BODY}

`;

const NORM = `---
name: ask-first
description: Ask before starting a conversation.
metadata:
  social-harness:
    applies: both
    guidance: Ask first.
---
Ask before starting a conversation.
`;

const SHARED = `---
name: meeting-booking
description: How members book a meeting.
metadata:
  social-harness:
    applies: both
    guidance: Hold a slot before you confirm one.
    feedback:
      text: Hold the slot first.
      nextAction: revise
---

Each member holds a slot before confirming it.
`;

/** Rule-count cases leave enough request budget to isolate the editable-document bound. */
function seedFixture(count: number) {
  const subject = fixture();
  writeFileSync(
    subject.path,
    JSON.stringify({
      ...subject.document,
      textSettings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 1_000_000,
        timeoutMilliseconds: 15_000,
      },
      text: {
        ...subject.document.text,
        initialRules: Array.from({ length: count }, () => INITIAL).map(
          (rule, index) => ({
            ...rule,
            id: `seed-${index}`,
          }),
        ),
      },
    }),
  );
  return subject;
}

function fixture(fixed = BEHAVIOR, initial = NORM) {
  const root = stateDirectory();
  mkdirSync(join(root, "fixed"));
  mkdirSync(join(root, "initial"));
  writeFileSync(join(root, "fixed", "BEHAVIOR.md"), fixed);
  writeFileSync(join(root, "initial", "BEHAVIOR.md"), initial);
  const path = join(root, "policy.json");
  const document = {
    format: FORMAT,
    mode: "guidance",
    decisionProvider: { kind: "unavailable" },
    text: {
      fixedRules: [FIXED],
      initialRules: [INITIAL],
      fixedBehaviors: ["fixed"],
      initialBehaviors: ["initial"],
    },
  };
  writeFileSync(path, JSON.stringify(document));
  return { root, path, document };
}
/** File loading supplies the same validated input as daemon startup. */
function options(path: string) {
  return loadMessagePolicy({ messagePolicyFile: path }, readCredential).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.dieMessage("expected configured policy"),
        onSome: Effect.succeed,
      }),
    ),
  );
}

it(
  "appends fixed and initial behaviors to inline rules and exposes them through policy_read",
  Effect.fn(function* () {
    const { path } = fixture();
    const { policy } = yield* options(path);
    expect(policy.text?.fixedRules).toEqual([
      FIXED,
      {
        id: "working-hours",
        applies: "both",
        text: BODY,
        feedback: { text: "Wait until working hours.", nextAction: "wait" },
      },
    ]);
    expect(policy.text?.initialRules).toEqual([
      INITIAL,
      {
        id: "ask-first",
        applies: "both",
        text: "Ask before starting a conversation.",
        guidance: "Ask first.",
      },
    ]);
    const configuration = yield* options(path);
    yield* withDaemonStore(stateDirectory(), () =>
      Effect.gen(function* () {
        const installed = yield* makeFirewall(configuration);
        const read = yield* installed.readPolicy();
        expect(read.fixedRules).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ id: FIXED.id }),
            expect.objectContaining({ id: "working-hours" }),
          ]),
        );
        expect(read.rules).toMatchObject(policy.text?.initialRules ?? []);
        const error = yield* installed
          .updatePolicy({ remove: ["working-hours"] })
          .pipe(Effect.flip);
        expect(error).toMatchObject({
          reason: "fixed-rule",
          ruleId: "working-hours",
          path: ["remove", 0],
        });
        expect(yield* installed.readPolicy()).toEqual(read);
      }),
    );
  }),
);

it(
  "keeps an existing editable document when initial behaviors change",
  Effect.fn(function* () {
    const { path, root } = fixture();
    const state = stateDirectory();
    const first = yield* options(path);
    yield* withDaemonStore(state, () =>
      Effect.gen(function* () {
        const installed = yield* makeFirewall(first);
        yield* installed.updatePolicy({ remove: [INITIAL.id, "ask-first"] });
      }),
    );
    writeFileSync(
      join(root, "initial", "BEHAVIOR.md"),
      NORM.replace("Ask first.", "Consult the owner first."),
    );
    const next = yield* options(path);
    yield* withDaemonStore(state, () =>
      Effect.gen(function* () {
        const restarted = yield* makeFirewall(next);
        expect((yield* restarted.readPolicy()).rules).toEqual([]);
      }),
    );
    writeFileSync(
      join(root, "initial", "BEHAVIOR.md"),
      "private-invalid-behavior",
    );
    const error = yield* options(path).pipe(Effect.flip);
    expect(error.message).toContain("text.initialBehaviors.0");
  }),
);

it(
  "seeds 255 inline initial rules and one initial behavior",
  Effect.fn(function* () {
    const { path } = seedFixture(255);
    const configuration = yield* options(path);
    yield* withDaemonStore(stateDirectory(), () =>
      Effect.gen(function* () {
        const installed = yield* makeFirewall(configuration);
        expect((yield* installed.readPolicy()).rules).toHaveLength(256);
      }),
    );
  }),
);

it(
  "names the initial behavior that exceeds the editable rule limit",
  Effect.fn(function* () {
    const { path } = seedFixture(256);
    const error = yield* options(path).pipe(Effect.flip);
    expect(error.behaviorReason).toBe("editable-limit");
    expect(error.message).toContain("text.initialBehaviors.0");
  }),
);

it("names the fixed behavior colliding with a stored editable rule", () =>
  Effect.gen(function* () {
    const { path, root } = fixture();
    const state = stateDirectory();
    const configuration = yield* options(path);
    yield* withDaemonStore(
      state,
      Effect.fn(function* () {
        const installed = yield* makeFirewall(configuration);
        yield* installed.updatePolicy({
          add: [{ ...FIXED, id: "new-fixed" }],
        });
      }),
    );
    writeFileSync(
      join(root, "fixed", "BEHAVIOR.md"),
      BEHAVIOR.replace("name: working-hours", "name: new-fixed"),
    );
    const installation = yield* options(path);
    yield* withDaemonStore(
      state,
      Effect.fn(function* () {
        const error = yield* makeFirewall(installation).pipe(Effect.flip);
        const startup = policyInstallationError(
          error,
          installation.behaviorRulePaths,
        );
        expect(error.reason).toBe("fixed-rule");
        expect(startup.message).toContain(
          "invalid at text.fixedBehaviors.0 (duplicate-id)",
        );
        expect(startup.message).toContain("Docs:");
        expect(startup.behaviorReason).toBe("duplicate-id");
      }),
    );
  }));

it(
  "adds a shared norm's BEHAVIOR.md text as the rule its frontmatter and body map to",
  Effect.fn(function* () {
    const configuration = yield* options(fixture().path);
    yield* withDaemonStore(stateDirectory(), () =>
      Effect.gen(function* () {
        const installed = yield* makeFirewall(configuration);
        yield* installed.updatePolicy({ add: [{ behavior: SHARED }] });
        expect((yield* installed.readPolicy()).rules).toContainEqual({
          id: "meeting-booking",
          applies: "both",
          text: "Each member holds a slot before confirming it.",
          guidance: "Hold a slot before you confirm one.",
          feedback: { text: "Hold the slot first.", nextAction: "revise" },
        });
      }),
    );
  }),
);

/** Edits the fixture's editable rules (`inline-initial`, `ask-first`) cannot apply, and what each refusal names. */
const REFUSED_EDITS = [
  {
    edit: { add: [INITIAL] },
    refusal: {
      reason: "invalid-rule",
      problem: "duplicate-id",
      ruleId: INITIAL.id,
      path: ["add", 0],
    },
  },
  {
    edit: {
      add: [
        { ...INITIAL, id: "twice" },
        { ...INITIAL, id: "twice" },
      ],
    },
    refusal: {
      reason: "invalid-rule",
      problem: "duplicate-id",
      ruleId: "twice",
      path: ["add", 1],
    },
  },
  {
    edit: { remove: ["absent"] },
    refusal: {
      reason: "invalid-rule",
      problem: "unknown-id",
      ruleId: "absent",
      path: ["remove", 0],
    },
  },
  {
    edit: {
      add: [...Array.from({ length: 255 }).keys()].map((index) => ({
        ...INITIAL,
        id: `extra-${String(index)}`,
      })),
    },
    refusal: { reason: "invalid-rule", problem: "editable-limit" },
  },
  {
    edit: { add: [{ ...INITIAL, id: FIXED.id }] },
    refusal: { reason: "fixed-rule", ruleId: FIXED.id, path: ["add", 0] },
  },
  {
    edit: {
      add: [{ behavior: "Each member holds a slot before confirming it." }],
    },
    refusal: {
      reason: "invalid-rule",
      problem: "frontmatter",
      path: ["add", 0],
    },
  },
  {
    edit: {
      add: [
        {
          behavior: SHARED.replace("    applies: both\n", ""),
        },
      ],
    },
    refusal: { reason: "invalid-rule", problem: "schema", path: ["add", 0] },
  },
  {
    edit: {
      add: [{ behavior: SHARED.padEnd(MAXIMUM_BEHAVIOR_BYTES + 1, " ") }],
    },
    refusal: { reason: "invalid-rule", problem: "too-large", path: ["add", 0] },
  },
] as const;

it.each(REFUSED_EDITS)(
  "refuses an edit, naming its failing rule and problem, and keeps the policy: $refusal",
  ({ edit, refusal }) =>
    refusedEdit(edit).pipe(
      Effect.tap(({ error, before, after }) =>
        Effect.sync(() => {
          expect(error).toMatchObject(refusal);
          expect(error.message).toContain("Docs: ");
          expect(after).toEqual(before);
        }),
      ),
    ),
);

/** The refusal `policy_update` gives `edit` on the fixture's policy, with `policy_read` before and after it. */
function refusedEdit(edit: (typeof REFUSED_EDITS)[number]["edit"]) {
  return Effect.gen(function* () {
    const configuration = yield* options(fixture().path);
    return yield* withDaemonStore(stateDirectory(), () =>
      Effect.gen(function* () {
        const installed = yield* makeFirewall(configuration);
        const before = yield* installed.readPolicy();
        const error = yield* installed.updatePolicy(edit).pipe(Effect.flip);
        return { error, before, after: yield* installed.readPolicy() };
      }),
    );
  });
}

it("persists replacement of an initial rule across restart", () =>
  Effect.gen(function* () {
    const configuration = yield* options(fixture().path);
    const directory = stateDirectory();
    const replacement = { ...INITIAL, text: "Ask before booking a meeting." };
    const { policy, summaries } = yield* replaceAndReopen(
      directory,
      configuration,
      replacement,
    );
    expect(policy.rules).toContainEqual(replacement);
    expect(summaries.activations).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ addedRuleIds: [replacement.id] }),
      ]),
    );
  }));
function replaceAndReopen(
  directory: string,
  configuration: Parameters<typeof makeFirewall>[0],
  replacement: Omit<typeof INITIAL, "text"> & { readonly text: string },
) {
  return Effect.gen(function* () {
    yield* withDaemonStore(directory, () =>
      Effect.gen(function* () {
        const installed = yield* makeFirewall(configuration);
        yield* installed.updatePolicy({
          remove: [INITIAL.id],
          add: [replacement],
        });
      }),
    );
    return yield* withDaemonStore(directory, () =>
      Effect.gen(function* () {
        const reopened = yield* makeFirewall(configuration);
        return {
          policy: yield* reopened.readPolicy(),
          summaries: yield* reopened.readDecisions({}),
        };
      }),
    );
  });
}

it("omits provider traces from the completed result while preserving its judgment", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const trace = { format: "test-provider/v1", data: "private response" };
      const middleware: MessageMiddleware = {
        id: "privacy",
        revision: "v1",
        rules: [{ id: "privacy" }],
        check: () =>
          Effect.succeed({
            findings: [{ ruleId: "privacy", outcome: "violation" }],
            expectations: [],
            trace,
          }),
      };
      const firewall = yield* makeFirewall(codeOptions(middleware));
      const [checked] = yield* firewall.check([yield* preparedCheck()]);
      const completed = yield* Effect.fromNullable(checked);
      expect(completed.result.judgment).toBe("reject");
      expect(completed.result.checks[0]?.trace).toEqual(trace);
      expect(completed.completion.admit).toBe(false);
      yield* saveCheckDecision(completed.completion);
      const feed = yield* firewall.readDecisions({});
      expect(feed.decisions[0]).toMatchObject({
        outcome: "reject",
        cause: "code",
        decidedBy: [{ ruleId: "privacy", from: "code" }],
      });
      expect(JSON.stringify(feed)).not.toContain(trace.data);
    }),
  ));

it("captures immutable installation provenance and distinguishes provider model changes", () =>
  withDaemonStore(stateDirectory(), () =>
    Effect.gen(function* () {
      const registration = {
        id: "model-policy",
        revision: "v1",
        rules: [{ id: "privacy" }],
        provenance: { format: "provider", data: "model-one" },
        check: () =>
          Effect.succeed({
            findings: [{ ruleId: "privacy", outcome: "violation" as const }],
            expectations: [],
          }),
      };
      const first = yield* makeFirewall(codeOptions(registration));
      registration.provenance.data = "model-two";
      const second = yield* makeFirewall(codeOptions(registration));
      const candidate = yield* preparedCheck();
      const original = yield* retainedInstallation(first, candidate);
      const changed = yield* retainedInstallation(second, candidate);
      expect(original.installation.incoming[0]?.provenance.data).toBe(
        "model-one",
      );
      expect(changed.installation.incoming[0]?.provenance.data).toBe(
        "model-two",
      );
      expect(original.configurationDigest).not.toBe(
        changed.configurationDigest,
      );
    }),
  ));

function retainedInstallation(
  firewall: Effect.Effect.Success<ReturnType<typeof makeFirewall>>,
  candidate: PreparedFirewallCheck,
) {
  return Effect.gen(function* () {
    const [checked] = yield* firewall.check([candidate]);
    return yield* decodeRuntimeValue(
      Schema.Struct({
        configurationDigest: Schema.String,
        installation: Schema.Struct({
          incoming: Schema.Array(
            Schema.Struct({
              provenance: Schema.Struct({ data: Schema.String }),
            }),
          ),
        }),
      }),
      (yield* Effect.fromNullable(checked)).completion.canonicalInput,
    );
  });
}

function codeOptions(middleware: MessageMiddleware): FirewallOptions {
  return {
    incoming: [middleware],
    outgoing: [middleware],
    mode: "enforcement",
    facts: [],
    codeDigest: "a".repeat(64),
    settings: {
      timeoutMilliseconds: 1_000,
      middlewareTimeoutMilliseconds: 100,
      maximumInputBytes: 100_000,
      maximumResultBytes: 100_000,
    },
  };
}

function preparedCheck() {
  return Effect.gen(function* () {
    const local = testAgentView("alice");
    const peer = testAgentView("bob");
    const message = new Message({
      id: "draft",
      sender: local.agentId,
      recipients: [peer.agentId],
      text: "Hello",
    });
    return {
      direction: "outgoing",
      message,
      agents: [local, peer],
      canonicalCandidate: yield* encodeRuntimeValue(message),
      context: new MessageContext({
        agentId: local.agentId,
        principal: local.principalId,
        participants: [{ agentId: peer.agentId, principal: peer.principalId }],
        history: [],
        facts: [],
        gaps: [],
      }),
    } satisfies PreparedFirewallCheck;
  });
}

it("refuses unknown stored editable properties as persistence failures", () =>
  Effect.gen(function* () {
    const configuration = yield* options(fixture().path);
    yield* withDaemonStore(stateDirectory(), () =>
      refuseUnknownStoredProperty(configuration),
    );
  }));

function refuseUnknownStoredProperty(
  configuration: Parameters<typeof makeFirewall>[0],
) {
  return Effect.gen(function* () {
    yield* makeFirewall(configuration);
    const reopened = yield* makeFirewall(configuration);
    expect((yield* reopened.readPolicy()).rules).toContainEqual(INITIAL);
    const bytes = yield* Effect.fromNullable(yield* readFirewallPolicy());
    const saved = yield* decodeRuntimeValue(
      Schema.Struct({
        format: Schema.String,
        rules: Schema.Array(
          Schema.Record({ key: Schema.String, value: Schema.Unknown }),
        ),
      }),
      bytes,
    );
    const first = yield* Effect.fromNullable(saved.rules[0]);
    yield* writeFirewallPolicy(
      yield* encodeRuntimeValue({
        ...saved,
        rules: [{ ...first, unexpected: true }, ...saved.rules.slice(1)],
      }),
    );
    expect(yield* Effect.flip(makeFirewall(configuration))).toMatchObject({
      reason: "persistence-failed",
    });
  });
}
/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after policy edit contract assertions. */
