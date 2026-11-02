/** @file Explicit rule files preserve bounded reads, containment and safe indexed failures. */
import { live as it } from "@effect/vitest";
import { Effect, Option } from "effect";
import { spawnSync } from "node:child_process";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Tests create exact owner files and special-file fixtures around bounded loading.
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect } from "vitest";
import { readCredential } from "../harness/config.js";
import { loadMessagePolicy, MAXIMUM_BEHAVIOR_BYTES } from "./rules.js";
const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Rule fields, policy entry paths and startup phases are the owner policy contract. */

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

function stateDirectory() {
  const directory = mkdtempSync(join(tmpdir(), "sharness-config-files-"));
  directories.push(directory);
  return directory;
}

function loaded(path: string) {
  return loadMessagePolicy({ messagePolicyFile: path }, readCredential).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.die("expected a configured message policy"),
        onSome: Effect.succeed,
      }),
    ),
  );
}

it(
  "accepts a behavior at the 64 KiB byte limit",
  Effect.fn(function* () {
    const { path } = fixture(BEHAVIOR.padEnd(MAXIMUM_BEHAVIOR_BYTES, " "));
    const { policy } = yield* loaded(path);
    expect(policy.text?.fixedRules).toContainEqual(
      expect.objectContaining({ id: "working-hours", text: BODY }),
    );
  }),
);

it(
  "reads YAML block scalars and CRLF frontmatter",
  Effect.fn(function* () {
    const text = NORM.replace(
      "guidance: Ask first.",
      "guidance: |\n      Ask first.\n      Wait for permission.",
    ).replaceAll("\n", "\r\n");
    const { path } = fixture(BEHAVIOR, text);
    const { policy } = yield* loaded(path);
    expect(policy.text?.initialRules).toContainEqual(
      expect.objectContaining({
        id: "ask-first",
        guidance: "Ask first.\nWait for permission.\n",
      }),
    );
  }),
);

it("attributes an invalid inline seed to initialRules before reading behaviors", () =>
  Effect.gen(function* () {
    const { path, document } = fixture();
    writeFileSync(
      path,
      JSON.stringify({
        ...document,
        text: {
          ...document.text,
          initialRules: [
            { id: INITIAL.id, applies: "invalid", text: INITIAL.text },
          ],
          fixedBehaviors: ["absent"],
        },
      }),
    );
    const error = yield* loaded(path).pipe(Effect.flip);
    expect(error.message).toContain("invalid at text.initialRules");
    expect(error.behaviorReason).toBeUndefined();
  }));

const INVALID_BEHAVIORS = [
  [
    "custom tag",
    "frontmatter",
    "fixed",
    BEHAVIOR.replace(
      "license: MIT",
      "license: !private-invalid-behavior secret",
    ),
  ],
  [
    "binary tag",
    "frontmatter",
    "fixed",
    BEHAVIOR.replace("license: MIT", "license: !!binary c2VjcmV0"),
  ],
  ["missing directory", "missing", "absent", BEHAVIOR],
  ["missing file", "missing", ".", BEHAVIOR],
  ["absolute path", "schema", "/private-owner-directory", BEHAVIOR],
  ["Windows absolute path", "schema", "C:\\private-owner-directory", BEHAVIOR],
  ["parent traversal", "schema", "fixed/../fixed", BEHAVIOR],
  ["missing frontmatter", "frontmatter", "fixed", "private-invalid-behavior"],
  [
    "invalid YAML",
    "frontmatter",
    "fixed",
    "---\nname: [private-invalid-behavior\n---\nText",
  ],
  [
    "missing name",
    "schema",
    "fixed",
    BEHAVIOR.replace("name: working-hours\n", ""),
  ],
  [
    "missing description",
    "schema",
    "fixed",
    BEHAVIOR.replace(
      "description: When the agent may start conversations.\n",
      "",
    ),
  ],
  [
    "unknown harness key",
    "schema",
    "fixed",
    BEHAVIOR.replace(
      "    applies: both",
      "    private-invalid-behavior: true\n    applies: both",
    ),
  ],
  [
    "missing metadata",
    "schema",
    "fixed",
    "---\nname: valid\ndescription: Valid\n---\nText",
  ],
  [
    "invalid direction",
    "schema",
    "fixed",
    BEHAVIOR.replace("applies: both", "applies: private-invalid-behavior"),
  ],
  [
    "invalid feedback",
    "schema",
    "fixed",
    BEHAVIOR.replace(
      "nextAction: wait",
      "nextAction: private-invalid-behavior",
    ),
  ],
  ["empty body", "schema", "fixed", BEHAVIOR.replace(BODY, "")],
  [
    "oversized file",
    "too-large",
    "fixed",
    BEHAVIOR.padEnd(MAXIMUM_BEHAVIOR_BYTES + 1, " "),
  ],
  [
    "duplicate fixed inline id",
    "duplicate-id",
    "fixed",
    BEHAVIOR.replace("name: working-hours", "name: inline-fixed"),
  ],
  [
    "duplicate YAML key",
    "frontmatter",
    "fixed",
    BEHAVIOR.replace(
      "name: working-hours",
      "name: working-hours\nname: secret",
    ),
  ],
] as const;

describe.each(["fixedBehaviors", "initialBehaviors"] as const)(
  "%s",
  (field) => {
    // @agent-code-guard/regression-only: Every case is an owner configuration refusal with a safe indexed path at daemon startup.
    it.each(INVALID_BEHAVIORS)(
      "fails configuration for %s, naming only its policy path",
      ([, reason, directory, text]) =>
        Effect.gen(function* () {
          const { path, document } = fixture(text);
          writeFileSync(
            path,
            JSON.stringify({
              ...document,
              text: {
                ...document.text,
                fixedBehaviors: [],
                initialBehaviors: [],
                [field]: [directory],
              },
            }),
          );
          const error = yield* loaded(path).pipe(Effect.flip);
          expect(error.behaviorReason).toBe(reason);
          expect(error.message).toContain(`text.${field}.0`);
          expect(error.message).not.toContain("private-invalid-behavior");
        }),
    );
  },
);

it(
  "refuses repeated fixed behaviors at the second entry",
  Effect.fn(function* () {
    const { path, document } = fixture();
    writeFileSync(
      path,
      JSON.stringify({
        ...document,
        text: { ...document.text, fixedBehaviors: ["fixed", "fixed"] },
      }),
    );
    const error = yield* loaded(path).pipe(Effect.flip);
    expect(error.message).toContain("text.fixedBehaviors.1");
  }),
);

it.each(["", "\uFEFF"])(
  "accepts frontmatter with leading prefix %j",
  (prefix) =>
    Effect.gen(function* () {
      const { path } = fixture(prefix + BEHAVIOR);
      const { policy } = yield* loaded(path);
      expect(policy.text?.fixedRules).toContainEqual(
        expect.objectContaining({ id: "working-hours", text: BODY }),
      );
    }),
);

it.each(["directory", "file"] as const)(
  "refuses a behavior %s symlink escaping the policy directory",
  (target) =>
    Effect.gen(function* () {
      const { path, root } = fixture();
      const outside = stateDirectory();
      writeFileSync(join(outside, "BEHAVIOR.md"), BEHAVIOR);
      if (target === "directory") {
        rmSync(join(root, "fixed"), { recursive: true });
        symlinkSync(outside, join(root, "fixed"));
      } else {
        rmSync(join(root, "fixed", "BEHAVIOR.md"));
        symlinkSync(
          join(outside, "BEHAVIOR.md"),
          join(root, "fixed", "BEHAVIOR.md"),
        );
      }
      const error = yield* loaded(path).pipe(Effect.flip);
      expect(error.behaviorReason).toBe("schema");
      expect(error.message).toContain("text.fixedBehaviors.0");
    }),
);

it("refuses a BEHAVIOR.md that is a directory", () =>
  Effect.gen(function* () {
    const { path, root } = fixture();
    rmSync(join(root, "fixed", "BEHAVIOR.md"));
    mkdirSync(join(root, "fixed", "BEHAVIOR.md"));
    const error = yield* loaded(path).pipe(Effect.flip);
    expect(error.behaviorReason).toBe("not-a-file");
    expect(error.message).toContain("text.fixedBehaviors.0");
  }));

it(
  "refuses a BEHAVIOR.md FIFO without waiting for a writer",
  (context) =>
    Effect.gen(function* () {
      const { path, root } = fixture();
      const behavior = join(root, "fixed", "BEHAVIOR.md");
      rmSync(behavior);
      // eslint-disable-next-line sonarjs/no-os-command-from-path -- The fixture uses the host's mkfifo when available and passes only a generated temporary path.
      const result = spawnSync("mkfifo", [behavior]);
      if (
        result.error !== undefined &&
        "code" in result.error &&
        result.error.code === "ENOENT"
      ) {
        context.skip();
        return;
      }
      expect(result.status).toBe(0);
      const error = yield* loaded(path).pipe(
        Effect.flip,
        Effect.timeout("1 second"),
      );
      expect(error.behaviorReason).toBe("not-a-file");
    }),
  2_000,
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after configuration contract assertions. */
