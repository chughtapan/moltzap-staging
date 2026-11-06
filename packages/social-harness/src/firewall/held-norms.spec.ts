/** @file An agent's editable policy holds norm documents: seeded, edited under approval, compiled at each activation and kept across restarts. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc, Option, Schema } from "effect";
import { join } from "node:path";
import { expect } from "vitest";
import type { AgentCard } from "../identity/agent-cards.js";
import type { Chain, Matcher } from "./chain.js";
import type { NormDocuments } from "./editable-policy.js";
import { Message } from "../communication/messages.js";
import { makeIdentityFixture } from "../identity/__tests__/authentication-fixtures.js";
import {
  evidence,
  peer,
  policy,
  withFirewall,
} from "./__tests__/firewall-fixtures.js";
import {
  FirewallPolicyError,
  MAXIMUM_DOCUMENT_BYTES,
  type policyUpdateSchema,
  TextRule,
} from "./rules.js";
import { Firewall, type FirewallOptions } from "./service.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Norm names, rule ids and closed edit failures are the editable policy contract under test. */

const fakeDocument = Schema.parseJson(
  Schema.Struct({ name: Schema.String, rule: Schema.optional(Schema.String) }),
);

/** A document of the fake norms: a norm's name and an optional English rule. */
function document(name: string, rule?: string): string {
  return JSON.stringify(rule === undefined ? { name } : { name, rule });
}

function read(text: string) {
  return Schema.decodeUnknownOption(fakeDocument)(text);
}

/** Matches a draft whose whole text is the name of a norm the agent holds. */
function heldName(names: readonly string[]): Matcher {
  return {
    match: ({ message }) =>
      Effect.succeed({
        matched:
          "content" in message &&
          message.content.some(
            (part) => part.type === "text" && names.includes(part.text),
          ),
      }),
  };
}

/**
 * Norm documents the way a norms layer reads them: a document names its norm
 * unless it is unreadable or names the built-in `feedback`, and the held norms
 * compile into one matcher and a fixed rule per English rule, unless one of
 * them is `clash`.
 */
const norms: NormDocuments = {
  initial: [document("chat")],
  name: (text) =>
    read(text).pipe(
      Option.map(({ name }) => name),
      Option.filter((name) => name !== "feedback"),
    ),
  compile: (documents) => {
    const held = documents.flatMap((text) => Option.toArray(read(text)));
    return held.some(({ name }) => name === "clash")
      ? Option.none()
      : Option.some({
          matchers: { "norms.held": heldName(held.map(({ name }) => name)) },
          fixedRules: held.flatMap(({ name, rule }) =>
            rule === undefined
              ? []
              : [
                  new TextRule({
                    id: `${name}-rule-1`,
                    applies: "both",
                    text: rule,
                  }),
                ],
          ),
        });
  },
};

/** Refuses a draft whose text names a held norm, so a test sees which norms are active. */
const chain: Chain = {
  rules: [
    { id: "held", applies: "outgoing", match: "norms.held", action: "deny" },
  ],
  matchers: {},
};

const swap = document("swap", "Ask for a swap a day ahead.");

/** The default test policy with an approval step that refuses every edit. */
const refusing: FirewallOptions = {
  ...policy(),
  approvePolicyUpdate: () =>
    Effect.fail(new FirewallPolicyError({ reason: "approval-refused" })),
};

/** A firewall whose agent holds the fake norms under the default test policy. */
function holding(agentKey: string) {
  return Firewall.layer(agentKey, policy(), chain, norms);
}

function draft(self: AgentCard, text: string) {
  return Message.create({
    author: self,
    recipients: [peer("bob", 2)],
    content: [{ type: "text", text }],
  });
}

it("seeds a new agent with the initial norms, and keeps an edit across a restart", () =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    const filesystem = yield* FileSystem.FileSystem;
    const directory = yield* filesystem.makeTempDirectoryScoped({
      prefix: "sharness-norms-",
    });
    const filename = join(directory, "harness.sqlite3");

    const seeded = yield* withFirewall(
      filename,
      fixture,
      (firewall) =>
        Effect.gen(function* () {
          const before = (yield* firewall.readPolicy()).norms;
          yield* firewall.updatePolicy({
            remove: [{ norm: "chat" }],
            add: [{ norm: swap }],
          });
          return before;
        }),
      holding,
    );
    const restored = yield* withFirewall(
      filename,
      fixture,
      (firewall) => firewall.readPolicy(),
      holding,
    );

    expect(seeded).toEqual(["chat"]);
    expect(restored.norms).toEqual(["swap"]);
  }).pipe(Effect.provide(NodeContext.layer)));

it("installs an added norm's matchers and English rules, and removes them with it", () =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    yield* withFirewall(
      ":memory:",
      fixture,
      (firewall, self) =>
        Effect.gen(function* () {
          const ask = yield* draft(self, "swap");
          const before = yield* firewall.checkOutgoing(ask, evidence);

          yield* firewall.updatePolicy({ add: [{ norm: swap }] });
          const added = yield* firewall.readPolicy();
          const held = yield* firewall.checkOutgoing(ask, evidence);
          const documents = yield* firewall.readNorms();
          yield* firewall.updatePolicy({ remove: [{ norm: "swap" }] });
          const after = yield* firewall.checkOutgoing(ask, evidence);

          expect(before.kind).toBe("allowed");
          expect(added.norms).toEqual(["chat", "swap"]);
          expect(added.fixedRules.map((rule) => rule.id)).toEqual([
            "swap-rule-1",
          ]);
          expect(held).toMatchObject({
            kind: "refused",
            refusal: { rule: "held" },
          });
          expect(documents).toEqual([document("chat"), swap]);
          expect(after.kind).toBe("allowed");
        }),
      holding,
    );
  }));

it("replaces a norm that an update removes and adds", () =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    const replacement = document("chat", "Keep chat brief.");

    const documents = yield* withFirewall(
      ":memory:",
      fixture,
      (firewall) =>
        firewall
          .updatePolicy({
            remove: [{ norm: "chat" }],
            add: [{ norm: replacement }],
          })
          .pipe(Effect.zipRight(firewall.readNorms())),
      holding,
    );

    expect(documents).toEqual([replacement]);
  }));

it.prop(
  "holds a document once its norms name it, and keeps the active norms for any other",
  [
    fc.oneof(
      fc.string(),
      fc.string().map((name) => document(name)),
    ),
  ],
  ([text]) =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const name = norms.name(text);
      const accepted = Option.isSome(name) && name.value !== "chat";

      const documents = yield* withFirewall(
        ":memory:",
        fixture,
        (firewall) =>
          firewall
            .updatePolicy({ add: [{ norm: text }] })
            .pipe(Effect.ignore, Effect.zipRight(firewall.readNorms())),
        holding,
      );

      expect(documents).toEqual(
        accepted ? [document("chat"), text] : [document("chat")],
      );
    }),
  { fastCheck: { numRuns: 24 } },
);

const refusedEdits: ReadonlyArray<{
  readonly what: string;
  readonly edit: typeof policyUpdateSchema.Type;
  readonly refusal: Readonly<Record<string, unknown>>;
}> = [
  {
    what: "a document naming a built-in norm",
    edit: { add: [{ norm: document("feedback") }] },
    refusal: {
      reason: "invalid-rule",
      problem: "invalid-norm",
      path: ["add", 0],
    },
  },
  {
    what: "a norm the agent holds already",
    edit: { add: [{ norm: document("chat") }] },
    refusal: {
      reason: "invalid-rule",
      problem: "duplicate-id",
      path: ["add", 0],
    },
  },
  {
    what: "a norm the agent does not hold",
    edit: { remove: [{ norm: "absent" }] },
    refusal: {
      reason: "invalid-rule",
      problem: "unknown-id",
      path: ["remove", 0],
    },
  },
  {
    what: "norms that do not compile together",
    edit: { add: [{ norm: document("clash") }] },
    refusal: { reason: "build-failed" },
  },
  {
    what: "a document over the size limit",
    edit: {
      add: [
        { norm: document("large").padEnd(MAXIMUM_DOCUMENT_BYTES + 1, " ") },
      ],
    },
    refusal: {
      reason: "invalid-rule",
      problem: "too-large",
      path: ["add", 0],
    },
  },
];

it.each(refusedEdits)(
  "refuses $what and keeps the active norms",
  ({ edit, refusal }) =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;

      const result = yield* withFirewall(
        ":memory:",
        fixture,
        (firewall) =>
          Effect.gen(function* () {
            const before = yield* firewall.readPolicy();
            const error = yield* firewall.updatePolicy(edit).pipe(Effect.flip);
            return { before, error, after: yield* firewall.readPolicy() };
          }),
        holding,
      );

      expect(result.error).toMatchObject(refusal);
      expect(result.after).toEqual(result.before);
    }),
);

it("keeps the active norms when the host refuses an update", () =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;

    const result = yield* withFirewall(
      ":memory:",
      fixture,
      (firewall) =>
        Effect.gen(function* () {
          const error = yield* firewall
            .updatePolicy({ add: [{ norm: swap }] })
            .pipe(Effect.flip);
          return { error, after: yield* firewall.readPolicy() };
        }),
      (agentKey) => Firewall.layer(agentKey, refusing, chain, norms),
    );

    expect(result.error.reason).toBe("approval-refused");
    expect(result.after.norms).toEqual(["chat"]);
  }));

it("refuses a norm on a firewall without norm documents", () =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;

    const error = yield* withFirewall(
      ":memory:",
      fixture,
      (firewall) =>
        firewall.updatePolicy({ add: [{ norm: swap }] }).pipe(Effect.flip),
      (agentKey) => Firewall.layer(agentKey, policy()),
    );

    expect(error).toMatchObject({ problem: "invalid-norm" });
  }));

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore default assertion rules. */
