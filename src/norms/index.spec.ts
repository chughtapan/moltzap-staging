/** @file A norms directory loads as the contracts of its norm documents, and the structure check built from them passes exactly the acts they permit. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc } from "effect";
import { join } from "node:path";
import { expect } from "vitest";
import type { AgentCard } from "../identity.js";
import { Message, type MsgId } from "../communication/index.js";
import { compareAscii } from "../encoding.js";
import { peer } from "../firewall/__tests__/firewall-fixtures.js";
import {
  contractOf,
  form,
  noFields,
  valueField,
} from "./__tests__/norms-fixtures.js";
import { type Act, formatAct } from "./contract.js";
import { loadNorms, structureCheck } from "./index.js";

/** A document of the norm `name`, whose one act opens and ends an instance. */
const documentOf = (name: string) =>
  `(norm ${name} :initiator Asker :participant Answerer)
(content ask :by Asker :starts true :ends true)
`;

const normName = fc.stringMatching(/^[a-z][a-z0-9]{0,7}$/u);

/** A directory holding `files`, by name. */
function directoryWith(files: Readonly<Record<string, string>>) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({
      prefix: "sharness-norms-",
    });
    yield* Effect.forEach(
      Object.entries(files),
      ([file, text]) => fs.writeFileString(join(directory, file), text),
      { concurrency: 1 },
    );
    return directory;
  });
}

it.prop(
  "loads the contract of every .lisp document directly in a directory, and nothing of its other files",
  [fc.uniqueArray(normName, { maxLength: 4 }), fc.uniqueArray(normName)],
  ([norms, others]) =>
    Effect.gen(function* () {
      const directory = yield* directoryWith({
        ...Object.fromEntries(
          others.map((name) => [`${name}.json`, "not a norm document"]),
        ),
        ...Object.fromEntries(
          norms.map((name) => [`${name}.lisp`, documentOf(name)]),
        ),
      });

      const loaded = yield* loadNorms(directory);

      expect(
        loaded.contracts.map((contract) => contract.source).sort(compareAscii),
      ).toEqual(norms.map(documentOf).sort(compareAscii));
    }).pipe(Effect.provide(NodeContext.layer)),
  { fastCheck: { numRuns: 20 } },
);

it("refuses a directory that holds a document that does not compile or two documents of one norm", () =>
  Effect.forEach(
    [
      { "a.lisp": documentOf("ask"), "b.lisp": documentOf("ask") },
      { "a.lisp": documentOf("ask").replace(":by Asker", ":by Absent") },
      { "a.lisp": `${documentOf("ask")}(` },
    ],
    (files: Readonly<Record<string, string>>) =>
      Effect.gen(function* () {
        const directory = yield* directoryWith(files);

        const failure = yield* Effect.flip(loadNorms(directory));

        expect(failure._tag).toBe("NormsConfigurationError");
      }),
    { concurrency: 1, discard: true },
  ).pipe(Effect.provide(NodeContext.layer)));

const alice = peer("alice", 1);
const bob = peer("bob", 2);

/** `open` starts an instance, `close` ends it, and `reject` answers a withheld act. */
const exchange = contractOf(
  "exchange",
  [
    {
      ...form,
      act: "open",
      by: ["Sender"],
      fields: valueField,
      starts: true,
      initiates: ["opened"],
    },
    {
      ...form,
      act: "close",
      by: ["Receiver"],
      fields: noFields,
      when: ["opened"],
      ends: true,
    },
    {
      ...form,
      act: "reject",
      by: ["Sender", "Receiver"],
      fields: {
        type: "object",
        properties: { reason: { type: "string" } },
        required: ["reason"],
      },
      when: ["opened"],
    },
  ],
  { onRefusal: "reject" },
);

/** Two one-act instances, with a rule for each act and one for both. */
const duo = contractOf(
  "duo",
  ["ask", "tell"].map((act) => ({
    ...form,
    act,
    by: ["Sender"],
    fields: noFields,
    starts: true,
    ends: true,
  })),
  {
    textRules: [
      { id: "duo-rule-1", act: "ask", text: "Ask politely." },
      { id: "duo-rule-2", act: "tell", text: "Tell briefly." },
      { id: "duo-rule-3", text: "Stay on the subject." },
    ],
  },
);

/** A direct message of `text` from `author` to `recipient`. */
function direct(
  author: AgentCard,
  recipient: AgentCard,
  text: string,
  replyToMsgId?: MsgId,
) {
  return Message.create({
    author,
    recipients: [recipient],
    content: [{ type: "text", text }],
    replyToMsgId,
  });
}

const actText = (name: string, norm: string, fields: Act["fields"] = {}) =>
  formatAct({ name, norm, fields });

it.prop(
  "refuses every text that is not an act of a held norm",
  [fc.string({ minLength: 1 })],
  ([text]) =>
    Effect.gen(function* () {
      const result = yield* structureCheck({ contracts: [exchange] })(
        yield* direct(alice, bob, text),
        [],
      );

      expect(result).toMatchObject({
        kind: "refused",
        reason: "rule-violation",
        ruleId: "norm-structural",
        detail: { stage: "structural" },
      });
    }),
  { fastCheck: { numRuns: 60 } },
);

it.each(["ask", "tell"])(
  "passes the act %s with the rules its norm gives that act and every act",
  (act) =>
    Effect.gen(function* () {
      const result = yield* structureCheck({ contracts: [duo] })(
        yield* direct(alice, bob, actText(act, "duo")),
        [],
      );

      expect(result).toMatchObject({
        kind: "passed",
        textRules: [
          { id: act === "ask" ? "duo-rule-1" : "duo-rule-2", applies: "both" },
          { id: "duo-rule-3", applies: "both" },
        ],
        protocol: { norm: "duo", act },
      });
    }),
);

it.prop(
  "continues an instance only from an opening act the firewall allowed",
  [
    fc.constantFrom(
      { checkedAt: 1, allowed: true },
      { checkedAt: 1, allowed: false },
      { allowed: false },
    ),
    fc.string(),
  ],
  ([checked, value]) =>
    Effect.gen(function* () {
      const open = yield* direct(
        alice,
        bob,
        actText("open", "exchange", { value }),
      );

      const result = yield* structureCheck({ contracts: [exchange] })(
        yield* direct(bob, alice, actText("close", "exchange"), open.msgId),
        [{ message: open, ...checked }],
      );

      expect(result.kind).toBe(checked.allowed ? "passed" : "refused");
    }),
  { fastCheck: { numRuns: 12 } },
);

it.prop(
  "answers a message withheld in an instance, by its own refusal or a later one, with the norm's refusal act",
  [
    fc.oneof(
      fc.string({ minLength: 1 }),
      fc.constantFrom(
        actText("unknown", "exchange"),
        actText("close", "exchange", { extra: "unasked" }),
        actText("open", "exchange", { value: "again" }),
        actText("close", "other"),
        actText("close", "exchange"),
      ),
    ),
    fc.string({ minLength: 1 }),
  ],
  ([text, feedback]) =>
    Effect.gen(function* () {
      const open = yield* direct(
        alice,
        bob,
        actText("open", "exchange", { value: "first" }),
      );

      const result = yield* structureCheck({ contracts: [exchange] })(
        yield* direct(bob, alice, text, open.msgId),
        [{ message: open, checkedAt: 1, allowed: true }],
      );

      expect(
        result.kind === "refused" ? result.reply : result.replyTo?.(feedback),
      ).toBe(
        actText("reject", "exchange", {
          reason: result.kind === "refused" ? result.feedback : feedback,
        }),
      );
    }),
  { fastCheck: { numRuns: 20 } },
);
