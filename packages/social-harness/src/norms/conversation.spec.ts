/** @file The acts a member may send now, read from a conversation's norm state. */

import { live as it } from "@effect/vitest";
import { Effect, Either } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- The tests read the shipped norm documents once, before any effect runs.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect } from "vitest";
import { readText } from "./acts.js";
import {
  foldActs,
  type SendableAct,
  sendableActs,
  type SentAct,
  type Setting,
} from "./conversation.js";
import { readNorm } from "./documents.js";

const directory = fileURLToPath(
  new URL("../../config/norms/", import.meta.url),
);

const held = [
  "built-in/norm-agreement.lisp",
  "built-in/norm-discovery.lisp",
  "built-in/feedback.lisp",
  "examples/chat.lisp",
  "examples/office-hours.lisp",
].map((file) =>
  Either.getOrThrow(readNorm(readFileSync(join(directory, file), "utf8"))),
);

const setting: Setting = {
  held: new Map(held.map((norm) => [norm.name, norm])),
  members: ["professor", "student"],
};

const proposal =
  '(propose :ontology norm-agreement :norm office-hours :task "Meet")';
const agreed = [
  ["student", proposal],
  ["professor", "(accept-proposal :ontology norm-agreement)"],
] as const;

/** A conversation's history, each act beside the member that sent it, and what one member may send next. */
interface Case {
  readonly state: string;
  readonly history: ReadonlyArray<readonly [string, string]>;
  readonly sender: string;
  readonly acts: readonly SendableAct[];
}

/** The acts of a history, in order. */
function sent(history: Case["history"]): readonly SentAct[] {
  return history.flatMap(([sender, text], index) => {
    const reading = readText(text);
    return reading.kind === "act"
      ? [{ act: reading.act, sender, msgId: `msg_${index}` }]
      : [];
  });
}

describe("sendableActs", () => {
  // @agent-code-guard/regression-only: each case pins the acts one conversation state allows a member to send.
  it.each<Case>([
    {
      state: "nothing is in flight",
      history: [],
      sender: "student",
      acts: [
        {
          performative: "propose",
          ontology: "norm-agreement",
          norm: "office-hours",
        },
        {
          performative: "query-ref",
          ontology: "norm-discovery",
          norm: "office-hours",
        },
      ],
    },
    {
      state: "another member's proposal waits for an answer",
      history: [["professor", proposal]],
      sender: "student",
      acts: [
        { performative: "accept-proposal", ontology: "norm-agreement" },
        { performative: "reject-proposal", ontology: "norm-agreement" },
        { performative: "cancel", ontology: "norm-agreement" },
      ],
    },
    {
      state: "the member's own proposal waits for an answer",
      history: [["student", proposal]],
      sender: "student",
      acts: [{ performative: "cancel", ontology: "norm-agreement" }],
    },
    {
      state: "the members agreed on a norm",
      history: agreed,
      sender: "student",
      acts: [{ performative: "request", ontology: "office-hours" }],
    },
    {
      state: "the norm's task is in flight",
      history: [
        ...agreed,
        ["student", '(request :ontology office-hours :topic "Project scope")'],
        [
          "professor",
          '(propose :ontology office-hours :option1 "Tue 14:00" :option2 "Thu 10:00")',
        ],
      ],
      sender: "student",
      acts: [
        { performative: "accept-proposal", ontology: "office-hours" },
        { performative: "reject-proposal", ontology: "office-hours" },
        { performative: "cancel", ontology: "office-hours" },
      ],
    },
  ])("names what a member may send when $state", ({ history, sender, acts }) =>
    Effect.gen(function* () {
      const state = yield* foldActs(setting, sent(history));

      expect(sendableActs(setting, state, sender)).toEqual(acts);
    }),
  );
});
