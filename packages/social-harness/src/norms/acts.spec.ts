/** @file FIPA acts read from and write to their string form. */

import { FastCheck as fc } from "effect";
import { describe, expect, it } from "vitest";
import { formatAct, readText } from "./acts.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Act parameters and performatives are the FIPA contract under test. */

describe("readText", () => {
  it("reads the envelope parameters and every other parameter as a field", () => {
    expect(
      readText(
        '(propose :ontology group-meeting :in-reply-to msg_1 :length "30 minutes" :free (1 3) :option 2)',
      ),
    ).toEqual({
      kind: "act",
      act: {
        performative: "propose",
        ontology: "group-meeting",
        inReplyTo: "msg_1",
        fields: { length: "30 minutes", free: ["1", "3"], option: 2 },
      },
    });
  });

  it("reads text that does not open with a performative as plain text", () => {
    expect(readText("(just kidding) Tuesday works.")).toEqual({
      kind: "text",
      text: "(just kidding) Tuesday works.",
    });
  });

  it.each([
    { what: "an unclosed act", text: "(cfp :ontology group-meeting" },
    {
      what: "a parameter without a value",
      text: "(cfp :ontology group-meeting :length)",
    },
    {
      what: "a repeated parameter",
      text: "(inform :ontology chat :ontology chat)",
    },
    { what: "a missing ontology", text: '(inform :content "hello")' },
    {
      what: "two acts in one message",
      text: "(inform :ontology chat) (inform :ontology chat)",
    },
  ])("refuses $what", ({ text }) => {
    expect(readText(text).kind).toBe("invalid");
  });
});

describe("formatAct", () => {
  it("writes acts that read back unchanged", () => {
    const text = fc.string().filter((value) => !/\\[uU]/u.test(value));
    fc.assert(
      fc.property(
        text,
        fc.array(text, { maxLength: 3 }),
        fc.integer(),
        (text, items, number) => {
          const act = {
            performative: "inform",
            ontology: "norm-discovery",
            inReplyTo: "msg_1",
            content: text,
            fields: { norms: text, items, count: number },
          };
          const read = readText(formatAct(act));
          expect(read).toEqual({ kind: "act", act });
        },
      ),
    );
  });

  it("writes a text ending in a backslash so it reads back", () => {
    const act = {
      performative: "inform",
      ontology: "chat",
      content: "C:\\",
      fields: {},
    };
    expect(readText(formatAct(act))).toEqual({ kind: "act", act });
  });
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore default assertion rules. */
