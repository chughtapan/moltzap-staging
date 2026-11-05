/** @file Norm checks over real messages and conversation history. */

import { live as it } from "@effect/vitest";
import { Effect, Either, Encoding, FastCheck as fc } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- The tests read the shipped norm documents once, before any effect runs.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect } from "vitest";
import type { MatchInput } from "../firewall/chain.js";
import { Message } from "../communication/service.js";
import {
  AgentCard,
  AgentId,
  AgentName,
  PrincipalId,
} from "../identity/agent-cards.js";
import { readText } from "./acts.js";
import { type Norm, readNorm } from "./documents.js";
import { normMatchers } from "./matchers.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Matcher names, breach reasons and acts are the norm contract under test. */

const directory = fileURLToPath(
  new URL("../../config/norms/", import.meta.url),
);

function norms(...files: readonly string[]): readonly Norm[] {
  return files.map((file) =>
    Either.getOrThrow(readNorm(readFileSync(join(directory, file), "utf8"))),
  );
}

const builtIn = [
  "norm-agreement.lisp",
  "norm-discovery.lisp",
  "feedback.lisp",
  "chat.lisp",
];

function card(name: string, byte: number): AgentCard {
  const id = Encoding.encodeBase64Url(new Uint8Array(16).fill(byte));
  return AgentCard.make({
    agentId: AgentId.make(`agt_${id}`),
    principalId: PrincipalId.make(`prn_${id}`),
    agentName: AgentName.make(name),
  });
}

const alice = card("alice", 1);
const bob = card("bob", 2);
const carol = card("carol", 3);
const group = [alice, bob, carol];

const cfp =
  '(cfp :ontology group-meeting :length "30 minutes" :candidates "1 Tue 2pm; 2 Wed 3pm")';

/** Alice proposes group-meeting, and Bob and Carol give `answers` in order. */
function agreement(answers: readonly string[]) {
  return Effect.all([
    from(
      alice,
      group,
      '(propose :ontology norm-agreement :norm group-meeting :task "Plan the review")',
    ),
    ...answers.map((answer, index) =>
      from(
        index === 0 ? bob : carol,
        group,
        `(${answer} :ontology norm-agreement)`,
      ),
    ),
  ]);
}

/** One message from `author` to the other `members`. */
function from(author: AgentCard, members: readonly AgentCard[], text: string) {
  const [first, ...rest] = members.filter((member) => member !== author);
  return first === undefined
    ? Effect.dieMessage("a message needs a recipient")
    : Message.create({
        author,
        recipients: [first, ...rest],
        content: [{ type: "text", text }],
      });
}

function check(
  matcher: string,
  matchers: ReturnType<typeof normMatchers>,
  input: MatchInput,
) {
  const found = matchers[matcher];
  return found === undefined
    ? Effect.dieMessage(`no matcher ${matcher}`)
    : found.match(input);
}

/** What the evidence lacks or holds beyond its ordered history. */
interface EvidenceOptions {
  readonly earlierHistoryOmitted?: boolean;
  readonly pendingOutgoing?: readonly Message[];
}

function incoming(
  self: AgentCard,
  message: Message,
  history: readonly Message[] = [],
  { pendingOutgoing = [] }: EvidenceOptions = {},
): MatchInput {
  return {
    direction: "incoming",
    message,
    self,
    evidence: {
      history: history.map((sent) => ({ message: sent, visibleToAgent: true })),
      earlierHistoryOmitted: false,
      pendingOutgoing,
      gaps: [],
    },
  };
}

function outgoing(
  message: Message,
  history: readonly Message[] = [],
  { earlierHistoryOmitted = false, pendingOutgoing = [] }: EvidenceOptions = {},
): MatchInput {
  return {
    direction: "outgoing",
    message,
    self: message.author,
    evidence: {
      history: history.map((sent) => ({ message: sent, visibleToAgent: true })),
      earlierHistoryOmitted,
      pendingOutgoing,
      gaps: [],
    },
  };
}

function replyOf(result: Effect.Effect.Success<ReturnType<typeof check>>) {
  return result.matched && result.reply !== undefined
    ? readText(result.reply)
    : undefined;
}

describe("norms.unparsed", () => {
  it("answers plain text with not-understood where chat is not held", () =>
    Effect.gen(function* () {
      const matchers = normMatchers(
        norms("norm-agreement.lisp", "norm-discovery.lisp", "feedback.lisp"),
      );
      const message = yield* from(bob, [alice, bob], "Can we meet today?");

      const result = yield* check(
        "norms.unparsed",
        matchers,
        incoming(alice, message),
      );

      expect(result).toMatchObject({
        matched: true,
        detail: { reason: "plain-text" },
      });
      expect(replyOf(result)).toMatchObject({
        kind: "act",
        act: {
          performative: "not-understood",
          ontology: "feedback",
          inReplyTo: message.msgId,
          fields: {
            reason: "plain-text",
            accepts: "norm-agreement norm-discovery feedback",
          },
        },
      });
    }));

  it("passes plain text where chat is held", () =>
    Effect.gen(function* () {
      const message = yield* from(bob, [alice, bob], "Can we meet today?");

      const result = yield* check(
        "norms.unparsed",
        normMatchers(norms(...builtIn)),
        incoming(alice, message),
      );

      expect(result).toEqual({ matched: false });
    }));

  it("reports an act that does not parse, and never answers a not-understood", () =>
    Effect.gen(function* () {
      const matchers = normMatchers(norms(...builtIn));
      const broken = yield* from(bob, [alice, bob], "(cfp :ontology");
      const feedback = yield* from(
        bob,
        [alice, bob],
        "(not-understood :ontology mystery)",
      );

      const brokenResult = yield* check(
        "norms.unparsed",
        matchers,
        incoming(alice, broken),
      );
      const feedbackResult = yield* check(
        "norms.unparsed",
        matchers,
        incoming(alice, feedback),
      );

      expect(brokenResult).toMatchObject({
        matched: true,
        detail: { reason: "unparsed" },
      });
      expect(replyOf(brokenResult)).toMatchObject({
        act: { performative: "not-understood" },
      });
      expect(feedbackResult).toEqual({
        matched: true,
        detail: {
          reason: "unknown-norm",
          accepts: "norm-agreement norm-discovery feedback chat",
        },
      });
    }));
});

describe("norm agreement", () => {
  const matchers = normMatchers(
    norms(...builtIn, "examples/group-meeting.lisp"),
  );

  it("lets a norm start once every member accepts it", () =>
    Effect.gen(function* () {
      const history = yield* agreement(["accept-proposal", "accept-proposal"]);

      const result = yield* check(
        "norms.nonconforming",
        matchers,
        outgoing(yield* from(alice, group, cfp), history),
      );

      expect(result).toEqual({ matched: false });
    }));

  it.each([
    { when: "one member has not answered", answers: ["accept-proposal"] },
    {
      when: "one member rejected it",
      answers: ["accept-proposal", "reject-proposal"],
    },
  ])("refuses to start a norm when $when", ({ answers }) =>
    Effect.gen(function* () {
      const history = yield* agreement(answers);

      const result = yield* check(
        "norms.nonconforming",
        matchers,
        outgoing(yield* from(alice, group, cfp), history),
      );

      expect(result).toMatchObject({
        matched: true,
        detail: { reason: "no-agreement", norm: "group-meeting" },
      });
    }),
  );
});

describe("agreement races, consent and order", () => {
  const matchers = normMatchers(
    norms(...builtIn, "examples/group-meeting.lisp"),
  );

  it("keeps the first of two concurrent proposals and refuses the second", () =>
    Effect.gen(function* () {
      const first = yield* from(
        alice,
        group,
        "(propose :ontology norm-agreement :norm group-meeting)",
      );
      const second = yield* from(
        bob,
        group,
        "(propose :ontology norm-agreement :norm group-meeting)",
      );
      const accept = yield* from(
        carol,
        group,
        "(accept-proposal :ontology norm-agreement)",
      );

      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          incoming(carol, second, [first]),
        ),
      ).toMatchObject({
        matched: true,
        detail: { reason: "in-flight" },
      });
      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(accept, [first, second]),
        ),
      ).toEqual({ matched: false });
    }));

  it("asks the owner to approve a proposal this agent sends, not one it receives", () =>
    Effect.gen(function* () {
      const [proposal] = yield* agreement([]);

      expect(
        yield* check("norms.consent", matchers, outgoing(proposal)),
      ).toEqual({ matched: true });
      expect(
        yield* check("norms.consent", matchers, incoming(bob, proposal)),
      ).toEqual({ matched: false });
    }));

  it("starts a norm only once every member accepts it, in any order", () =>
    Effect.gen(function* () {
      const [proposal] = yield* agreement([]);
      const accept = "(accept-proposal :ontology norm-agreement)";
      const reject = "(reject-proposal :ontology norm-agreement)";
      const answers = {
        bob: {
          accept: yield* from(bob, group, accept),
          reject: yield* from(bob, group, reject),
        },
        carol: {
          accept: yield* from(carol, group, accept),
          reject: yield* from(carol, group, reject),
        },
      };
      const start = yield* from(alice, group, cfp);
      const answer = fc.constantFrom<"accept" | "reject" | "none">(
        "accept",
        "reject",
        "none",
      );

      fc.assert(
        fc.property(
          answer,
          answer,
          fc.boolean(),
          (fromBob, fromCarol, bobFirst) => {
            const bobs = fromBob === "none" ? undefined : answers.bob[fromBob];
            const carols =
              fromCarol === "none" ? undefined : answers.carol[fromCarol];
            const ordered = bobFirst ? [bobs, carols] : [carols, bobs];
            const history: Message[] = [proposal];
            history.push(...ordered.filter((message) => message !== undefined));
            const result = Effect.runSync(
              check("norms.nonconforming", matchers, outgoing(start, history)),
            );
            expect(result.matched).toBe(
              !(fromBob === "accept" && fromCarol === "accept"),
            );
          },
        ),
      );
    }));
});

describe("agreement answers", () => {
  const matchers = normMatchers(
    norms(...builtIn, "examples/group-meeting.lisp"),
  );

  it("refuses an answer to an agreement when none is open", () =>
    Effect.gen(function* () {
      const accept = yield* from(
        carol,
        group,
        "(accept-proposal :ontology norm-agreement)",
      );

      expect(
        yield* check("norms.nonconforming", matchers, outgoing(accept)),
      ).toMatchObject({
        matched: true,
        detail: { reason: "unknown-instance" },
      });
    }));
});

describe("norm instances", () => {
  const matchers = normMatchers(
    norms(...builtIn, "examples/group-meeting.lisp"),
  );

  it("names the acts a member may send instead of one out of turn", () =>
    Effect.gen(function* () {
      const history = [
        ...(yield* agreement(["accept-proposal", "accept-proposal"])),
        yield* from(alice, group, cfp),
      ];
      const early = yield* from(
        alice,
        group,
        "(accept-proposal :ontology group-meeting :option 1)",
      );
      const wrongRole = yield* from(
        bob,
        group,
        '(cfp :ontology group-meeting :length "1 hour" :candidates "1 Fri")',
      );

      expect(
        yield* check("norms.nonconforming", matchers, outgoing(early, history)),
      ).toMatchObject({
        matched: true,
        detail: { reason: "out-of-turn", allowed: ["cancel"] },
      });
      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          incoming(carol, wrongRole, history),
        ),
      ).toMatchObject({
        matched: true,
        detail: {
          reason: "out-of-turn",
          allowed: ["propose", "refuse", "cancel"],
        },
      });
    }));

  it("reports each field a form does not accept to the sender, and answers a peer with not-understood", () =>
    Effect.gen(function* () {
      const history = [
        ...(yield* agreement(["accept-proposal", "accept-proposal"])),
        yield* from(alice, group, cfp),
      ];
      const proposal = yield* from(
        bob,
        group,
        "(propose :ontology group-meeting :free (11) :note late)",
      );

      const own = yield* check(
        "norms.unparsed",
        matchers,
        outgoing(proposal, history),
      );
      const peer = yield* check(
        "norms.unparsed",
        matchers,
        incoming(alice, proposal, history),
      );

      const free: unknown = expect.objectContaining({ field: "free" });
      const note: unknown = expect.objectContaining({ field: "note" });
      const bothFields: unknown = expect.arrayContaining([free, note]);
      const namesNote: unknown = expect.stringContaining('"note"');
      expect(own).toMatchObject({
        matched: true,
        detail: { reason: "invalid-content", fields: bothFields },
      });
      expect(replyOf(peer)).toMatchObject({
        act: {
          performative: "not-understood",
          inReplyTo: proposal.msgId,
          fields: { reason: "invalid-content", error: namesNote },
        },
      });
    }));

  it("holds an act that depends on history the window omits, but not chat", () =>
    Effect.gen(function* () {
      const accept = yield* from(
        bob,
        group,
        "(accept-proposal :ontology norm-agreement)",
      );
      const chat = yield* from(bob, group, "Sounds good.");

      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(accept, [], { earlierHistoryOmitted: true }),
        ),
      ).toMatchObject({
        matched: true,
        refusal: "unresolved-evidence",
        detail: { reason: "history-unavailable" },
      });
      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(chat, [], { earlierHistoryOmitted: true }),
        ),
      ).toEqual({ matched: false });
    }));
});

describe("a peer's act out of turn", () => {
  const pair = [alice, bob];

  it("is answered with not-understood naming the acts the peer may send, in a direct conversation", () =>
    Effect.gen(function* () {
      const matchers = normMatchers(
        norms(...builtIn, "examples/office-hours.lisp"),
      );
      const agreed = [
        yield* from(
          bob,
          pair,
          '(propose :ontology norm-agreement :norm office-hours :task "Book a slot")',
        ),
        yield* from(alice, pair, "(accept-proposal :ontology norm-agreement)"),
      ];
      const early = yield* from(bob, pair, "(query-if :ontology office-hours)");

      const result = yield* check(
        "norms.nonconforming",
        matchers,
        incoming(alice, early, agreed),
      );

      expect(replyOf(result)).toMatchObject({
        act: {
          performative: "not-understood",
          inReplyTo: early.msgId,
          fields: {
            reason: "out-of-turn",
            error: "acts you may send now: request",
          },
        },
      });
    }));

  it("is withheld without an answer in a group, where every member would answer", () =>
    Effect.gen(function* () {
      const matchers = normMatchers(
        norms(...builtIn, "examples/group-meeting.lisp"),
      );
      const history = yield* agreement(["accept-proposal", "accept-proposal"]);
      const early = yield* from(
        bob,
        group,
        "(propose :ontology group-meeting :free (1))",
      );

      const result = yield* check(
        "norms.nonconforming",
        matchers,
        incoming(carol, early, history),
      );

      expect(result).toEqual({
        matched: true,
        detail: {
          reason: "out-of-turn",
          norm: "group-meeting",
          allowed: ["cfp"],
        },
      });
    }));
});

describe("office hours: offers and escalation", () => {
  const matchers = normMatchers(
    norms(...builtIn, "examples/office-hours.lisp"),
  );
  const pair = [alice, bob];
  const student = (text: string) => from(bob, pair, text);
  const professor = (text: string) => from(alice, pair, text);
  const offer =
    '(propose :ontology office-hours :option1 "Tue 14:00" :option2 "Thu 10:00")';
  const none = "(reject-proposal :ontology office-hours)";

  /** The student proposes office-hours, the professor accepts, and the student asks to meet. */
  const asked = Effect.all([
    student(
      '(propose :ontology norm-agreement :norm office-hours :task "Meet")',
    ),
    professor("(accept-proposal :ontology norm-agreement)"),
    student('(request :ontology office-hours :topic "Project scope")'),
  ]);

  it("allows an offer only while fewer than two rounds were declined", () =>
    Effect.gen(function* () {
      const opening = yield* asked;
      const rounds = yield* Effect.all(
        Array.from({ length: 3 }, () =>
          Effect.all([professor(offer), student(none)]),
        ),
      );
      const next = yield* professor(offer);

      fc.assert(
        fc.property(fc.nat({ max: rounds.length }), (declined) => {
          const history = [...opening, ...rounds.slice(0, declined).flat()];
          const result = Effect.runSync(
            check("norms.nonconforming", matchers, outgoing(next, history)),
          );
          expect(result.matched).toBe(declined >= 2);
        }),
      );
    }));

  it("allows two rounds of offers, then only escalation", () =>
    Effect.gen(function* () {
      const history = [
        ...(yield* asked),
        yield* professor(offer),
        yield* student(none),
        yield* professor(offer),
        yield* student(none),
      ];
      const third = yield* professor(offer);
      const escalate = yield* professor(
        '(failure :ontology office-hours :reason "No offered slot works.")',
      );

      expect(
        yield* check("norms.nonconforming", matchers, outgoing(third, history)),
      ).toMatchObject({
        matched: true,
        detail: { reason: "out-of-turn", allowed: ["failure", "cancel"] },
      });
      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(escalate, history),
        ),
      ).toEqual({ matched: false });
    }));

  it("lets the professor escalate only once the student declines an offer", () =>
    Effect.gen(function* () {
      const requested = yield* asked;
      const declined = [
        ...requested,
        yield* professor(offer),
        yield* student(none),
      ];
      const escalate = yield* professor(
        '(failure :ontology office-hours :reason "No offered slot works.")',
      );

      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(escalate, requested),
        ),
      ).toMatchObject({
        matched: true,
        detail: { reason: "out-of-turn", allowed: ["propose", "cancel"] },
      });
      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(escalate, declined),
        ),
      ).toEqual({ matched: false });
    }));

  it("lets each side send only its own acts", () =>
    Effect.gen(function* () {
      const history = yield* asked;
      const studentOffer = yield* student(offer);

      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(studentOffer, history),
        ),
      ).toMatchObject({
        matched: true,
        detail: { reason: "out-of-turn", allowed: ["cancel"] },
      });
    }));
});

describe("office hours: holds", () => {
  const matchers = normMatchers(
    norms(...builtIn, "examples/office-hours.lisp"),
  );
  const pair = [alice, bob];
  const student = (text: string) => from(bob, pair, text);
  const professor = (text: string) => from(alice, pair, text);
  const offer =
    '(propose :ontology office-hours :option1 "Tue 14:00" :option2 "Thu 10:00")';

  /** The student proposes office-hours, the professor accepts, and the student asks to meet. */
  const asked = Effect.all([
    student(
      '(propose :ontology norm-agreement :norm office-hours :task "Meet")',
    ),
    professor("(accept-proposal :ontology norm-agreement)"),
    student('(request :ontology office-hours :topic "Project scope")'),
  ]);

  it("books a held slot once the student checks back", () =>
    Effect.gen(function* () {
      const history = [
        ...(yield* asked),
        yield* professor(offer),
        yield* student("(accept-proposal :ontology office-hours :option 1)"),
        yield* professor(
          '(agree :ontology office-hours :slot "Tue 14:00" :recheck-after 5)',
        ),
        yield* student("(query-if :ontology office-hours)"),
      ];
      const confirm = yield* professor(
        '(confirm :ontology office-hours :slot "Tue 14:00")',
      );

      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(confirm, history),
        ),
      ).toEqual({ matched: false });
    }));

  it("lets the professor offer again at the check-back while a round is left", () =>
    Effect.gen(function* () {
      const history = [
        ...(yield* asked),
        yield* professor(offer),
        yield* student("(accept-proposal :ontology office-hours :option 1)"),
        yield* professor(
          '(agree :ontology office-hours :slot "Tue 14:00" :recheck-after 5)',
        ),
        yield* student("(query-if :ontology office-hours)"),
      ];
      const again = yield* professor(offer);

      expect(
        yield* check("norms.nonconforming", matchers, outgoing(again, history)),
      ).toEqual({ matched: false });
    }));
});

describe("a norm's own turns", () => {
  it("ends an instance at once when its opening act starts no fact", () =>
    Effect.gen(function* () {
      const notice = Either.getOrThrow(
        readNorm(`
          (norm notice :initiator Sender :participant Reader :description "Send one notice.")
          (content inform :by Sender (field text :type string))
        `),
      );
      const matchers = normMatchers([...norms(...builtIn), notice]);
      const pair = [alice, bob];
      const history = [
        yield* from(
          alice,
          pair,
          '(propose :ontology norm-agreement :norm notice :task "Closing")',
        ),
        yield* from(bob, pair, "(accept-proposal :ontology norm-agreement)"),
        yield* from(
          alice,
          pair,
          '(inform :ontology notice :text "Closed Fri")',
        ),
      ];
      const second = yield* from(
        alice,
        pair,
        '(inform :ontology notice :text "Open Mon")',
      );

      expect(
        yield* check(
          "norms.nonconforming",
          matchers,
          outgoing(second, history),
        ),
      ).toMatchObject({ matched: true, detail: { reason: "no-agreement" } });
    }));
});

describe("acts the Router has not ordered yet", () => {
  const matchers = normMatchers(
    norms(...builtIn, "examples/office-hours.lisp"),
  );
  const pair = [alice, bob];

  /** Bob proposes office-hours and Alice accepts. */
  const agreed = Effect.all([
    from(
      bob,
      pair,
      '(propose :ontology norm-agreement :norm office-hours :task "Book a slot")',
    ),
    from(alice, pair, "(accept-proposal :ontology norm-agreement)"),
  ]);
  const request = '(request :ontology office-hours :topic "Scope")';

  it("judges an outgoing act while the agent's own acts wait to be ordered", () =>
    Effect.gen(function* () {
      const history = yield* agreed;
      const asked = yield* from(bob, pair, request);
      const cancel = yield* from(bob, pair, "(cancel :ontology office-hours)");

      const result = yield* check(
        "norms.nonconforming",
        matchers,
        outgoing(cancel, history, { pendingOutgoing: [asked] }),
      );

      expect(result).toEqual({ matched: false });
    }));

  it("refuses an outgoing act out of turn after the agent's own act still waiting to be ordered", () =>
    Effect.gen(function* () {
      const history = yield* agreed;
      const asked = yield* from(bob, pair, request);
      const again = yield* from(bob, pair, request);

      const result = yield* check(
        "norms.nonconforming",
        matchers,
        outgoing(again, history, { pendingOutgoing: [asked] }),
      );

      expect(result).toMatchObject({
        matched: true,
        detail: { reason: "out-of-turn" },
      });
    }));

  it("judges an incoming act on the ordered history alone", () =>
    Effect.gen(function* () {
      const history = yield* agreed;
      const asked = yield* from(bob, pair, request);
      const note = yield* from(
        alice,
        pair,
        "One moment, checking the calendar.",
      );

      const result = yield* check(
        "norms.nonconforming",
        matchers,
        incoming(alice, asked, history, { pendingOutgoing: [note] }),
      );

      expect(result).toEqual({ matched: false });
    }));
});

describe("norm state", () => {
  const matchers = normMatchers(
    norms(...builtIn, "examples/group-meeting.lisp"),
  );

  it("judges an act the same whatever broken acts and chat the history holds", () =>
    Effect.gen(function* () {
      const valid = [
        ...(yield* agreement(["accept-proposal", "accept-proposal"])),
        yield* from(alice, group, cfp),
      ];
      const noise = yield* Effect.all([
        from(bob, group, "(cfp :ontology"),
        from(carol, group, "(propose :ontology group-meeting :free (12))"),
        from(alice, group, "(accept-proposal :ontology norm-agreement)"),
        from(alice, group, "Running five minutes late."),
      ]);
      const proposal = yield* from(
        bob,
        group,
        "(propose :ontology group-meeting :free (1 2))",
      );
      const judge = (history: readonly Message[]) =>
        Effect.runSync(
          check("norms.nonconforming", matchers, outgoing(proposal, history)),
        );

      fc.assert(
        fc.property(
          fc.array(
            fc.tuple(fc.nat({ max: valid.length }), fc.constantFrom(...noise)),
            { maxLength: 8 },
          ),
          (insertions) => {
            const history = [...valid];
            for (const [position, message] of insertions) {
              history.splice(Math.min(position, history.length), 0, message);
            }
            expect(judge(history)).toEqual(judge(valid));
          },
        ),
      );
    }));
});

describe("norm discovery", () => {
  it("answers a direct query with the norms held, or one norm's document", () =>
    Effect.gen(function* () {
      const matchers = normMatchers(
        norms(...builtIn, "examples/office-hours.lisp"),
      );
      const list = yield* from(
        bob,
        [alice, bob],
        "(query-ref :ontology norm-discovery)",
      );
      const one = yield* from(
        bob,
        [alice, bob],
        "(query-ref :ontology norm-discovery :norm office-hours)",
      );

      const listed = yield* check(
        "norms.query",
        matchers,
        incoming(alice, list),
      );
      const sent = yield* check("norms.query", matchers, incoming(alice, one));

      expect(replyOf(listed)).toMatchObject({
        act: {
          performative: "inform",
          inReplyTo: list.msgId,
          fields: {
            norms: "norm-agreement norm-discovery feedback chat office-hours",
          },
        },
      });
      const document = replyOf(sent);
      expect(
        document?.kind === "act" && document.act.content !== undefined
          ? Either.getOrThrow(readNorm(document.act.content)).name
          : undefined,
      ).toBe("office-hours");
    }));

  it("lets a member accept a norm once it holds or received the document", () =>
    Effect.gen(function* () {
      const proposer = normMatchers(
        norms(...builtIn, "examples/office-hours.lisp"),
      );
      const member = normMatchers(norms(...builtIn));
      const pair = [alice, bob];
      const query = yield* from(
        bob,
        pair,
        "(query-ref :ontology norm-discovery :norm office-hours)",
      );
      const answered = yield* check(
        "norms.query",
        proposer,
        incoming(alice, query),
      );
      const reply = yield* from(
        alice,
        pair,
        answered.matched ? (answered.reply ?? "") : "",
      );
      const proposal = yield* from(
        alice,
        pair,
        "(propose :ontology norm-agreement :norm office-hours)",
      );
      const accept = yield* from(
        bob,
        pair,
        "(accept-proposal :ontology norm-agreement)",
      );

      expect(
        yield* check(
          "norms.nonconforming",
          member,
          outgoing(accept, [proposal]),
        ),
      ).toMatchObject({
        matched: true,
        detail: { reason: "norm-unknown", norm: "office-hours" },
      });
      expect(
        yield* check(
          "norms.nonconforming",
          member,
          outgoing(accept, [query, reply, proposal]),
        ),
      ).toEqual({ matched: false });
    }));
});

describe("discovery among several members", () => {
  it("answers each member's query by its message, however many others asked first", () =>
    Effect.gen(function* () {
      const matchers = normMatchers(
        norms(...builtIn, "examples/group-meeting.lisp"),
      );
      const [proposal] = yield* agreement([]);
      const ask = (member: AgentCard) =>
        from(
          member,
          group,
          "(query-ref :ontology norm-discovery :norm group-meeting)",
        );
      const others = yield* Effect.all([ask(bob), ask(carol), ask(bob)]);
      const query = yield* ask(carol);

      fc.assert(
        fc.property(fc.shuffledSubarray(others), (earlier) => {
          const answered = Effect.runSync(
            check(
              "norms.query",
              matchers,
              incoming(alice, query, [proposal, ...earlier]),
            ),
          );
          expect(replyOf(answered)).toMatchObject({
            act: { inReplyTo: query.msgId },
          });
        }),
      );
    }));

  it("refuses a discovery answer that names no open query", () =>
    Effect.gen(function* () {
      const matchers = normMatchers(norms(...builtIn));
      const stray = yield* from(
        alice,
        [alice, bob],
        "(inform :ontology norm-discovery :in-reply-to msg_unasked :norms chat)",
      );

      expect(
        yield* check("norms.nonconforming", matchers, incoming(bob, stray)),
      ).toMatchObject({
        matched: true,
        detail: { reason: "unknown-instance" },
      });
    }));
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore default assertion rules. */
