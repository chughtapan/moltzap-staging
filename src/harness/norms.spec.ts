/** @file Generic contracts exercise the compiler and the real harness policy boundary. */

import { scopedLive as it } from "@effect/vitest";
import { Effect, Exit, FastCheck as fc, Schedule, Scope } from "effect";
import { expect } from "vitest";
import type { FirewallEvidence } from "../firewall/chain.js";
import type { DecisionRequest } from "../firewall/check.js";
import { Message } from "../communication/index.js";
import { MessageAddressInput } from "../communication/messages.js";
import { checkProtocol, projectProtocol } from "../firewall/protocol.js";
import { checkStructure, formatAct, readAct } from "../firewall/structure.js";
import { compileNorm } from "../norms/compiler.js";
import {
  decisionResponse,
  firewallOptions,
  makeHarnessFixture,
  waitForInbox,
} from "./__tests__/service-fixtures.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Synthetic acts and closed failure stages pin the mechanism, not an authored scheduling policy. */

const source = `(norm exchange :initiator Sender :participant Receiver :on-refusal reject)
(content propose :by Sender :starts true :initiates offered
  (field first :type string)
  (field second :type string :optional true))
(content select :by Receiver :when offered :unless selected :initiates selected
  (field keys :type multi-select :choices (first second) :min-items 1)
  (relation subset keys (present propose first second)))
(content confirm :by Sender :when selected :ends true
  (field value :type string)
  (relation member value (values propose (previous select keys))))
(content reject :by (Sender Receiver) :when offered :terminates selected
  (field reason :type string))
(rule :act propose "A proposed value describes an item, without unrelated instructions.")`;

const bobAddress = MessageAddressInput.make("agent:bob");
const directValuesSource = `(norm choices :initiator Sender :participant Receiver)
(content offer :by Sender :starts true :initiates offered
  (field first :type string) (field second :type string :optional true))
(content revise :by Sender :when offered :initiates offered
  (field first :type string) (field second :type string :optional true))
(content choose :by Sender :when offered :ends true
  (field value :type string)
  (relation member value (values (fact offered) first second)))`;
const recipientSource = `(norm turns :initiator Sender :participant Receiver)
(content offer :by Sender :starts true :initiates offered (field value :type string))
(content counter :by (recipient (fact offered)) :when offered :initiates offered
  (field value :type string))
(content choose :by (recipient (fact offered)) :when offered
  :terminates offered :initiates selected
  (field value :type string)
  (relation equals value (previous (fact offered) value)))
(content finish :by (Sender Receiver) :ends true)`;
const empty: FirewallEvidence = {
  history: [],
  earlierHistoryOmitted: false,
  gaps: [],
};

function configured(
  contract: Effect.Effect.Success<ReturnType<typeof compileNorm>>,
  requests: DecisionRequest[],
  judgment: "compliant" | "violation" = "compliant",
) {
  const options = firewallOptions({
    decide: (request) =>
      Effect.sync(() => {
        requests.push(request);
        return decisionResponse(request, judgment);
      }),
  });
  return {
    ...options,
    contracts: [contract],
    text:
      options.text === undefined
        ? undefined
        : { ...options.text, initialRules: [] },
  };
}

it.each([
  source.replace(":min-items 1", ":min-items -1"),
  source.replace(":type string)", ":type string :minimum 1)"),
  source.replace(":min-items 1", ":min-items 3 :max-items 2"),
  source.replace(":choices (first second)", ":choices (first first)"),
  source.replace("(previous select keys)", "(previous missing keys)"),
  source.replace(":when offered", ":when absent"),
  source.replace("(relation subset keys", "(relation subset missing"),
  source.replace(":by Receiver", ":by Undeclared"),
  source.replace(":min-items 1", ":min-items 1 :min-items 2"),
  recipientSource.replace(
    "recipient (fact offered)",
    "recipient (fact absent)",
  ),
  recipientSource.replace("recipient (fact offered)", "recipient offered"),
  recipientSource.replace(
    "recipient (fact offered)",
    "recipient (fact offered extra)",
  ),
  recipientSource.replace(
    "recipient (fact offered)",
    "recipient (fact offered) extra",
  ),
  recipientSource.replace(
    ":by Sender :starts",
    ":by (recipient (fact offered)) :starts",
  ),
])("rejects malformed declarations at installation", (document) =>
  Effect.gen(function* () {
    expect((yield* compileNorm(document).pipe(Effect.flip))._tag).toBe(
      "NormDocumentError",
    );
  }),
);

it("compiles exact revisions and exposes deterministic inventory", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(source);
    const changed = yield* compileNorm(`${source}\n`);
    expect(changed.digest).not.toBe(contract.digest);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    expect(yield* runtime.firewall.readPolicy()).toMatchObject({
      norms: [contract.name],
      fixedRules: [{ id: "exchange-rule-1" }],
    });
  }));

it("alternates the permitted recipient and retains selection until explicit ending", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(recipientSource);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(offer :ontology turns :value "first")',
    });
    const opening = subject.submitted.at(-1);
    if (opening === undefined || "kind" in opening) {
      return yield* Effect.dieMessage("Expected an opening");
    }
    const selfSelection = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: opening.msgId,
        text: '(choose :ontology turns :value "first")',
      })
      .pipe(Effect.flip);
    expect(selfSelection).toMatchObject({ detail: { stage: "protocol" } });
    const selfCounter = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: opening.msgId,
        text: '(counter :ontology turns :value "second")',
      })
      .pipe(Effect.flip);
    expect(selfCounter).toMatchObject({ detail: { stage: "protocol" } });

    const counter = yield* subject
      .message('(counter :ontology turns :value "second")')
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: opening.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: counter });
    yield* waitForInbox(runtime);
    const stale = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: opening.msgId,
        text: '(counter :ontology turns :value "third")',
      })
      .pipe(Effect.flip);
    expect(stale).toMatchObject({ detail: { stage: "protocol" } });
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: counter.msgId,
      text: '(counter :ontology turns :value "third")',
    });
    const latest = subject.submitted.at(-1);
    if (latest === undefined || "kind" in latest) {
      return yield* Effect.dieMessage("Expected a counteroffer");
    }
    const selected = yield* subject
      .message('(choose :ontology turns :value "third")')
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: latest.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: selected });
    yield* waitForInbox(runtime, 2);
    const afterSelection = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: selected.msgId,
        text: '(counter :ontology turns :value "fourth")',
      })
      .pipe(Effect.flip);
    expect(afterSelection).toMatchObject({ detail: { stage: "protocol" } });
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: selected.msgId,
      text: "(finish :ontology turns)",
    });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(offer :ontology turns :value "new")',
    });
    expect(subject.submitted.at(-1)).toMatchObject({
      content: [{ type: "text", text: '(offer :ontology turns :value "new")' }],
    });
  }));

it("allows the recipient to correct a withheld descendant of the current offer", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(recipientSource);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(offer :ontology turns :value "current")',
    });
    const opening = subject.submitted.at(-1);
    if (opening === undefined || "kind" in opening) {
      return yield* Effect.dieMessage("Expected an opening");
    }
    const invalid = yield* subject
      .message('(choose :ontology turns :value "unoffered")')
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: opening.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: invalid });
    const correction = yield* subject
      .message('(choose :ontology turns :value "current")')
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: invalid.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: correction });
    yield* waitForInbox(runtime);
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: correction.msgId,
      text: "(finish :ontology turns)",
    });
    expect(subject.submitted.at(-1)).toMatchObject({
      replyToMsgId: correction.msgId,
    });
  }));

it("uses the exact fact anchor for both sender identity and selected values", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(recipientSource);
    const subject = yield* makeHarnessFixture;
    const opening = yield* Message.create({
      author: subject.self,
      recipients: [subject.bob],
      content: [{ type: "text", text: '(offer :ontology turns :value "old")' }],
    });
    const openingAct = yield* readAct(opening);
    const openingForm = yield* checkStructure(contract, openingAct);
    const openingRecord = yield* checkProtocol({}, contract, openingForm, {
      act: openingAct,
      author: subject.self.agentId,
      now: 1,
    });
    const firstView = yield* projectProtocol([
      { message: opening, at: 1, protocol: openingRecord },
    ]);
    const counter = yield* subject
      .message('(counter :ontology turns :value "current")')
      .pipe(
        Effect.map((message) => ({
          ...message,
          msgId: opening.msgId,
          replyToMsgId: opening.msgId,
        })),
      );
    const counterAct = yield* readAct(counter);
    const counterForm = yield* checkStructure(contract, counterAct);
    const counterRecord = yield* checkProtocol(
      firstView,
      contract,
      counterForm,
      {
        act: counterAct,
        replyToMsgId: opening.msgId,
        author: subject.bob.agentId,
        now: 2,
      },
    );
    const view = yield* projectProtocol([
      { message: opening, at: 1, protocol: openingRecord },
      { message: counter, at: 2, protocol: counterRecord },
    ]);
    const selected = {
      name: "choose",
      norm: "turns",
      fields: { value: "current" },
    };
    const form = yield* checkStructure(contract, selected);
    yield* checkProtocol(view, contract, form, {
      act: selected,
      replyToMsgId: counter.msgId,
      author: subject.self.agentId,
      now: 3,
    });
    const wrongValue = yield* checkProtocol(view, contract, form, {
      act: { ...selected, fields: { value: "old" } },
      replyToMsgId: counter.msgId,
      author: subject.self.agentId,
      now: 3,
    }).pipe(Effect.flip);
    expect(wrongValue).toMatchObject({ stage: "protocol" });
    const wrongSender = yield* checkProtocol(view, contract, form, {
      act: selected,
      replyToMsgId: counter.msgId,
      author: subject.bob.agentId,
      now: 3,
    }).pipe(Effect.flip);
    expect(wrongSender).toMatchObject({ stage: "protocol" });
  }));

it.each(["one", "two"])("accepts the directly offered value %s", (value) =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(directValuesSource);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(offer :ontology choices :first "one" :second "two")',
    });
    const offer = subject.submitted.at(-1);
    if (offer === undefined || "kind" in offer) {
      return yield* Effect.dieMessage("Expected an offer");
    }
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: offer.msgId,
      text: `(choose :ontology choices :value "${value}")`,
    });
    expect(subject.submitted.at(-1)).toMatchObject({
      replyToMsgId: offer.msgId,
    });
  }),
);

it("uses only the latest offer's present values across alternative act forms", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(directValuesSource);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(offer :ontology choices :first "old" :second "removed")',
    });
    const old = subject.submitted.at(-1);
    if (old === undefined || "kind" in old) {
      return yield* Effect.dieMessage("Expected an offer");
    }
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: old.msgId,
      text: '(revise :ontology choices :first "current")',
    });
    const current = subject.submitted.at(-1);
    if (current === undefined || "kind" in current) {
      return yield* Effect.dieMessage("Expected a revised offer");
    }
    const stale = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: old.msgId,
        text: '(choose :ontology choices :value "current")',
      })
      .pipe(Effect.flip);
    expect(stale).toMatchObject({ detail: { stage: "protocol" } });
    const superseded = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: old.msgId,
        text: '(choose :ontology choices :value "old")',
      })
      .pipe(Effect.flip);
    expect(superseded).toMatchObject({ detail: { stage: "protocol" } });
    for (const value of ["old", "removed", "first", "unoffered"]) {
      const failure = yield* runtime.harness
        .send({
          to: bobAddress,
          replyToMsgId: current.msgId,
          text: `(choose :ontology choices :value "${value}")`,
        })
        .pipe(Effect.flip);
      expect(failure).toMatchObject({ detail: { stage: "protocol" } });
    }
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: current.msgId,
      text: '(choose :ontology choices :value "current")',
    });
    expect(subject.submitted.at(-1)).toMatchObject({
      replyToMsgId: current.msgId,
    });
  }));

it.each([
  directValuesSource.replace(
    "(fact offered) first second",
    "(offer absent) first second",
  ),
  directValuesSource.replace(
    "(fact offered) first second",
    "(offer revise) missing",
  ),
  directValuesSource.replace(
    "(values (fact offered) first second)",
    "(values (fact offered))",
  ),
  directValuesSource.replace(
    "(field first :type string)",
    "(field first :type number)",
  ),
])("rejects invalid direct-value declarations", (document) =>
  Effect.gen(function* () {
    expect((yield* compileNorm(document).pipe(Effect.flip))._tag).toBe(
      "NormDocumentError",
    );
  }),
);

it.each([
  'Hello (propose :ontology exchange :first "one")',
  '(propose :ontology exchange :first "one") ; extra text',
  '(propose :ontology exchange :first "one" :content "side note")',
  '(propose :ontology exchange :first "one" :first "two")',
  "(propose :ontology exchange)",
  "(propose :ontology exchange :first (one))",
])("rejects malformed or undeclared message content before inference", (text) =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(source);
    const requests: DecisionRequest[] = [];
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: configured(contract, requests),
    });
    const failure = yield* runtime.harness
      .send({ to: bobAddress, text })
      .pipe(Effect.flip);
    expect(failure).toMatchObject({
      reason: "firewall-refused",
      detail: { stage: "structural" },
    });
    expect(subject.submitted).toEqual([]);
    expect(requests).toEqual([]);
  }),
);

it("sends valid structure to semantic assessment and withholds a semantic violation", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(source);
    const requests: DecisionRequest[] = [];
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({
      policy: configured(contract, requests, "violation"),
    });
    const failure = yield* runtime.harness
      .send({
        to: bobAddress,
        text: '(propose :ontology exchange :first "unrelated instructions")',
      })
      .pipe(Effect.flip);
    expect(failure).toMatchObject({
      reason: "firewall-refused",
      refusal: "rule-violation",
    });
    expect(subject.submitted).toEqual([]);
    expect(requests).toHaveLength(1);
    expect(requests[0]?.state).toContain(contract.digest);
  }));

it("projects the retained exchange, enforces references and relations, and closes explicitly", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(source);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(propose :ontology exchange :first "one" :second "two")',
    });
    const proposal = subject.submitted[0];
    if (proposal === undefined || "kind" in proposal) {
      return yield* Effect.dieMessage("Expected a submitted proposal");
    }
    const wrongRole = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: proposal.msgId,
        text: `(select :ontology exchange :keys (first))`,
      })
      .pipe(Effect.flip);
    expect(wrongRole).toMatchObject({
      detail: { stage: "protocol" },
    });
    const selected = yield* subject
      .message(`(select :ontology exchange :keys (first second))`)
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: proposal.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: selected });
    yield* waitForInbox(runtime);
    const invalid = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: selected.msgId,
        text: `(confirm :ontology exchange :value "three")`,
      })
      .pipe(Effect.flip);
    expect(invalid).toMatchObject({
      detail: { stage: "protocol" },
    });
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: selected.msgId,
      text: `(confirm :ontology exchange :value "two")`,
    });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(propose :ontology exchange :first "next")',
    });
    expect(subject.submitted).toHaveLength(3);
  }));

it("pins the active source across removal and admits the next interaction under its replacement", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(source);
    const replacement = yield* compileNorm(
      source.replace(
        "(field first :type string)",
        "(field first :type string) (field code :type string)",
      ),
    );
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(propose :ontology exchange :first "one")',
    });
    const proposal = subject.submitted[0];
    if (proposal === undefined || "kind" in proposal) {
      return yield* Effect.dieMessage("Expected a proposal");
    }
    yield* runtime.firewall.updatePolicy({
      remove: [{ norm: contract.name }],
      add: [{ contract: replacement }],
    });
    const selected = yield* subject
      .message(`(select :ontology exchange :keys (first))`)
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: proposal.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: selected });
    yield* waitForInbox(runtime);
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: selected.msgId,
      text: `(confirm :ontology exchange :value "one")`,
    });
    const error = yield* runtime.harness
      .send({
        to: bobAddress,
        text: '(propose :ontology exchange :first "old shape")',
      })
      .pipe(Effect.flip);
    expect(error).toMatchObject({ detail: { stage: "structural" } });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(propose :ontology exchange :first "next" :code "new")',
    });
  }));

it.each(["finish", "reject"])(
  "refuses %s from an ended interaction without changing the current interaction",
  (act) =>
    Effect.gen(function* () {
      const contract = yield* compileNorm(
        `${source}\n(content finish :by (Sender Receiver) :ends true)`,
      );
      const subject = yield* makeHarnessFixture;
      const runtime = yield* subject.open({ policy: configured(contract, []) });
      yield* runtime.harness.send({
        to: bobAddress,
        text: '(propose :ontology exchange :first "old")',
      });
      const old = subject.submitted.at(-1);
      if (old === undefined || "kind" in old) {
        return yield* Effect.dieMessage("Expected an opening message");
      }
      yield* runtime.harness.send({
        to: bobAddress,
        replyToMsgId: old.msgId,
        text: "(finish :ontology exchange)",
      });
      yield* runtime.harness.send({
        to: bobAddress,
        text: '(propose :ontology exchange :first "current")',
      });
      const current = subject.submitted.at(-1);
      if (current === undefined || "kind" in current) {
        return yield* Effect.dieMessage("Expected the current opening");
      }
      const selected = yield* subject
        .message("(select :ontology exchange :keys (first))")
        .pipe(
          Effect.map((message) => ({
            ...message,
            replyToMsgId: current.msgId,
          })),
        );
      yield* subject.deliver({ kind: "message", message: selected });
      yield* waitForInbox(runtime);
      const text =
        act === "finish"
          ? "(finish :ontology exchange)"
          : '(reject :ontology exchange :reason "Declined")';
      const failure = yield* runtime.harness
        .send({ to: bobAddress, replyToMsgId: old.msgId, text })
        .pipe(Effect.flip);
      expect(failure).toMatchObject({
        reason: "firewall-refused",
        detail: { stage: "protocol" },
      });
      yield* runtime.harness.send({
        to: bobAddress,
        replyToMsgId: selected.msgId,
        text: '(confirm :ontology exchange :value "current")',
      });
      expect(subject.submitted.at(-1)).toMatchObject({
        content: [
          {
            type: "text",
            text: '(confirm :ontology exchange :value "current")',
          },
        ],
      });
    }),
);

it("does not attach an unreferenced withheld opening to the current interaction", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(
      `${source}\n(content finish :by (Sender Receiver) :ends true)`,
    );
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(propose :ontology exchange :first "current")',
    });
    const current = subject.submitted.at(-1);
    if (current === undefined || "kind" in current) {
      return yield* Effect.dieMessage("Expected the current opening");
    }
    const unrelated = yield* subject.message(
      '(propose :ontology exchange :first "unrelated")',
    );
    const ending = yield* Message.create({
      author: subject.self,
      recipients: [subject.bob],
      replyToMsgId: unrelated.msgId,
      content: [{ type: "text", text: "(finish :ontology exchange)" }],
    });
    const decision = yield* runtime.firewall.checkOutgoing(ending, {
      ...empty,
      protocolHistory: { messages: [current], complete: true },
      references: [unrelated],
    });
    expect(decision.kind).toBe("refused");
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: current.msgId,
      text: "(finish :ontology exchange)",
    });
    expect(subject.submitted.at(-1)).toMatchObject({
      replyToMsgId: current.msgId,
    });
  }));

it("does not advance on a withheld incoming act and checks configured feedback as an ordinary send", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(source);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    yield* runtime.harness.send({
      to: bobAddress,
      text: '(propose :ontology exchange :first "one")',
    });
    const proposal = subject.submitted[0];
    if (proposal === undefined || "kind" in proposal) {
      return yield* Effect.dieMessage("Expected a proposal");
    }
    const bad = yield* subject
      .message(`(select :ontology exchange :keys (second))`)
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: proposal.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: bad });
    yield* runtime.firewall.readDecisions({}).pipe(
      Effect.repeat({
        until: (page) => page.decisions.length >= 3,
        schedule: Schedule.spaced("5 millis"),
      }),
      Effect.raceFirst(runtime.failed),
      Effect.timeout("10 seconds"),
    );
    expect((yield* runtime.harness.listInbox()).items).toEqual([]);
    const feedback = subject.submitted[1];
    if (feedback === undefined || !("content" in feedback)) {
      return yield* Effect.dieMessage("Expected ordinary feedback");
    }
    expect(
      feedback.content[0]?.type === "text" ? feedback.content[0].text : "",
    ).toContain("(reject :ontology exchange");
    const tooSoon = yield* runtime.harness
      .send({
        to: bobAddress,
        replyToMsgId: bad.msgId,
        text: `(confirm :ontology exchange :value "one")`,
      })
      .pipe(Effect.flip);
    expect(tooSoon).toMatchObject({ detail: { stage: "protocol" } });
    const good = yield* subject
      .message(`(select :ontology exchange :keys (first))`)
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: proposal.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: good });
    yield* waitForInbox(runtime);
    yield* runtime.harness.send({
      to: bobAddress,
      replyToMsgId: good.msgId,
      text: `(confirm :ontology exchange :value "one")`,
    });
  }));

it("uses original admission time to enforce a declared delay", () =>
  Effect.gen(function* () {
    const contract =
      yield* compileNorm(`(norm delayed :initiator Sender :participant Receiver)
    (content request :by Sender :starts true :initiates waiting (field delay :type number :minimum 0))
    (content inform :by Receiver :when waiting :after waiting :delay delay :unit seconds :ends true)`);
    const subject = yield* makeHarnessFixture;
    const request = yield* Message.create({
      author: subject.self,
      recipients: [subject.bob],
      content: [{ type: "text", text: "(request :ontology delayed :delay 5)" }],
    });
    const act = yield* readAct(request);
    const form = yield* checkStructure(contract, act);
    const protocol = yield* checkProtocol({}, contract, form, {
      act,
      author: subject.self.agentId,
      now: 1000,
    });
    const view = yield* projectProtocol([
      { message: request, at: 1000, protocol },
    ]);
    const response = { name: "inform", norm: "delayed", fields: {} };
    const replyForm = yield* checkStructure(contract, response);
    expect(
      (yield* checkProtocol(view, contract, replyForm, {
        act: response,
        replyToMsgId: request.msgId,
        author: subject.bob.agentId,
        now: 5999,
      }).pipe(Effect.flip)).stage,
    ).toBe("protocol");
    yield* checkProtocol(view, contract, replyForm, {
      act: response,
      replyToMsgId: request.msgId,
      author: subject.bob.agentId,
      now: 6000,
    });
  }));

it("keeps draft assessments out of state until their messages appear in history", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(source);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    const message = yield* Message.create({
      author: subject.self,
      recipients: [subject.bob],
      content: [
        {
          type: "text",
          text: formatAct({
            name: "propose",
            norm: "exchange",
            fields: { first: "one" },
          }),
        },
      ],
    });
    const first = yield* runtime.firewall.checkOutgoing(message, empty);
    yield* first.save;
    expect((yield* runtime.firewall.checkOutgoing(message, empty)).kind).toBe(
      "allowed",
    );
    const incomplete = yield* runtime.firewall.checkOutgoing(message, {
      ...empty,
      protocolHistory: { messages: [], complete: false },
    });
    expect(incomplete).toMatchObject({
      kind: "refused",
      refusal: { refusal: "unresolved-evidence" },
    });
  }));

it("binds retained history to exact assessed content even when an unsent draft shares its ID", () =>
  Effect.gen(function* () {
    const contract = yield* compileNorm(source);
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open({ policy: configured(contract, []) });
    const draft = yield* Message.create({
      author: subject.self,
      recipients: [subject.bob],
      content: [
        { type: "text", text: '(propose :ontology exchange :first "unsent")' },
      ],
    });
    yield* (yield* runtime.firewall.checkOutgoing(draft, empty)).save;
    const sent = {
      ...draft,
      content: [
        { type: "text", text: '(propose :ontology exchange :first "sent")' },
      ],
    } satisfies Message;
    yield* (yield* runtime.firewall.checkOutgoing(sent, empty)).save;
    yield* subject.retain(sent);
    const selected = yield* subject
      .message(`(select :ontology exchange :keys (first))`)
      .pipe(
        Effect.map((message) => ({ ...message, replyToMsgId: sent.msgId })),
      );
    yield* subject.deliver({ kind: "message", message: selected });
    yield* waitForInbox(runtime);
    const confirmation = `(confirm :ontology exchange :value "sent")`;
    yield* runtime.harness.send({
      to: bobAddress,
      text: confirmation,
      replyToMsgId: selected.msgId,
    });
    expect(subject.submitted.at(-1)).toMatchObject({
      content: [{ type: "text", text: confirmation }],
    });
  }));

it("reopens baseline saved inbox guidance without running response prediction", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const scope = yield* Scope.make();
    const legacy = {
      checkId: `chk_${"a".repeat(64)}`,
      subjects: [],
      instructions: [
        {
          ruleId: "saved-rule",
          instruction: "Wait for the earlier reply.",
          expectation: "wait" as const,
          subjects: [],
        },
      ],
    };
    const runtime = yield* subject
      .open({
        decorateFirewall: (firewall) => ({
          ...firewall,
          checkIncoming: (value, evidence) =>
            firewall
              .checkIncoming(value, evidence)
              .pipe(
                Effect.map((decision) =>
                  decision.kind === "allowed"
                    ? { ...decision, guidance: legacy }
                    : decision,
                ),
              ),
        }),
      })
      .pipe(Scope.extend(scope));
    yield* subject.deliver({
      kind: "message",
      message: yield* subject.message("Saved message"),
    });
    const saved = (yield* waitForInbox(runtime)).items[0];
    if (saved === undefined) {
      return yield* Effect.dieMessage("Expected saved delivery");
    }
    yield* Scope.close(scope, Exit.void);
    const reopened = yield* subject.open();
    expect((yield* waitForInbox(reopened)).items[0]?.guidance).toEqual(legacy);
  }));

it.prop(
  "preserves arbitrary quoted text through the structured representation",
  [fc.string()],
  ([value]) =>
    Effect.gen(function* () {
      const subject = yield* makeHarnessFixture;
      const expected = {
        name: "propose",
        norm: "exchange",
        fields: { first: value },
      };
      const message = yield* Message.create({
        author: subject.self,
        recipients: [subject.bob],
        content: [{ type: "text", text: formatAct(expected) }],
      });
      expect(yield* readAct(message)).toEqual(expected);
    }),
  { fastCheck: { numRuns: 20 } },
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore default assertion checks. */
