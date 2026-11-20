/** @file A firewall concludes each check as plain data: what it decided, what to save of it and what to reply. */

import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { scopedLive as it } from "@effect/vitest";
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { Effect, FastCheck as fc, Schema } from "effect";
import { describe, expect } from "vitest";
import {
  alice,
  certainProvider,
  firewallOf,
  incoming,
  outgoing,
  peer,
  policy,
  privacy,
} from "./__tests__/firewall-fixtures.js";
import {
  type DecisionProvider,
  DecisionProviderError,
  type StructureCheck,
} from "./check.js";
import { type Direction, TextRule } from "./rules.js";
import { makeFirewall } from "./service.js";

const bob = peer("bob", 2);
const notice = "Your message was not accepted.";

/** Checks the fixtures' draft from Alice, or their peer message to her, with `firewall`. */
function check(
  firewall: ReturnType<typeof makeFirewall>,
  direction: Direction,
) {
  return Effect.gen(function* () {
    return direction === "outgoing"
      ? yield* firewall.checkOutgoing(yield* outgoing(alice), [])
      : yield* firewall.checkIncoming(yield* incoming(alice), []);
  });
}

describe("Firewall checks", () => {
  it.each(["incoming", "outgoing"] as const)(
    "allows every %s message of an agent without a message policy, with nothing to save",
    (direction) =>
      Effect.gen(function* () {
        const result = yield* check(
          makeFirewall({ me: alice, historyLimit: 256 }),
          direction,
        );

        expect(result).toEqual({ kind: "allowed" });
      }),
  );

  it.each(["incoming", "outgoing"] as const)(
    "refuses an %s message that breaks a rule, with the record of that decision for its peer",
    (direction) =>
      Effect.gen(function* () {
        const result = yield* check(firewallOf(policy([privacy])), direction);

        expect(result).toMatchObject({
          kind: "refused",
          reason: "rule-violation",
          feedback: [privacy.feedback],
          record: {
            direction,
            decision: direction === "outgoing" ? "refused" : "withheld",
            peer: bob,
            reason: "rule-violation",
            decidedBy: [{ ruleId: privacy.id, advice: privacy.feedback }],
            checkedRuleIds: [privacy.id],
          },
        });
      }),
  );

  it.each(["incoming", "outgoing"] as const)(
    "refuses an %s message whose check could not run, with nothing to save",
    (direction) =>
      Effect.gen(function* () {
        const result = yield* check(
          firewallOf(
            policy([privacy], {
              decide: () =>
                Effect.fail(new DecisionProviderError({ transient: false })),
            }),
            { notice },
          ),
          direction,
        );

        expect(result).toMatchObject({
          kind: "refused",
          reason: "check-unavailable",
        });
        expect(result).not.toHaveProperty("record");
      }),
  );

  it.prop(
    "shows the provider the latest earlier messages up to its history limit, each visible to the agent only if allowed or its own",
    [fc.integer({ min: 1, max: 4 }), fc.array(fc.boolean(), { maxLength: 6 })],
    ([historyLimit, allowed]) =>
      Effect.gen(function* () {
        const states: unknown[] = [];
        const recording: DecisionProvider = {
          decide: (request) => {
            states.push(
              Schema.decodeUnknownSync(
                Schema.parseJson(
                  Schema.Struct({
                    history: Schema.Array(
                      Schema.Struct({ visibleToAgent: Schema.Boolean }),
                    ),
                    earlierHistoryOmitted: Schema.Boolean,
                  }),
                ),
              )(request.state),
            );
            return certainProvider.decide(request);
          },
        };
        const earlier = yield* Effect.forEach(
          allowed,
          (wasAllowed) =>
            Effect.map(incoming(alice), (message) => ({
              message,
              allowed: wasAllowed,
              checkedAt: 1,
            })),
          { concurrency: 1 },
        );

        yield* firewallOf(policy([privacy], recording), {
          historyLimit,
        }).checkIncoming(yield* incoming(alice), earlier);

        expect(states).toEqual([
          {
            history: allowed
              .slice(-historyLimit)
              .map((visibleToAgent) => ({ visibleToAgent })),
            earlierHistoryOmitted: allowed.length > historyLimit,
          },
        ]);
      }),
    { fastCheck: { numRuns: 20 } },
  );
});

describe("Firewall check activity", () => {
  it.prop(
    "exports a pending span and then a settled span for each check, naming its direction",
    [
      fc.array(fc.constantFrom<Direction>("incoming", "outgoing"), {
        minLength: 1,
        maxLength: 4,
      }),
    ],
    ([directions]) =>
      Effect.gen(function* () {
        const exporter = new InMemorySpanExporter();
        const firewall = firewallOf(policy([privacy]));
        const exported = yield* Effect.forEach(
          directions,
          (direction) => check(firewall, direction),
          { concurrency: 1 },
        ).pipe(
          Effect.map(() => exporter.getFinishedSpans()),
          Effect.withSpan("checks"),
          Effect.provide(
            NodeSdk.layer(() => ({
              spanProcessor: new SimpleSpanProcessor(exporter),
            })),
          ),
        );

        expect(
          exported
            .filter(({ name }) => name.startsWith("retry."))
            .map(({ name, attributes }) => ({ name, ...attributes })),
        ).toEqual(
          directions.flatMap((id) => [
            { name: "retry.pending", kind: "firewall-check", id },
            {
              name: "retry.settled",
              kind: "firewall-check",
              id,
              outcome: "checked",
            },
          ]),
        );
      }),
    { fastCheck: { numRuns: 12 } },
  );
});

describe("Firewall replies", () => {
  it.each([
    { action: "reply", given: notice, text: privacy.feedback },
    { action: "deny", given: notice, text: notice },
    { action: "deny", given: undefined, text: undefined },
  ] as const)(
    "answers a message that breaks a $action rule with $text, and records the reply as a message it allowed",
    ({ action, given, text }) =>
      Effect.gen(function* () {
        const result = yield* check(
          firewallOf(
            policy([{ ...privacy, action }]),
            given === undefined ? {} : { notice: given },
          ),
          "incoming",
        );

        const reply = "reply" in result ? result.reply : undefined;
        expect(result).toMatchObject({ kind: "refused" });
        expect(
          reply && [
            reply.text,
            reply.record.direction,
            reply.record.decision,
            reply.record.peer,
          ],
        ).toEqual(text && [text, "outgoing", "allowed", bob]);
      }),
  );

  it("gives a refused outgoing message no reply", () =>
    Effect.gen(function* () {
      const result = yield* check(
        firewallOf(policy([{ ...privacy, action: "reply" }]), { notice }),
        "outgoing",
      );

      expect(result).toMatchObject({ kind: "refused" });
      expect(result).not.toHaveProperty("reply");
    }));
});

describe("Firewall structure check", () => {
  const refusal = {
    kind: "refused",
    reason: "rule-violation",
    ruleId: "shape",
    feedback: "Send a greeting first.",
    detail: { step: 1 },
  } as const;
  const silent = { decide: () => Effect.dieMessage("Provider asked") };

  it.each([
    { reply: "Greet me first.", text: "Greet me first." },
    { reply: undefined, text: `${notice} ${refusal.feedback}` },
  ])(
    "refuses what its structure check refuses under that check's rule without asking the provider, and answers with $text",
    ({ reply, text }) =>
      Effect.gen(function* () {
        const structureCheck: StructureCheck = () =>
          Effect.succeed(reply === undefined ? refusal : { ...refusal, reply });
        const firewall = firewallOf(policy([], silent), {
          notice,
          structureCheck,
        });

        expect(yield* check(firewall, "outgoing")).toMatchObject({
          kind: "refused",
          reason: "rule-violation",
          rule: "shape",
          feedback: [refusal.feedback],
          detail: { step: 1 },
          record: {
            decidedBy: [{ ruleId: "shape" }],
            checkedRuleIds: ["shape"],
          },
        });
        expect(yield* check(firewall, "incoming")).toMatchObject({
          kind: "refused",
          reply: { text },
        });
      }),
  );

  it("hands its structure check the earlier messages it was given", () =>
    Effect.gen(function* () {
      const handed: unknown[] = [];
      const earlier = [
        { message: yield* incoming(alice), allowed: true, checkedAt: 7 },
        { message: yield* incoming(alice), allowed: false },
      ];

      yield* firewallOf(policy(), {
        structureCheck: (...given) => {
          handed.push(given[1]);
          return Effect.succeed({ kind: "passed", textRules: [] });
        },
      }).checkIncoming(yield* incoming(alice), earlier);

      expect(handed).toEqual([earlier]);
    }));

  it("judges a message its structure check passes by that check's rules too, and answers a withheld one as the check phrases it", () =>
    Effect.gen(function* () {
      const asked: unknown[] = [];
      const recording: DecisionProvider = {
        decide: (request) => {
          asked.push(
            Schema.decodeUnknownSync(
              Schema.parseJson(Schema.Struct({ protocol: Schema.Unknown })),
            )(request.state).protocol,
            ...Object.values(request.questions).map(({ ruleId }) => ruleId),
          );
          return certainProvider.decide(request);
        },
      };

      const result = yield* check(
        firewallOf(
          policy(
            [{ id: "polite", applies: "both", text: "Be polite." }],
            recording,
          ),
          {
            notice,
            structureCheck: () =>
              Effect.succeed({
                kind: "passed",
                textRules: [new TextRule(privacy)],
                protocol: { turn: 2 },
                replyTo: (feedback) => `Refused: ${feedback}`,
              }),
          },
        ),
        "incoming",
      );

      expect(asked).toEqual([{ turn: 2 }, "polite", privacy.id]);
      expect(result).toMatchObject({
        kind: "refused",
        reply: { text: `Refused: ${privacy.feedback}` },
      });
    }));
});
