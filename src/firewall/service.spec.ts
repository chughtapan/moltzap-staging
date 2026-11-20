/** @file The firewall retains safe decisions across transactions and restart. */

import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import {
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import {
  type Context,
  Duration,
  Effect,
  FastCheck as fc,
  Schema,
} from "effect";
import { describe, expect } from "vitest";
import type { AgentCard } from "../identity.js";
import type {
  DecisionProvider,
  EarlierMessage,
  StructureCheck,
} from "./check.js";
import { openTestDatabase } from "../__tests__/test.js";
import {
  certainProvider,
  evidence,
  firewallLayer,
  incoming,
  outgoing,
  peer,
  policy,
  privacy,
  withFirewall,
} from "./__tests__/firewall-fixtures.js";
import { type Direction, FirewallPolicyError, TextRule } from "./rules.js";
import { Firewall } from "./service.js";

/** Checks the fixtures' draft from `self`, or their peer message to it, with `firewall`. */
function check(
  firewall: Context.Tag.Service<typeof Firewall>,
  self: AgentCard,
  direction: Direction,
) {
  return Effect.gen(function* () {
    return direction === "outgoing"
      ? yield* firewall.checkOutgoing(yield* outgoing(self), evidence)
      : yield* firewall.checkIncoming(yield* incoming(self), evidence);
  });
}

it("isolates saved decisions between agents sharing a database", () =>
  Effect.gen(function* () {
    const database = yield* openTestDatabase();
    const first = { database, agentKey: "first" };
    const second = { database, agentKey: "second" };
    const saved = yield* withFirewall(
      first,
      (firewall, self) =>
        Effect.gen(function* () {
          const decision = yield* firewall.checkOutgoing(
            yield* outgoing(self),
            evidence,
          );
          yield* decision.save;
          return yield* firewall.readDecisions({});
        }),
      (agentKey) => firewallLayer(agentKey, policy([privacy])),
    );
    yield* withFirewall(
      second,
      (firewall) =>
        Effect.gen(function* () {
          expect(yield* firewall.readDecisions({})).toEqual({ decisions: [] });
        }),
      (agentKey) => firewallLayer(agentKey, policy([privacy])),
    );
    yield* withFirewall(
      first,
      (firewall) =>
        Effect.gen(function* () {
          expect(yield* firewall.readDecisions({})).toEqual(saved);
        }),
      (agentKey) => firewallLayer(agentKey, policy([privacy])),
    );
  }));

describe("Firewall.readDecisions", () => {
  it("reads a refused message's saved policy and advice after restart without configuration", () =>
    Effect.gen(function* () {
      const database = yield* openTestDatabase();
      const saved = yield* withFirewall(
        { database },
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );
            expect(decision.kind).toBe("refused");
            yield* decision.save;
            return (yield* firewall.readDecisions({})).decisions;
          }),
        (agentKey) => firewallLayer(agentKey, policy([privacy])),
      );
      expect(saved).toMatchObject([
        {
          reason: "rule-violation",
          decision: "refused",
          peer: peer("bob", 2),
          decidedBy: [
            {
              ruleId: privacy.id,
              advice: privacy.feedback,
            },
          ],
        },
      ]);
      const reopened = yield* withFirewall(
        { database },
        (firewall) =>
          firewall
            .readDecisions({})
            .pipe(Effect.map(({ decisions }) => decisions)),
        Firewall.layer,
      );
      expect(reopened).toEqual(saved);
    }));

  it("keeps the original summary and save time across concurrent and repeated saves", () =>
    Effect.gen(function* () {
      yield* withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );
            yield* Effect.all([decision.save, decision.save], {
              concurrency: 2,
            });
            const before = yield* firewall.readDecisions({});
            yield* Effect.sleep(Duration.millis(5));
            yield* decision.save;
            expect(yield* firewall.readDecisions({})).toEqual(before);
          }),
        (agentKey) => firewallLayer(agentKey, policy([privacy])),
      );
    }));
});

describe("Firewall.checkOutgoing", () => {
  it("allows every message of an agent without a message policy without reading its evidence", () =>
    withFirewall(
      {},
      (firewall, self) =>
        Effect.gen(function* () {
          const unread = () => Effect.dieMessage("Evidence was read");
          const sent = yield* firewall.checkOutgoing(
            yield* outgoing(self),
            unread,
          );
          const received = yield* firewall.checkIncoming(
            yield* incoming(self),
            unread,
          );

          expect([sent.kind, received.kind]).toEqual(["allowed", "allowed"]);
        }),
      (agentKey) => Firewall.layer(agentKey),
    ));

  it("rolls back a completed decision with the enclosing handoff transaction", () =>
    Effect.gen(function* () {
      yield* withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );
            const aborted = yield* sql
              .withTransaction(
                decision.save.pipe(
                  Effect.zipRight(
                    Effect.fail(
                      new FirewallPolicyError({ reason: "persistence-failed" }),
                    ),
                  ),
                ),
              )
              .pipe(Effect.either);
            expect(aborted._tag).toBe("Left");
            expect((yield* firewall.readDecisions({})).decisions).toEqual([]);
            yield* decision.save;
            expect((yield* firewall.readDecisions({})).decisions).toHaveLength(
              1,
            );
          }),
        (agentKey) => firewallLayer(agentKey, policy([privacy])),
      );
    }));
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
        const exported = yield* withFirewall(
          {},
          (firewall, self) =>
            Effect.forEach(
              directions,
              (direction) => check(firewall, self, direction),
              { concurrency: 1 },
            ).pipe(Effect.map(() => exporter.getFinishedSpans())),
          (agentKey) => firewallLayer(agentKey, policy([privacy])),
        ).pipe(
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

describe("Firewall text rule replies", () => {
  it.each(["deny", "reply"] as const)(
    "answers the sender of a message that breaks a %s rule with the rule's feedback only when the rule replies",
    (action) =>
      withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkIncoming(
              yield* incoming(self),
              evidence,
            );

            expect(decision.kind).toBe("withheld");
            expect("reply" in decision ? decision.reply : undefined).toBe(
              action === "reply" ? privacy.feedback : undefined,
            );
          }),
        (agentKey) => firewallLayer(agentKey, policy([{ ...privacy, action }])),
      ),
  );
});

/** The firewall of the agent `agentKey` names under `configured`, with `structureCheck` before its text rules. */
function checkedLayer(
  agentKey: string,
  configured: ReturnType<typeof policy>,
  structureCheck: StructureCheck,
) {
  return Firewall.layer(
    agentKey,
    configured.source,
    structureCheck,
    configured.provider,
  );
}

describe("Firewall structure check refusals", () => {
  const refuses: StructureCheck = () =>
    Effect.succeed({
      kind: "refused",
      reason: "rule-violation",
      ruleId: "shape",
      feedback: "Send a greeting first.",
      detail: { step: 1 },
      reply: "Greet me first.",
    });

  it.each(["incoming", "outgoing"] as const)(
    "refuses an %s message its structure check refuses, under that check's rule, without asking the provider",
    (direction) =>
      withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* check(firewall, self, direction);
            yield* decision.save;

            expect(decision).toMatchObject(
              direction === "incoming"
                ? {
                    kind: "withheld",
                    reason: "rule-violation",
                    reply: "Greet me first.",
                  }
                : {
                    kind: "refused",
                    refusal: {
                      refusal: "rule-violation",
                      rule: "shape",
                      feedback: ["Send a greeting first."],
                      detail: { step: 1 },
                    },
                  },
            );
            expect((yield* firewall.readDecisions({})).decisions).toMatchObject(
              [{ decidedBy: [{ ruleId: "shape" }], checkedRuleIds: ["shape"] }],
            );
          }),
        (agentKey) =>
          checkedLayer(
            agentKey,
            policy([], { decide: () => Effect.dieMessage("Provider asked") }),
            refuses,
          ),
      ),
  );

  it("allows its own reply to a withheld message without its structure check", () =>
    withFirewall(
      {},
      (firewall, self) =>
        Effect.gen(function* () {
          const withheld = yield* incoming(self);

          const reply = yield* firewall.checkOutgoing(
            yield* outgoing(self),
            () =>
              Effect.succeed({
                history: [],
                earlierHistoryOmitted: false,
                gaps: [],
                references: [withheld],
              }),
          );

          expect(reply.kind).toBe("allowed");
        }),
      (agentKey) => checkedLayer(agentKey, policy(), refuses),
    ));
});

describe("Firewall structure check passes", () => {
  it("hands its structure check each earlier message with whether its own saved check allowed it", () => {
    const handed: Array<readonly EarlierMessage[]> = [];
    const recording: StructureCheck = (...[, earlier]) => {
      handed.push(earlier);
      return Effect.succeed({ kind: "passed", textRules: [] });
    };
    return withFirewall(
      {},
      (firewall, self) =>
        Effect.gen(function* () {
          const [allowed, refused, unchecked] = yield* Effect.all([
            incoming(self),
            outgoing(self),
            incoming(self),
          ]);
          yield* Effect.flatMap(
            firewall.checkIncoming(allowed, evidence),
            ({ save }) => save,
          );
          yield* Effect.flatMap(
            firewall.checkOutgoing(refused, evidence),
            ({ save }) => save,
          );

          yield* firewall.checkIncoming(yield* incoming(self), () =>
            Effect.succeed({
              history: [],
              earlierHistoryOmitted: false,
              gaps: [],
              protocolHistory: {
                messages: [allowed, refused, unchecked],
                complete: true,
              },
            }),
          );

          expect(
            handed
              .at(-1)
              ?.map((earlier) => [
                earlier.message.msgId,
                earlier.allowed,
                typeof earlier.checkedAt,
              ]),
          ).toEqual([
            [allowed.msgId, true, "number"],
            [refused.msgId, false, "number"],
            [unchecked.msgId, false, "undefined"],
          ]);
        }),
      (agentKey) =>
        checkedLayer(
          agentKey,
          policy([{ ...privacy, applies: "outgoing" }]),
          recording,
        ),
    );
  });

  it("judges a message its structure check passes by that check's rules too, and answers a withheld one as the check phrases it", () => {
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
    return withFirewall(
      {},
      (firewall, self) =>
        Effect.gen(function* () {
          const decision = yield* firewall.checkIncoming(
            yield* incoming(self),
            evidence,
          );

          expect(asked).toEqual([{ turn: 2 }, "polite", privacy.id]);
          expect(decision).toMatchObject({
            kind: "withheld",
            reply: `Refused: ${privacy.feedback}`,
          });
        }),
      (agentKey) =>
        checkedLayer(
          agentKey,
          policy(
            [{ id: "polite", applies: "both", text: "Be polite." }],
            recording,
          ),
          () =>
            Effect.succeed({
              kind: "passed",
              textRules: [new TextRule(privacy)],
              protocol: { turn: 2 },
              replyTo: (feedback) => `Refused: ${feedback}`,
            }),
        ),
    );
  });
});
