/** @file Public policy operations retain safe decisions and rule edits across transactions and restart. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Duration, Effect, Either, FastCheck as fc } from "effect";
import { join } from "node:path";
import { describe, expect } from "vitest";
import type { Chain, Matcher, MessageToMatch } from "./chain.js";
import {
  always,
  evidence,
  incoming,
  never,
  outgoing,
  peer,
  policy,
  privacy,
  privateProviderResponse,
  withFirewall,
} from "./__tests__/firewall-fixtures.js";
import { FirewallPolicyError, type TextRule } from "./rules.js";
import { type ComposedMessagePolicy, Firewall } from "./service.js";

const initial = {
  id: "brief",
  applies: "both",
  text: "Keep replies brief.",
} satisfies TextRule;

/**
 * Edits of a policy that holds `initial` and the fixed rule `privacy`. They
 * remove and add ids the policy holds and ids it lacks, so the firewall
 * applies some and refuses others.
 */
const edits = fc.record({
  remove: fc.subarray([initial.id, privacy.id, "absent"]),
  add: fc.array(
    fc
      .constantFrom(initial.id, privacy.id, "concise", "courteous")
      .map((id) => ({ ...initial, id })),
    { maxLength: 3 },
  ),
});
/** A chain's chain rules, each naming the matcher that matches every message or the one that matches none. */
const chainRules = fc
  .array(
    fc.record({
      applies: fc.constantFrom<TextRule["applies"]>(
        "incoming",
        "outgoing",
        "both",
      ),
      match: fc.constantFrom("always", "never"),
      action: fc.constantFrom<"allow" | "deny" | "reply">(
        "allow",
        "deny",
        "reply",
      ),
    }),
    { maxLength: 4 },
  )
  .map((rules) =>
    rules.map((rule, index) => ({ ...rule, id: `rule-${index}` })),
  );
/** What a matcher reports with a match: a detail, and its reason when that is not a rule violation. */
const matchReport = fc.record(
  {
    reason: fc.constantFrom<"unresolved-evidence" | "check-unavailable">(
      "unresolved-evidence",
      "check-unavailable",
    ),
    detail: fc.jsonValue(),
  },
  { requiredKeys: ["detail"] },
);
/** Whether a rule replies to the sender of a message it withholds, and the reply its matcher gives, if any. */
const replyCase = fc.record({
  action: fc.constantFrom<"deny" | "reply">("deny", "reply"),
  reply: fc.option(fc.string({ minLength: 1 }), { nil: undefined }),
});

/** A matcher that matches every message and gives `reply`, if any. */
function replying(reply?: string): Matcher {
  return {
    match: () =>
      Effect.succeed(
        reply === undefined ? { matched: true } : { matched: true, reply },
      ),
  };
}

it("isolates saved decisions and editable policy between agents sharing a database", () =>
  Effect.gen(function* () {
    const filesystem = yield* FileSystem.FileSystem;
    const directory = yield* filesystem.makeTempDirectoryScoped({
      prefix: "sharness-policy-",
    });
    const filename = join(directory, "harness.sqlite3");
    const first = { filename, agentKey: "first" };
    const second = { filename, agentKey: "second" };
    const saved = yield* withFirewall(
      first,
      (firewall, self) =>
        Effect.gen(function* () {
          const decision = yield* firewall.checkOutgoing(
            yield* outgoing(self),
            evidence,
          );
          yield* decision.save;
          yield* firewall.updatePolicy({ remove: [privacy.id] });
          expect((yield* firewall.readPolicy()).rules).toEqual([]);
          return yield* firewall.readDecisions({});
        }),
      (agentKey) => Firewall.layer(agentKey, policy([privacy])),
    );
    yield* withFirewall(
      second,
      (firewall) =>
        Effect.gen(function* () {
          expect((yield* firewall.readPolicy()).rules).toEqual([privacy]);
          expect(yield* firewall.readDecisions({})).toEqual({
            decisions: [],
            activations: [],
          });
        }),
      (agentKey) => Firewall.layer(agentKey, policy([privacy])),
    );
    yield* withFirewall(
      first,
      (firewall) =>
        Effect.gen(function* () {
          expect((yield* firewall.readPolicy()).rules).toEqual([]);
          expect(yield* firewall.readDecisions({})).toEqual(saved);
        }),
      (agentKey) => Firewall.layer(agentKey, policy([privacy])),
    );
  }).pipe(Effect.provide(NodeContext.layer)));

describe("Firewall.readDecisions", () => {
  it("reads a refused message's saved policy and advice after restart without configuration", () =>
    Effect.gen(function* () {
      const filesystem = yield* FileSystem.FileSystem;
      const directory = yield* filesystem.makeTempDirectoryScoped({
        prefix: "sharness-decisions-",
      });
      const filename = join(directory, "harness.sqlite3");
      const saved = yield* withFirewall(
        filename,
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
        (agentKey) => Firewall.layer(agentKey, policy([privacy])),
      );
      expect(saved).toMatchObject([
        {
          reason: "rule-violation",
          decision: "refused",
          peer: peer("bob", 2),
          decidedBy: [
            {
              ruleId: privacy.id,
              from: "editable",
              advice: privacy.feedback?.text,
            },
          ],
        },
        { peer: peer("carol", 3) },
      ]);
      const reopened = yield* withFirewall(
        filename,
        (firewall) =>
          Effect.gen(function* () {
            expect(
              yield* firewall.readPolicy().pipe(Effect.flip),
            ).toMatchObject({
              reason: "not-configured",
            });
            return (yield* firewall.readDecisions({})).decisions;
          }),
        Firewall.layer,
      );
      expect(reopened).toEqual(saved);
      expect(JSON.stringify(reopened)).not.toContain(privateProviderResponse);
    }).pipe(Effect.provide(NodeContext.layer)));

  it("keeps the original summary and save time across concurrent and repeated saves", () =>
    Effect.gen(function* () {
      yield* withFirewall(
        ":memory:",
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
        (agentKey) => Firewall.layer(agentKey, policy([privacy])),
      );
    }));
});

describe("Firewall.checkOutgoing", () => {
  it("rolls back a completed decision with the enclosing handoff transaction", () =>
    Effect.gen(function* () {
      yield* withFirewall(
        ":memory:",
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
              2,
            );
          }),
        (agentKey) => Firewall.layer(agentKey, policy([privacy])),
      );
    }));

  it("retains activation metadata and historical advice after a rule is removed", () =>
    Effect.gen(function* () {
      yield* withFirewall(
        ":memory:",
        (firewall, self) =>
          Effect.gen(function* () {
            const installed = yield* firewall.updatePolicy({ add: [privacy] });
            const activated = yield* firewall.readDecisions({});
            expect(activated.decisions).toEqual([]);
            expect(activated.activations).toMatchObject([
              {
                activeRevision: installed.activeRevision,
                addedRuleIds: [privacy.id],
                removedRuleIds: [],
              },
            ]);
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );
            yield* decision.save;
            const saved = (yield* firewall.readDecisions({})).decisions;
            yield* firewall.updatePolicy({ remove: [privacy.id] });
            const feed = yield* firewall.readDecisions({});
            expect(feed.decisions).toEqual(saved);
            expect(feed.activations).toMatchObject([
              { addedRuleIds: [], removedRuleIds: [privacy.id] },
              { addedRuleIds: [privacy.id], removedRuleIds: [] },
            ]);
          }),
        (agentKey) => Firewall.layer(agentKey, policy()),
      );
    }));
});

describe("Firewall.updatePolicy", () => {
  it("preserves the policy when the host refuses an update", () =>
    Effect.gen(function* () {
      const configuration: ComposedMessagePolicy = {
        ...policy([initial], [privacy]),
        approvePolicyUpdate: () =>
          Effect.fail(new FirewallPolicyError({ reason: "approval-refused" })),
      };
      yield* withFirewall(
        ":memory:",
        (firewall) =>
          Effect.gen(function* () {
            const before = yield* firewall.readPolicy();
            const error = yield* firewall
              .updatePolicy({ remove: [initial.id] })
              .pipe(Effect.flip);
            expect(error.reason).toBe("approval-refused");
            expect(yield* firewall.readPolicy()).toEqual(before);
            expect((yield* firewall.readDecisions({})).activations).toEqual([]);
          }),
        (agentKey) => Firewall.layer(agentKey, configuration),
      );
    }));

  it.prop(
    "applies an edit and records its activation, or refuses it and keeps the policy and its activations",
    [edits],
    ([edit]) =>
      withFirewall(
        ":memory:",
        (firewall) =>
          Effect.gen(function* () {
            const before = yield* firewall.readPolicy();
            const result = yield* firewall
              .updatePolicy(edit)
              .pipe(Effect.either);
            const after = {
              policy: yield* firewall.readPolicy(),
              activations: (yield* firewall.readDecisions({})).activations,
            };

            expect(after).toMatchObject(
              Either.match(result, {
                onLeft: () => ({ policy: before, activations: [] }),
                onRight: ({ activeRevision }) => ({
                  policy: { activeRevision },
                  activations: [{ activeRevision }],
                }),
              }),
            );
          }),
        (agentKey) => Firewall.layer(agentKey, policy([initial], [privacy])),
      ),
  );
});

describe("Firewall.layer", () => {
  it.prop(
    "should retain an edited rule after restart with different startup seeds",
    [fc.string()],
    ([text]) =>
      Effect.gen(function* () {
        const filesystem = yield* FileSystem.FileSystem;
        const directory = yield* filesystem.makeTempDirectoryScoped({
          prefix: "sharness-policy-",
        });
        const filename = join(directory, "harness.sqlite3");
        const replacement = {
          ...initial,
          text: `Ask before booking: ${text}.`,
        };
        const activated = yield* withFirewall(
          filename,
          (firewall) =>
            firewall.updatePolicy({ remove: [initial.id], add: [replacement] }),
          (agentKey) => Firewall.layer(agentKey, policy([initial])),
        );
        yield* withFirewall(
          filename,
          (firewall) =>
            Effect.gen(function* () {
              const current = yield* firewall.readPolicy();
              expect(current.rules).toEqual([replacement]);
              expect(current.activeRevision).toBe(activated.activeRevision);
              expect(
                (yield* firewall.readDecisions({})).activations,
              ).toMatchObject([
                { addedRuleIds: [initial.id], removedRuleIds: [initial.id] },
              ]);
            }),
          (agentKey) =>
            Firewall.layer(agentKey, policy([{ ...initial, id: "new-seed" }])),
        );
      }).pipe(Effect.provide(NodeContext.layer)),
    { fastCheck: { numRuns: 24 } },
  );

  it("rejects a policy that cannot fit a model request without replacing saved rules", () =>
    Effect.gen(function* () {
      const filesystem = yield* FileSystem.FileSystem;
      const directory = yield* filesystem.makeTempDirectoryScoped({
        prefix: "sharness-policy-budget-",
      });
      const filename = join(directory, "harness.sqlite3");
      const before = yield* withFirewall(
        filename,
        (firewall, self) =>
          Effect.gen(function* () {
            const installed = yield* firewall.readPolicy();
            const rejected = yield* firewall
              .updatePolicy({
                add: [{ ...initial, text: "n".repeat(100_000) }],
              })
              .pipe(Effect.flip);
            expect(rejected.reason).toBe("build-failed");
            expect(yield* firewall.readPolicy()).toEqual(installed);
            expect(
              (yield* firewall.checkOutgoing(yield* outgoing(self), evidence))
                .kind,
            ).toBe("refused");
            return installed;
          }),
        (agentKey) => Firewall.layer(agentKey, policy([privacy])),
      );
      yield* withFirewall(
        filename,
        (firewall) =>
          Effect.gen(function* () {
            expect(yield* firewall.readPolicy()).toEqual(before);
            expect((yield* firewall.readDecisions({})).activations).toEqual([]);
          }),
        (agentKey) => Firewall.layer(agentKey, policy([initial])),
      );
    }).pipe(Effect.provide(NodeContext.layer)));

  it.prop(
    "starts only when every chain rule names a registered matcher, and otherwise names the first that does not",
    [fc.array(fc.constantFrom("always", "missing"), { maxLength: 4 })],
    ([matches]) =>
      Effect.gen(function* () {
        const rules = matches.map((match, index): Chain["rules"][number] => ({
          id: `rule-${index}`,
          applies: "both",
          match,
          action: "deny",
        }));
        const unregistered = rules.find(({ match }) => match === "missing");
        const started = yield* withFirewall(
          ":memory:",
          () => Effect.void,
          (agentKey) =>
            Firewall.layer(agentKey, undefined, {
              rules,
              matchers: { always },
            }),
        ).pipe(Effect.either);

        expect(started).toMatchObject(
          unregistered === undefined
            ? { _tag: "Right" }
            : {
                _tag: "Left",
                left: { reason: "build-failed", ruleId: unregistered.id },
              },
        );
      }),
  );
});

describe("Firewall chain actions", () => {
  it.prop(
    "decides a message by the first rule for its direction whose matcher matches, and leaves a message no rule matches to the text rules",
    [
      chainRules,
      fc.constantFrom<MessageToMatch["direction"]>("incoming", "outgoing"),
    ],
    ([rules, direction]) =>
      withFirewall(
        ":memory:",
        (firewall, self) =>
          Effect.gen(function* () {
            const decision =
              direction === "outgoing"
                ? yield* firewall.checkOutgoing(yield* outgoing(self), evidence)
                : yield* firewall.checkIncoming(
                    yield* incoming(self),
                    evidence,
                  );
            yield* decision.save;
            const saved = (yield* firewall.readDecisions({})).decisions;
            const first = rules.find(
              ({ applies, match }) =>
                match === "always" &&
                (applies === "both" || applies === direction),
            );

            expect(decision.kind === "allowed").toBe(first?.action === "allow");
            expect(
              new Set(
                saved
                  .flatMap((row) => row.decidedBy)
                  .map(({ ruleId }) => ruleId),
              ),
            ).toEqual(new Set([first?.id ?? privacy.id]));
          }),
        (agentKey) =>
          Firewall.layer(agentKey, policy([], [privacy]), {
            rules,
            matchers: { always, never },
          }),
      ),
  );

  it.prop(
    "refuses a draft with the deciding rule, its matcher's reason and its matcher's detail",
    [matchReport],
    ([report]) =>
      withFirewall(
        ":memory:",
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );
            const refusal =
              decision.kind === "refused" ? decision.refusal : undefined;

            expect(refusal).toMatchObject({
              refusal: report.reason ?? "rule-violation",
              rule: "refuse-reported",
              feedback: [],
            });
            expect(refusal?.detail).toEqual(report.detail);
            expect(refusal?.message).toContain(
              `Details: ${JSON.stringify(report.detail)}`,
            );
          }),
        (agentKey) =>
          Firewall.layer(agentKey, undefined, {
            rules: [
              {
                id: "refuse-reported",
                applies: "outgoing",
                match: "reporting",
                action: "deny",
              },
            ],
            matchers: {
              reporting: {
                match: () => Effect.succeed({ matched: true, ...report }),
              },
            },
          }),
      ),
  );

  it("refuses for an unavailable check when a matcher fails", () =>
    Effect.gen(function* () {
      const chain: Chain = {
        rules: [
          { id: "broken", applies: "both", match: "broken", action: "deny" },
        ],
        matchers: { broken: { match: () => Effect.die("matcher failed") } },
      };
      yield* withFirewall(
        ":memory:",
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );

            expect(decision).toMatchObject({
              kind: "refused",
              refusal: { refusal: "check-unavailable", rule: "broken" },
            });
          }),
        (agentKey) => Firewall.layer(agentKey, undefined, chain),
      );
    }));
});

describe("Firewall chain replies and holds", () => {
  it.prop(
    "sends its matcher's reply to the sender of a message a reply rule withholds, and nothing for a deny rule",
    [replyCase],
    ([{ action, reply }]) =>
      withFirewall(
        ":memory:",
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkIncoming(
              yield* incoming(self),
              evidence,
            );

            expect(decision).toMatchObject({
              kind: "withheld",
              visibility: [{ visibleToAgent: false }],
            });
            expect("reply" in decision ? decision.reply : undefined).toBe(
              action === "reply" ? reply : undefined,
            );
          }),
        (agentKey) =>
          Firewall.layer(agentKey, undefined, {
            rules: [
              {
                id: "not-understood",
                applies: "incoming",
                match: "replying",
                action,
              },
            ],
            matchers: { replying: replying(reply) },
          }),
      ),
  );

  it.each([
    { approved: false, refusal: "owner-approval-required", rule: "consent" },
    { approved: true, refusal: "rule-violation", rule: "refuse-drafts" },
  ] as const)(
    "holds a draft for its owner, who approves it: $approved",
    ({ approved, refusal, rule }) =>
      Effect.gen(function* () {
        const chain: Chain = {
          rules: [
            {
              id: "consent",
              applies: "outgoing",
              match: "always",
              action: "hold",
            },
            {
              id: "refuse-drafts",
              applies: "outgoing",
              match: "always",
              action: "deny",
            },
          ],
          matchers: { always },
          approveHeld: () => Effect.succeed(approved),
        };
        yield* withFirewall(
          ":memory:",
          (firewall, self) =>
            Effect.gen(function* () {
              const decision = yield* firewall.checkOutgoing(
                yield* outgoing(self),
                evidence,
              );

              expect(decision).toMatchObject({
                kind: "refused",
                refusal: { refusal, rule },
              });
            }),
          (agentKey) => Firewall.layer(agentKey, undefined, chain),
        );
      }),
  );
});

describe("Firewall text rules scoped by a matcher", () => {
  it.prop(
    "asks only about the scoped text rules whose matchers match the message",
    [fc.array(fc.constantFrom("always", "never"), { maxLength: 4 })],
    ([matches]) =>
      Effect.gen(function* () {
        const rules = matches.map((match, index) => ({
          ...initial,
          id: `rule-${index}`,
          match,
        }));
        const asked = rules.flatMap(({ id, match }) =>
          match === "always" ? [id] : [],
        );
        yield* withFirewall(
          ":memory:",
          (firewall, self) =>
            Effect.gen(function* () {
              const decision = yield* firewall.checkOutgoing(
                yield* outgoing(self),
                evidence,
              );
              yield* decision.save;

              expect(
                (yield* firewall.readDecisions({})).decisions,
              ).toMatchObject([
                { checkedRuleIds: asked },
                { checkedRuleIds: asked },
              ]);
            }),
          (agentKey) =>
            Firewall.layer(agentKey, policy([], rules), {
              rules: [],
              matchers: { always, never },
            }),
        );
      }),
  );

  it.prop(
    "answers the sender of a message that breaks a reply rule with its matcher's reply, or with the rule's feedback when the matcher gives none, and sends nothing for a deny rule",
    [replyCase],
    ([{ action, reply }]) =>
      withFirewall(
        ":memory:",
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkIncoming(
              yield* incoming(self),
              evidence,
            );

            expect(decision.kind).toBe("withheld");
            expect("reply" in decision ? decision.reply : undefined).toBe(
              action === "reply" ? (reply ?? privacy.feedback.text) : undefined,
            );
          }),
        (agentKey) =>
          Firewall.layer(
            agentKey,
            policy([], [{ ...privacy, action, match: "replying" }]),
            { rules: [], matchers: { replying: replying(reply) } },
          ),
      ),
  );
});
