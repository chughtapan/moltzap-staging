/** @file Public policy operations retain safe decisions and rule edits across transactions and restart. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Duration, Effect, FastCheck as fc } from "effect";
import { join } from "node:path";
import { describe, expect } from "vitest";
import type { Chain } from "./chain.js";
import { makeIdentityFixture } from "../identity/__tests__/authentication-fixtures.js";
import {
  always,
  evidence,
  incoming,
  judgments,
  never,
  outgoing,
  peer,
  policy,
  privacy,
  privateProviderResponse,
  withFirewall,
} from "./__tests__/firewall-fixtures.js";
import {
  FirewallPolicyError,
  MAXIMUM_BEHAVIOR_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type policyUpdateSchema,
  type TextRule,
} from "./rules.js";
import { Firewall, type FirewallOptions } from "./service.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Closed policy failures and dispositions are the public contract under test. */

const initial = {
  id: "brief",
  applies: "both",
  text: "Keep replies brief.",
} satisfies TextRule;
const sharedBehavior = `---
name: booking
description: How members book a meeting.
metadata:
  social-harness:
    applies: both
    guidance: Hold a slot before confirming it.
    feedback:
      text: Hold the slot first.
---
Each member holds a slot before confirming it.
`;
const refusedEdits: ReadonlyArray<{
  readonly edit: typeof policyUpdateSchema.Type;
  readonly refusal: Readonly<Record<string, unknown>>;
}> = [
  {
    edit: { add: [initial] },
    refusal: {
      reason: "invalid-rule",
      problem: "duplicate-id",
      ruleId: initial.id,
      path: ["add", 0],
    },
  },
  {
    edit: {
      add: [
        { ...initial, id: "twice" },
        { ...initial, id: "twice" },
      ],
    },
    refusal: {
      reason: "invalid-rule",
      problem: "duplicate-id",
      ruleId: "twice",
      path: ["add", 1],
    },
  },
  {
    edit: { remove: ["absent"] },
    refusal: {
      reason: "invalid-rule",
      problem: "unknown-id",
      ruleId: "absent",
      path: ["remove", 0],
    },
  },
  {
    edit: {
      add: [...Array.from({ length: MAXIMUM_EDITABLE_RULES }).keys()].map(
        (index) => ({
          ...initial,
          id: `extra-${index}`,
        }),
      ),
    },
    refusal: { reason: "invalid-rule", problem: "editable-limit" },
  },
  {
    edit: { remove: [privacy.id] },
    refusal: { reason: "fixed-rule", ruleId: privacy.id, path: ["remove", 0] },
  },
  {
    edit: { add: [privacy] },
    refusal: { reason: "fixed-rule", ruleId: privacy.id, path: ["add", 0] },
  },
  {
    edit: { add: [{ behavior: "Hold the slot first." }] },
    refusal: {
      reason: "invalid-rule",
      problem: "frontmatter",
      path: ["add", 0],
    },
  },
  {
    edit: {
      add: [{ behavior: sharedBehavior.replace("    applies: both\n", "") }],
    },
    refusal: { reason: "invalid-rule", problem: "schema", path: ["add", 0] },
  },
  {
    edit: {
      add: [
        { behavior: sharedBehavior.padEnd(MAXIMUM_BEHAVIOR_BYTES + 1, " ") },
      ],
    },
    refusal: { reason: "invalid-rule", problem: "too-large", path: ["add", 0] },
  },
];

function editablePolicy(): FirewallOptions {
  const configured = policy([initial]);
  return {
    ...configured,
    text: {
      ...configured.text,
      fixedRules: [privacy],
      initialRules: [initial],
      provider: judgments,
      settings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 1_000_000,
        timeoutMilliseconds: 5_000,
      },
    },
  };
}

it("isolates saved decisions and editable policy between agents sharing a database", () =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    const filesystem = yield* FileSystem.FileSystem;
    const directory = yield* filesystem.makeTempDirectoryScoped({
      prefix: "sharness-policy-",
    });
    const filename = join(directory, "harness.sqlite3");
    const first = { filename, agentKey: "first" };
    const second = { filename, agentKey: "second" };
    const saved = yield* withFirewall(
      first,
      fixture,
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
      fixture,
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
      fixture,
      (firewall) =>
        Effect.gen(function* () {
          expect((yield* firewall.readPolicy()).rules).toEqual([]);
          expect(yield* firewall.readDecisions({})).toEqual(saved);
        }),
      (agentKey) => Firewall.layer(agentKey, policy([privacy])),
    );
  }).pipe(Effect.provide(NodeContext.layer)));

describe("Firewall.readDecisions", () => {
  // @agent-code-guard/regression-only: Public policy operations exercise real SQLite edits, retained evidence and transaction rollback.
  it("reads a refused message's saved policy and advice after restart without configuration", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const filesystem = yield* FileSystem.FileSystem;
      const directory = yield* filesystem.makeTempDirectoryScoped({
        prefix: "sharness-decisions-",
      });
      const filename = join(directory, "harness.sqlite3");
      const saved = yield* withFirewall(
        filename,
        fixture,
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
          disposition: "withheld",
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
        fixture,
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
      const fixture = yield* makeIdentityFixture;
      yield* withFirewall(
        ":memory:",
        fixture,
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
      const fixture = yield* makeIdentityFixture;
      yield* withFirewall(
        ":memory:",
        fixture,
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
      const fixture = yield* makeIdentityFixture;
      yield* withFirewall(
        ":memory:",
        fixture,
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
      const fixture = yield* makeIdentityFixture;
      const configuration: FirewallOptions = {
        ...editablePolicy(),
        approvePolicyUpdate: () =>
          Effect.fail(new FirewallPolicyError({ reason: "approval-refused" })),
      };
      yield* withFirewall(
        ":memory:",
        fixture,
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

  it.each(refusedEdits)(
    "refuses an invalid edit and preserves active policy: $refusal",
    ({ edit, refusal }) =>
      Effect.gen(function* () {
        const fixture = yield* makeIdentityFixture;
        yield* withFirewall(
          ":memory:",
          fixture,
          (firewall) =>
            Effect.gen(function* () {
              const before = yield* firewall.readPolicy();
              expect(
                yield* firewall.updatePolicy(edit).pipe(Effect.flip),
              ).toMatchObject(refusal);
              expect(yield* firewall.readPolicy()).toEqual(before);
              expect((yield* firewall.readDecisions({})).activations).toEqual(
                [],
              );
            }),
          (agentKey) => Firewall.layer(agentKey, editablePolicy()),
        );
      }),
  );

  it("installs shared behavior as an editable rule", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall) =>
          Effect.gen(function* () {
            yield* firewall.updatePolicy({
              add: [{ behavior: sharedBehavior }],
            });
            expect((yield* firewall.readPolicy()).rules).toContainEqual({
              id: "booking",
              applies: "both",
              text: "Each member holds a slot before confirming it.",
              guidance: "Hold a slot before confirming it.",
              feedback: { text: "Hold the slot first." },
            });
          }),
        (agentKey) => Firewall.layer(agentKey, editablePolicy()),
      );
    }));
});

describe("Firewall.layer", () => {
  it.prop(
    "should retain an edited rule after restart with different startup seeds",
    [fc.string()],
    ([text]) =>
      Effect.gen(function* () {
        const fixture = yield* makeIdentityFixture;
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
          fixture,
          (firewall) =>
            firewall.updatePolicy({ remove: [initial.id], add: [replacement] }),
          (agentKey) => Firewall.layer(agentKey, policy([initial])),
        );
        yield* withFirewall(
          filename,
          fixture,
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
      const fixture = yield* makeIdentityFixture;
      const filesystem = yield* FileSystem.FileSystem;
      const directory = yield* filesystem.makeTempDirectoryScoped({
        prefix: "sharness-policy-budget-",
      });
      const filename = join(directory, "harness.sqlite3");
      const before = yield* withFirewall(
        filename,
        fixture,
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
        fixture,
        (firewall) =>
          Effect.gen(function* () {
            expect(yield* firewall.readPolicy()).toEqual(before);
            expect((yield* firewall.readDecisions({})).activations).toEqual([]);
          }),
        (agentKey) => Firewall.layer(agentKey, policy([initial])),
      );
    }).pipe(Effect.provide(NodeContext.layer)));
});

describe("Firewall rule chain actions", () => {
  it("lets the first rule for the message's direction decide before the text rules", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const chain: Chain = {
        rules: [
          {
            id: "drop-incoming",
            applies: "incoming",
            match: "always",
            action: "deny",
          },
          {
            id: "allow-drafts",
            applies: "outgoing",
            match: "always",
            action: "allow",
          },
          {
            id: "refuse-drafts",
            applies: "outgoing",
            match: "always",
            action: "deny",
          },
        ],
        matchers: { always },
      };
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );
            yield* decision.save;

            expect(decision.kind).toBe("allowed");
            expect((yield* firewall.readDecisions({})).decisions).toMatchObject(
              [
                {
                  outcome: "allow",
                  disposition: "admitted",
                  decidedBy: [{ ruleId: "allow-drafts", from: "chain" }],
                },
                { outcome: "allow", disposition: "admitted" },
              ],
            );
          }),
        (agentKey) => Firewall.layer(agentKey, policy([privacy]), chain),
      );
    }));

  it("refuses a draft with the deciding rule and its matcher's detail", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const chain: Chain = {
        rules: [
          {
            id: "refuse-unparsed",
            applies: "outgoing",
            match: "unparsed",
            action: "deny",
          },
        ],
        matchers: {
          unparsed: {
            match: () =>
              Effect.succeed({
                matched: true,
                detail: { accepts: ["inform"] },
              }),
          },
        },
      };
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );
            const refusal =
              decision.kind === "refused" ? decision.refusal : undefined;

            expect(refusal).toMatchObject({
              refusal: "rule-violation",
              rule: "refuse-unparsed",
              feedback: [],
              detail: { accepts: ["inform"] },
            });
            expect(refusal?.message).toContain(
              'Details: {"accepts":["inform"]}',
            );
          }),
        (agentKey) => Firewall.layer(agentKey, undefined, chain),
      );
    }));
});

describe("Firewall text rules scoped by a matcher", () => {
  it("refuses a draft that breaks a scoped text rule its matcher matches", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const chain: Chain = { rules: [], matchers: { always } };
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );

            expect(decision).toMatchObject({
              kind: "refused",
              refusal: {
                refusal: "rule-violation",
                feedback: [privacy.feedback],
              },
            });
          }),
        (agentKey) =>
          Firewall.layer(
            agentKey,
            policy([], [{ ...privacy, match: "always" }]),
            chain,
          ),
      );
    }));

  it("does not ask about a scoped rule for a message its matcher does not match", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const plain = {
        ...privacy,
        id: "plain-language",
        text: "Write in plain language.",
      } satisfies TextRule;
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );
            yield* decision.save;

            expect(decision.kind).toBe("allowed");
            expect((yield* firewall.readDecisions({})).decisions).toMatchObject(
              [{ checkedRuleIds: [plain.id] }, { checkedRuleIds: [plain.id] }],
            );
          }),
        (agentKey) =>
          Firewall.layer(
            agentKey,
            policy([], [{ ...privacy, match: "never" }, plain]),
            { rules: [], matchers: { never } },
          ),
      );
    }));

  it("replies with its matcher's reply when a scoped reply rule withholds a message", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const answer =
        "(not-understood :ontology feedback :reason rule-violation)";
      const chain: Chain = {
        rules: [],
        matchers: {
          answering: {
            match: () => Effect.succeed({ matched: true, reply: answer }),
          },
        },
      };
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkIncoming(
              yield* incoming(self),
              evidence,
            );

            expect(decision).toMatchObject({ kind: "withheld", reply: answer });
          }),
        (agentKey) =>
          Firewall.layer(
            agentKey,
            policy([], [{ ...privacy, action: "reply", match: "answering" }]),
            chain,
          ),
      );
    }));

  it("refuses to install a text rule whose matcher is not registered", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const failure = yield* withFirewall(
        ":memory:",
        fixture,
        () => Effect.void,
        (agentKey) =>
          Firewall.layer(
            agentKey,
            policy([], [{ ...privacy, match: "missing" }]),
            { rules: [], matchers: {} },
          ),
      ).pipe(Effect.flip);

      expect(failure).toMatchObject({
        reason: "build-failed",
        ruleId: privacy.id,
      });
    }));
});

describe("Firewall rule chain replies and holds", () => {
  it("withholds an incoming message with the reply of a reply rule", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const chain: Chain = {
        rules: [
          {
            id: "not-understood",
            applies: "incoming",
            match: "unparsed",
            action: "reply",
          },
        ],
        matchers: {
          unparsed: {
            match: () =>
              Effect.succeed({ matched: true, reply: "(not-understood)" }),
          },
        },
      };
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkIncoming(
              yield* incoming(self),
              evidence,
            );

            expect(decision).toMatchObject({
              kind: "withheld",
              reply: "(not-understood)",
              visibility: [{ visibleToAgent: false }],
            });
          }),
        (agentKey) => Firewall.layer(agentKey, undefined, chain),
      );
    }));

  it.each([
    { approved: false, refusal: "owner-approval-required", rule: "consent" },
    { approved: true, refusal: "rule-violation", rule: "refuse-drafts" },
  ] as const)(
    "holds a draft for its owner, who approves it: $approved",
    ({ approved, refusal, rule }) =>
      Effect.gen(function* () {
        const fixture = yield* makeIdentityFixture;
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
          fixture,
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

describe("Firewall rule chain beside text rules", () => {
  it("leaves a message no chain rule matches to the text rules", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const chain: Chain = {
        rules: [
          { id: "unused", applies: "both", match: "never", action: "deny" },
        ],
        matchers: { never },
      };
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );

            expect(decision).toMatchObject({
              kind: "refused",
              refusal: {
                refusal: "rule-violation",
                feedback: [privacy.feedback],
              },
            });
          }),
        (agentKey) => Firewall.layer(agentKey, policy([privacy]), chain),
      );
    }));

  it("refuses for an unavailable check when a matcher fails", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const chain: Chain = {
        rules: [
          { id: "broken", applies: "both", match: "broken", action: "deny" },
        ],
        matchers: { broken: { match: () => Effect.die("matcher failed") } },
      };
      yield* withFirewall(
        ":memory:",
        fixture,
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

  it("refuses to install a rule whose matcher is not registered", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const chain: Chain = {
        rules: [
          { id: "unknown", applies: "both", match: "missing", action: "deny" },
        ],
        matchers: {},
      };
      const failure = yield* withFirewall(
        ":memory:",
        fixture,
        () => Effect.void,
        (agentKey) => Firewall.layer(agentKey, undefined, chain),
      ).pipe(Effect.flip);

      expect(failure).toMatchObject({
        reason: "build-failed",
        ruleId: "unknown",
      });
    }));

  it("replies with the feedback of a violated text rule whose action is reply", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkIncoming(
              yield* incoming(self),
              evidence,
            );

            expect(decision).toMatchObject({
              kind: "withheld",
              reply: privacy.feedback.text,
            });
          }),
        (agentKey) =>
          Firewall.layer(agentKey, policy([{ ...privacy, action: "reply" }])),
      );
    }));
});
/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after policy contract assertions. */
