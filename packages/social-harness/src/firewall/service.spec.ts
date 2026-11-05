/** @file Public policy operations retain safe decisions and rule edits across transactions and restart. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { scopedLive as it } from "@effect/vitest";
import {
  type Context,
  Duration,
  Effect,
  Encoding,
  FastCheck as fc,
  Layer,
  Redacted,
} from "effect";
import { join } from "node:path";
import { describe, expect } from "vitest";
import type { Chain, FirewallEvidence, Matcher } from "./chain.js";
import type { DecisionProvider } from "./check.js";
import { Message } from "../communication/messages.js";
import { migrateDaemon } from "../database/migrations.js";
import {
  type IdentityFixture,
  makeIdentityFixture,
} from "../identity/__tests__/authentication-fixtures.js";
import {
  AgentCard,
  agentCardOf,
  AgentId,
  AgentName,
  AgentSigningAuthority,
  PrincipalId,
} from "../identity/agent-cards.js";
import { RegistrationId, Registry } from "../identity/registry.js";
import { Identity } from "../identity/service.js";
import {
  FirewallPolicyError,
  MAXIMUM_BEHAVIOR_BYTES,
  MAXIMUM_EDITABLE_RULES,
  type policyUpdateSchema,
  type TextRule,
} from "./rules.js";
import { Firewall, type FirewallOptions } from "./service.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Closed policy failures and dispositions are the public contract under test. */

const privacy = {
  id: "privacy",
  applies: "both",
  text: "Do not reveal the owner's private records.",
  feedback: { text: "Keep private records private." },
} satisfies TextRule;
const evidence: FirewallEvidence = {
  history: [],
  earlierHistoryOmitted: false,
  pendingOutgoing: [],
  gaps: [],
};
const privateProviderResponse = "private provider explanation";
const judgments: DecisionProvider = {
  decide: (request) =>
    Effect.succeed({
      provider: "test",
      model: "test",
      raw: privateProviderResponse,
      usage: {},
      answers: Object.fromEntries(
        Object.entries(request.questions).map(([id, question]) => {
          const choices = Object.keys(question.criteria);
          const selected =
            question.ruleId === privacy.id ? "violation" : "compliant";
          return [
            id,
            {
              choice: selected,
              probabilities: Object.fromEntries(
                choices.map((choice) => [choice, Number(choice === selected)]),
              ),
            },
          ];
        }),
      ),
    }),
};

function withFirewall<A, E, R>(
  storage: string | { readonly filename: string; readonly agentKey: string },
  fixture: IdentityFixture,
  use: (
    firewall: Context.Tag.Service<typeof Firewall>,
    self: AgentCard,
  ) => Effect.Effect<A, E, R>,
  configure: (agentKey: string) => ReturnType<typeof Firewall.layer>,
) {
  const filename = typeof storage === "string" ? storage : storage.filename;
  const agentKey = typeof storage === "string" ? storage : storage.agentKey;
  const identity = Identity.layer({
    agentKey,
    signingAuthority: fixture.signingAuthority,
    registrySignerPublicKey: AgentSigningAuthority.publicKey(
      fixture.registryAuthority,
    ),
    admissionCredential: Effect.succeed(Redacted.make("test-admission")),
  }).pipe(
    Layer.provide(
      Layer.succeed(
        Registry,
        Registry.of({
          register: () =>
            Effect.succeed({
              kind: "registered",
              identityCard: fixture.identityCard,
            }),
          lookup: () => Effect.succeed({ kind: "not_found" }),
        }),
      ),
    ),
  );
  const layer = configure(agentKey);
  const services = layer.pipe(
    Layer.provideMerge(identity),
    Layer.provideMerge(
      Layer.effectDiscard(migrateDaemon).pipe(
        Layer.provideMerge(SqliteClient.layer({ filename })),
      ),
    ),
  );
  return Effect.scoped(
    Effect.gen(function* () {
      const local = yield* Identity;
      yield* local.register({
        operationId: RegistrationId.make(
          `opn_${Encoding.encodeBase64Url(new Uint8Array(16))}`,
        ),
        principalId: fixture.identityCard.principalId,
        agentName: fixture.identityCard.agentName,
      });
      return yield* use(yield* Firewall, agentCardOf(fixture.identityCard));
    }).pipe(Effect.provide(services)),
  );
}

function outgoing(self: AgentCard) {
  return Message.create({
    author: self,
    recipients: [peer("bob", 2), peer("carol", 3)],
    content: [
      { type: "text", text: "The private record says the owner is away." },
    ],
  });
}

function incoming(self: AgentCard) {
  return Message.create({
    author: peer("bob", 2),
    recipients: [self],
    content: [{ type: "text", text: "Hello" }],
  }).pipe(Effect.map((message) => ({ kind: "message" as const, message })));
}

function peer(name: string, byte: number): AgentCard {
  return AgentCard.make({
    agentId: AgentId.make(
      `agt_${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`,
    ),
    principalId: PrincipalId.make(
      `prn_${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`,
    ),
    agentName: AgentName.make(name),
  });
}

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

function policy(
  initialRules: readonly TextRule[] = [],
  fixedRules: readonly TextRule[] = [],
): FirewallOptions {
  return {
    facts: [],
    settings: {
      timeoutMilliseconds: 10_000,
      maximumInputBytes: 1_000_000,
      maximumResultBytes: 100_000,
    },
    text: {
      fixedRules,
      initialRules,
      provider: judgments,
      settings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 60_000,
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

const always: Matcher = { match: () => Effect.succeed({ matched: true }) };
const never: Matcher = { match: () => Effect.succeed({ matched: false }) };

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
  it("asks about a scoped text rule only for messages its matcher matches", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const chain: Chain = { rules: [], matchers: { never } };
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* outgoing(self),
              evidence,
            );

            expect(decision.kind).toBe("allowed");
          }),
        (agentKey) =>
          Firewall.layer(
            agentKey,
            policy([], [{ ...privacy, match: "never" }]),
            chain,
          ),
      );
    }));

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

  it("lists only the rules it asked about as a decision's checked rules", () =>
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
