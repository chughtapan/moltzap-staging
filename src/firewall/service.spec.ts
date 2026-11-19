/** @file Public policy operations retain safe decisions and rule edits across transactions and restart. */

import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import {
  Deferred,
  Duration,
  Effect,
  Either,
  FastCheck as fc,
  Fiber,
} from "effect";
import { describe, expect } from "vitest";
import { openTestDatabase } from "../database/testing.js";
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
import { FirewallPolicyError, type TextRule } from "./rules.js";
import { Firewall } from "./service.js";

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

it("isolates saved decisions and editable policy between agents sharing a database", () =>
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
          yield* firewall.updatePolicy({ remove: [privacy.id] });
          expect((yield* firewall.readPolicy()).rules).toEqual([]);
          return yield* firewall.readDecisions({});
        }),
      (agentKey) => firewallLayer(agentKey, policy([privacy])),
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
      (agentKey) => firewallLayer(agentKey, policy([privacy])),
    );
    yield* withFirewall(
      first,
      (firewall) =>
        Effect.gen(function* () {
          expect((yield* firewall.readPolicy()).rules).toEqual([]);
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
              from: "editable",
              advice: privacy.feedback,
            },
          ],
        },
      ]);
      const reopened = yield* withFirewall(
        { database },
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

  it("retains activation metadata and historical advice after a rule is removed", () =>
    Effect.gen(function* () {
      yield* withFirewall(
        {},
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
        (agentKey) => firewallLayer(agentKey, policy()),
      );
    }));
});

describe("Firewall.updatePolicy", () => {
  it.prop(
    "applies an edit and records its activation, or refuses it and keeps the policy and its activations",
    [edits],
    ([edit]) =>
      withFirewall(
        {},
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
        (agentKey) => firewallLayer(agentKey, policy([initial], [privacy])),
      ),
    { fastCheck: { numRuns: 24 } },
  );

  it.prop(
    "keeps every rule of additions that arrive at once",
    [
      fc.uniqueArray(fc.constantFrom("concise", "courteous", "candid"), {
        minLength: 1,
      }),
    ],
    ([ids]) =>
      withFirewall(
        {},
        (firewall) =>
          Effect.gen(function* () {
            yield* Effect.forEach(
              ids,
              (id) => firewall.updatePolicy({ add: [{ ...initial, id }] }),
              { concurrency: ids.length },
            );

            expect(
              new Set((yield* firewall.readPolicy()).rules.map(({ id }) => id)),
            ).toEqual(new Set(ids));
          }),
        (agentKey) => firewallLayer(agentKey, policy()),
      ),
    { fastCheck: { numRuns: 12 } },
  );

  it("checks a draft against the policy it started under when an edit lands mid-check, and later drafts against the edited policy", () =>
    Effect.gen(function* () {
      const asked = yield* Deferred.make<undefined>();
      const edited = yield* Deferred.make<undefined>();
      const waitsForEdit = policy([initial], [], {
        decide: (request) =>
          Deferred.succeed(asked, undefined).pipe(
            Effect.zipRight(Deferred.await(edited)),
            Effect.zipRight(certainProvider.decide(request)),
          ),
      });
      yield* withFirewall(
        {},
        (firewall, self) =>
          Effect.gen(function* () {
            const draft = yield* outgoing(self);
            const inFlight = yield* firewall
              .checkOutgoing(draft, evidence)
              .pipe(Effect.fork);
            yield* Deferred.await(asked);
            yield* firewall.updatePolicy({ add: [privacy] });
            yield* Deferred.succeed(edited, undefined);

            expect((yield* Fiber.join(inFlight)).kind).toBe("allowed");
            expect((yield* firewall.checkOutgoing(draft, evidence)).kind).toBe(
              "refused",
            );
          }),
        (agentKey) => firewallLayer(agentKey, waitsForEdit),
      );
    }));
});

describe("Firewall.layer", () => {
  it.prop(
    "should retain an edited rule after restart with different startup seeds",
    [fc.string()],
    ([text]) =>
      Effect.gen(function* () {
        const database = yield* openTestDatabase();
        const replacement = {
          ...initial,
          text: `Ask before booking: ${text}.`,
        };
        const activated = yield* withFirewall(
          { database },
          (firewall) =>
            firewall.updatePolicy({ remove: [initial.id], add: [replacement] }),
          (agentKey) => firewallLayer(agentKey, policy([initial])),
        );
        yield* withFirewall(
          { database },
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
            firewallLayer(agentKey, policy([{ ...initial, id: "new-seed" }])),
        );
      }),
    { fastCheck: { numRuns: 24 } },
  );

  it("rejects a policy that cannot fit a model request without replacing saved rules", () =>
    Effect.gen(function* () {
      const database = yield* openTestDatabase();
      const before = yield* withFirewall(
        { database },
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
        (agentKey) => firewallLayer(agentKey, policy([privacy])),
      );
      yield* withFirewall(
        { database },
        (firewall) =>
          Effect.gen(function* () {
            expect(yield* firewall.readPolicy()).toEqual(before);
            expect((yield* firewall.readDecisions({})).activations).toEqual([]);
          }),
        (agentKey) => firewallLayer(agentKey, policy([initial])),
      );
    }));
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
        (agentKey) =>
          firewallLayer(agentKey, policy([], [{ ...privacy, action }])),
      ),
  );
});
