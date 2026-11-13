/** @file Canonical agent identifiers and names, an agent's own card, and name resolution among the agents of one daemon. */

import { SqlClient } from "@effect/sql";
import { scopedLive as it } from "@effect/vitest";
import { Context, Effect, Either, Layer, Schema } from "effect";
import * as fc from "fast-check";
import { describe, expect, it as vitestIt } from "vitest";
import { createTestAgent } from "./__tests__/agents.js";
import { openTestDatabase } from "./communication/__tests__/store-fixtures.js";
import { AgentId, AgentName, Identity, PrincipalId } from "./identity.js";

const IDENTIFIER_BYTE_LENGTH = 16;

const decodeSucceeds = <A, I>(
  schema: Schema.Schema<A, I>,
  value: unknown,
): boolean =>
  Schema.decodeUnknownEither(schema)(value).pipe(
    Either.match({
      onLeft: () => false,
      onRight: () => true,
    }),
  );

const expectCanonicalRoundTrip = <A>(
  schema: Schema.Schema<A, string>,
  encoded: string,
): void => {
  const value = Schema.decodeUnknownSync(schema)(encoded);
  expect(Schema.encodeSync(schema)(value)).toBe(encoded);
};

describe("canonical identifiers", () => {
  vitestIt("round-trips every canonical identifier spelling", () => {
    expect.hasAssertions();
    fc.assert(
      fc.property(
        fc.uint8Array({
          minLength: IDENTIFIER_BYTE_LENGTH,
          maxLength: IDENTIFIER_BYTE_LENGTH,
        }),
        (bytes) => {
          const payload = Buffer.from(bytes).toString("base64url");
          expectCanonicalRoundTrip(AgentId, `agt_${payload}`);
          expectCanonicalRoundTrip(PrincipalId, `prn_${payload}`);
        },
      ),
    );
  });

  vitestIt.each([
    { reason: "wrong prefix", value: `prn_${"A".repeat(22)}` },
    { reason: "short payload", value: `agt_${"A".repeat(21)}` },
    { reason: "long payload", value: `agt_${"A".repeat(23)}` },
    { reason: "padding", value: `agt_${"A".repeat(22)}=` },
    { reason: "standard alphabet plus", value: `agt_${"A".repeat(21)}+` },
    { reason: "standard alphabet slash", value: `agt_${"A".repeat(21)}/` },
    { reason: "whitespace", value: `agt_${"A".repeat(21)} ` },
    { reason: "nonzero trailing bits", value: `agt_${"A".repeat(21)}B` },
  ])("rejects a noncanonical AgentId with $reason", ({ value }) => {
    expect(decodeSucceeds(AgentId, value)).toBe(false);
  });
});

describe("AgentName", () => {
  vitestIt.each(["abc", "a".repeat(32), "agent-7"])(
    "accepts the AgentName %s without normalization",
    (value) => {
      const decoded = Schema.decodeUnknownSync(AgentName)(value);
      expect(Schema.encodeSync(AgentName)(decoded)).toBe(value);
    },
  );

  vitestIt.each([
    "ab",
    "a".repeat(33),
    "Agent",
    "agent_name",
    "agént",
    "-agent",
    "agent-",
    "agent--name",
    "agent name",
  ])("rejects the invalid AgentName %s", (value) => {
    expect(decodeSucceeds(AgentName, value)).toBe(false);
  });
});

const names = { alice: "alice", bob: "bob" };

const fixture = Effect.gen(function* () {
  const database = yield* openTestDatabase();
  return {
    alice: yield* database.run(createTestAgent(names.alice)),
    bob: yield* database.run(createTestAgent(names.bob)),
    open: (agentKey: string) =>
      Layer.build(Identity.layer(agentKey)).pipe(
        Effect.provideService(SqlClient.SqlClient, database.sql),
        Effect.map((context) => Context.get(context, Identity)),
      ),
  };
});

it("gives each agent its own card under the name it was created with", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const alice = yield* f.open(f.alice.agentKey);
    const bob = yield* f.open(f.bob.agentKey);
    expect(alice.me.agentName).toBe(names.alice);
    expect(bob.me.agentName).toBe(names.bob);
    expect(alice.me.agentId).not.toBe(bob.me.agentId);
    expect((yield* f.open(f.alice.agentKey)).me).toEqual(alice.me);
  }));

it("resolves names in input order and fails with the first unknown name", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const alice = yield* f.open(f.alice.agentKey);
    const missing = AgentName.make("missing-agent");
    expect(
      yield* alice.resolve([f.bob.card.agentName, f.alice.card.agentName]),
    ).toEqual([f.bob.card, f.alice.card]);
    expect(
      yield* alice
        .resolve([
          f.bob.card.agentName,
          missing,
          AgentName.make("also-missing"),
        ])
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "unknown-agent", agentName: missing });
  }));

it("does not open for a key the daemon has no agent under", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    expect(yield* f.open("no-such-key").pipe(Effect.flip)).toMatchObject({
      reason: "unknown-agent",
    });
  }));
