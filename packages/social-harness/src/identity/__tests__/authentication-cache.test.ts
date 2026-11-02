/** @file Authenticated HTTP card retention and eviction through Registry resolution. */

import { Array, Effect, Encoding, Ref, Schema } from "effect";
import { expect, it } from "vitest";
import type { Authentication } from "../authentication.js";
import { AgentId, AgentName } from "../agent.js";
import {
  AgentIdentityCardIssuedAt,
  makeVerifiedCard,
} from "./agent-card-fixtures.js";
import {
  authenticatedHttpLayerWithRegistry,
  type IdentityFixture,
  makeIdentityFixture,
  prepareRequest,
  registryWithLookup,
  verifyAgentRequest,
} from "./authentication-fixtures.js";

const makeDistinctFixture = (index: number) =>
  Effect.gen(function* () {
    const fixture = yield* makeIdentityFixture;
    const identityCard = yield* makeVerifiedCard({
      agentId: yield* Schema.decodeUnknown(AgentId)(
        `agt_${Encoding.encodeBase64Url(new Uint8Array(16).fill(index))}`,
      ),
      principalId: fixture.identityCard.principalId,
      agentName: yield* Schema.decodeUnknown(AgentName)(
        `cache-signer-${index}`,
      ),
      publicKey: fixture.identityCard.publicKey,
      issuedAt: yield* Schema.decodeUnknown(AgentIdentityCardIssuedAt)(
        fixture.identityCard.issuedAt,
      ),
      registrySigningAuthority: fixture.registryAuthority,
    });
    return { ...fixture, identityCard };
  });

const lookupCards =
  (
    fixtures: readonly IdentityFixture[],
    lookups: Ref.Ref<readonly AgentId[]>,
  ): Parameters<typeof Authentication.layer>[0]["lookup"] =>
  (request) => {
    if (!("agentId" in request)) {
      return Effect.dieMessage("LRU lookup requires an AgentId");
    }
    const { agentId } = request;
    return Ref.update(lookups, (ids) => [...ids, agentId]).pipe(
      Effect.map(() => {
        const fixture = fixtures.find(
          (candidate) => candidate.identityCard.agentId === agentId,
        );
        return fixture === undefined
          ? ({ kind: "not_found" } as const)
          : ({ kind: "found", identityCard: fixture.identityCard } as const);
      }),
    );
  };

const makeCacheFixtures = Effect.gen(function* () {
  const cacheCapacity = 8;
  const fixtures = yield* Effect.all(
    Array.makeBy(cacheCapacity + 1, (index) => makeDistinctFixture(index)),
  );
  const first = fixtures[0];
  const second = fixtures[1];
  const newest = fixtures[cacheCapacity];
  if (first === undefined || second === undefined || newest === undefined) {
    return yield* Effect.dieMessage("LRU fixtures are missing");
  }
  return { cacheCapacity, fixtures, first, second, newest };
});

it("retains a recently used card and looks up the least recently used card after eviction", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { cacheCapacity, fixtures, first, second, newest } =
        yield* makeCacheFixtures;
      const lookups = yield* Ref.make<readonly AgentId[]>([]);
      const verifyFresh = (fixture: IdentityFixture) =>
        prepareRequest({ fixture }).pipe(Effect.flatMap(verifyAgentRequest));
      yield* Effect.gen(function* () {
        yield* Effect.forEach(fixtures.slice(0, cacheCapacity), verifyFresh, {
          concurrency: 1,
        });
        yield* verifyFresh(first);
        yield* verifyFresh(newest);
        yield* Ref.set(lookups, []);
        expect((yield* verifyFresh(first)).identityCard).toBe(
          first.identityCard,
        );
        expect(yield* Ref.get(lookups)).toStrictEqual([]);
        expect((yield* verifyFresh(second)).identityCard).toBe(
          second.identityCard,
        );
        expect(yield* Ref.get(lookups)).toStrictEqual([
          second.identityCard.agentId,
        ]);
      }).pipe(
        Effect.provide(
          authenticatedHttpLayerWithRegistry(
            registryWithLookup(lookupCards(fixtures, lookups)),
            { identityCardCacheCapacity: cacheCapacity },
          ),
        ),
      );
    }),
  ));
