/** @file OAuth admission validates owner-bound JWTs against a real issuer key-set endpoint. */
import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc, Ref } from "effect";
import { expect } from "vitest";
import { mcpFixture, mcpRequest } from "./__tests__/mcp-fixtures.js";

const resource = new URL("https://mcp.example.test/mcp");
const owner = "owner-subject";
const scope = "use:mcp";

const fixture = Effect.gen(function* () {
  const test = yield* mcpFixture({ resource });
  const token = (
    changes: {
      readonly audience?: string;
      readonly subject?: string;
      readonly granted?: readonly string[];
      readonly secondsLeft?: number;
    } = {},
  ) =>
    test.issuer.token(
      changes.subject ?? owner,
      changes.audience ?? resource.href,
      {
        scope: (changes.granted ?? [scope]).join(" "),
        secondsLeft: changes.secondsLeft ?? 300,
      },
    );
  const call = (bearer: string) =>
    Effect.tryPromise(() =>
      test.fetch(
        mcpRequest(
          "server/discover",
          {},
          {
            headers: {
              host: resource.host,
              authorization: `Bearer ${bearer}`,
            },
          },
        ),
      ),
    );
  return { ...test, token, call };
});

/** A scope that begins with the configured one and goes on past it. */
const longerScope = fc
  .stringMatching(/^[a-z:]{1,3}$/u)
  .map((more) => `${scope}${more}`);

/** Claims the resource admits: any subject, and the configured scope anywhere among those granted. */
const admissible = fc.record({
  subject: fc.string({ minLength: 1 }),
  granted: fc
    .tuple(fc.array(longerScope), fc.array(longerScope))
    .map(([before, after]) => [...before, scope, ...after]),
});

/** One claim that puts a token outside the resource's authority. */
const defect = fc.oneof(
  fc.record({ subject: fc.constant("") }),
  fc.record({ audience: fc.webUrl().filter((url) => url !== resource.href) }),
  fc.record({ granted: fc.array(longerScope) }),
  fc.record({ secondsLeft: fc.integer({ min: -3600, max: 0 }) }),
);

it("selects the subject of a token granted the scope, under the verified issuer", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(admissible, (claims) =>
          Effect.gen(function* () {
            yield* Ref.set(test.owners, []);
            const response = yield* test.call(yield* test.token(claims));
            expect(response.status).toBe(200);
            expect(yield* Ref.get(test.owners)).toEqual([
              { issuer: test.issuer.issuer, subject: claims.subject },
            ]);
          }).pipe(Effect.runPromise),
        ),
      ),
    );
  }));

it("refuses a token with an empty subject, another audience, no grant of the scope or no time left", () =>
  Effect.gen(function* () {
    const test = yield* fixture;
    yield* Effect.tryPromise(() =>
      fc.assert(
        fc.asyncProperty(defect, (claim) =>
          Effect.gen(function* () {
            const response = yield* test.call(yield* test.token(claim));
            expect(response.ok).toBe(false);
            expect(yield* Ref.get(test.owners)).toEqual([]);
          }).pipe(Effect.runPromise),
        ),
      ),
    );
  }));

it(
  "closes an admitted stream at token expiry without closing later requests",
  () =>
    Effect.gen(function* () {
      const test = yield* fixture;
      const token = yield* test.token({ secondsLeft: 2 });
      const response = yield* Effect.tryPromise(() =>
        test.fetch(
          mcpRequest(
            "subscriptions/listen",
            { notifications: { toolsListChanged: true } },
            {
              headers: {
                host: resource.host,
                authorization: `Bearer ${token}`,
              },
            },
          ),
        ),
      );
      expect(response.status).toBe(200);
      const body = yield* Effect.tryPromise(() => response.text());
      expect(body).toContain("notifications/subscriptions/acknowledged");
      const later = yield* test.call(yield* test.token());
      expect(later.status).toBe(200);
    }),
  15000,
);
