/** @file A retained card and signed envelope survive beyond live admission. */

import { Effect, Schema } from "effect";
import { expect, it } from "vitest";

import {
  decodeCanonicalJson,
  encodeCanonicalJson,
} from "../../encoding/canonical-json.js";
import { AgentIdentityCard, AgentPublicKey } from "../agent.js";
import golden from "../envelope/__tests__/wire-golden.json" with { type: "json" };
import { SignedMessage } from "../envelope/signed-message.js";
import { WIRE_VERSION, WireVersions } from "../wire-version.js";

/** Reproduce the fixed signed bytes with pnpm nx run workspace:generate-wire-golden. */
const retainedVersion = "2099.101.0";

it.each([
  { name: "card", representation: golden.card },
  { name: "message", representation: golden.message },
])(
  "refuses the retired version field in a $name even alongside the current field",
  ({ representation }) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const fields = yield* decodeCanonicalJson(
          Schema.Record({ key: Schema.String, value: Schema.Unknown }),
          Buffer.from(representation.payload, "base64url"),
        );
        const { sharnessVersion, ...rest } = fields;
        const legacyOnly = yield* encodeCanonicalJson({
          ...rest,
          moltzapVersion: sharnessVersion,
        });
        const both = yield* encodeCanonicalJson({
          ...fields,
          moltzapVersion: sharnessVersion,
        });
        const decode = Schema.decodeUnknown(
          Schema.Union(AgentIdentityCard, SignedMessage),
        );
        expect(yield* decode(representation).pipe(Effect.isSuccess)).toBe(true);
        expect(
          yield* decode({
            ...representation,
            payload: Buffer.from(legacyOnly).toString("base64url"),
          }).pipe(Effect.isFailure),
        ).toBe(true);
        expect(
          yield* decode({
            ...representation,
            payload: Buffer.from(both).toString("base64url"),
          }).pipe(Effect.isFailure),
        ).toBe(true);
      }).pipe(
        Effect.provideService(WireVersions, {
          live: [],
          history: [retainedVersion],
        }),
      ),
    ),
);

// @agent-code-guard/regression-only: Fixed signed golden artifacts pin retained-artifact and admission regressions.
it("verifies golden card and signed bytes without changing their representation", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const agentCard = yield* Schema.decodeUnknown(AgentIdentityCard)(
        golden.card,
      );
      const verifiedCard = yield* AgentIdentityCard.verify({
        identityCard: agentCard,
        registrySignerPublicKey: Schema.decodeUnknownSync(AgentPublicKey)(
          golden.registryKey,
        ),
      });
      const message = yield* Schema.decodeUnknown(SignedMessage)(
        golden.message,
      );
      yield* SignedMessage.verify({
        signedMessage: message,
        identityCard: verifiedCard,
      });
      expect(yield* Schema.encode(SignedMessage)(message)).toEqual(
        golden.message,
      );
      expect(yield* Schema.encode(AgentIdentityCard)(verifiedCard)).toEqual(
        golden.card,
      );
      expect(
        yield* Schema.decodeUnknown(SignedMessage.live)(golden.message).pipe(
          Effect.isFailure,
        ),
      ).toBe(true);
    }).pipe(
      Effect.provideService(WireVersions, {
        live: [WIRE_VERSION],
        history: [retainedVersion, WIRE_VERSION],
      }),
    ),
  ));

it("cannot widen live admission even when the supplied live set includes a retained version", () =>
  Effect.runPromise(
    Schema.decodeUnknown(SignedMessage.live)(golden.message).pipe(
      Effect.isFailure,
      Effect.provideService(WireVersions, {
        live: [retainedVersion, WIRE_VERSION],
        history: [retainedVersion, WIRE_VERSION],
      }),
      Effect.tap((rejected) =>
        Effect.sync(() => {
          expect(rejected).toBe(true);
        }),
      ),
    ),
  ));

it("keeps the binary's retained history when configured history is empty", () =>
  Effect.runPromise(
    Schema.decodeUnknown(WireVersions.history)(WIRE_VERSION).pipe(
      Effect.provideService(WireVersions, { live: [], history: [] }),
      Effect.tap((version) =>
        Effect.sync(() => {
          expect(version).toBe(WIRE_VERSION);
        }),
      ),
    ),
  ));

it("exposes a signed envelope's retained version independently from the binary", () =>
  Effect.runPromise(
    Schema.decodeUnknown(SignedMessage)(golden.message).pipe(
      Effect.provideService(WireVersions, {
        live: [],
        history: [retainedVersion],
      }),
      Effect.tap((message) =>
        Effect.sync(() => {
          expect(SignedMessage.wireVersion(message)).toBe(retainedVersion);
        }),
      ),
    ),
  ));
