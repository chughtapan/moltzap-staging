/** @file Live admission stays within the binary window while history follows its configured window. */

import { effect as it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
import { WIRE_VERSION, WireVersions } from "./version.js";

const otherVersion = `${WIRE_VERSION}0`;

it("keeps live admission within the binary's window", () =>
  Effect.gen(function* () {
    const admitted = yield* WireVersions.live(otherVersion);
    expect(admitted).toBe(false);
  }).pipe(
    Effect.provideService(WireVersions, {
      live: [WIRE_VERSION, otherVersion],
      history: [WIRE_VERSION, otherVersion],
    }),
  ));

it("reads configured history independently of live admission", () =>
  Effect.gen(function* () {
    const version = yield* Schema.decodeUnknown(WireVersions.history)(
      otherVersion,
    );
    expect(version).toBe(otherVersion);
  }).pipe(
    Effect.provideService(WireVersions, {
      live: [WIRE_VERSION],
      history: [otherVersion],
    }),
  ));

it("keeps the binary's history readable with an empty configured window", () =>
  Effect.gen(function* () {
    const version = yield* Schema.decodeUnknown(WireVersions.history)(
      WIRE_VERSION,
    );
    expect(version).toBe(WIRE_VERSION);
  }).pipe(Effect.provideService(WireVersions, { live: [], history: [] })));
