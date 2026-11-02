/** @file Canonical host destinations select the intended conversation. */

import { live as it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { expect } from "vitest";
import {
  maximumMembers,
  MessageAddressInput,
} from "../communication/records/values.js";
import { canonicalMessageAddress } from "./messages.js";

it.each([
  { input: "agent:agent-2", expected: "agent:agent-2" },
  { input: "group:agent-2", expected: "agent:agent-2" },
  { input: "group:agent-1,agent-2", expected: "agent:agent-2" },
  {
    input: "group:agent-2,agent-10",
    expected: "group:agent-1,agent-10,agent-2",
  },
])("canonicalizes $input as $expected", ({ input, expected }) =>
  Effect.gen(function* () {
    const to = yield* Schema.decodeUnknown(MessageAddressInput)(input);
    expect((yield* canonicalMessageAddress(to, "agent-1")).address).toBe(
      expected,
    );
  }),
);

const oversized = Array.from(
  Array(maximumMembers).keys(),
  (index) => `remote-${String(index)}`,
).join(",");

it.each([
  "agent:agent-1",
  "group:agent-1",
  "group:agent-2,agent-2",
  `group:${oversized}`,
])("refuses invalid membership in %s", (input) =>
  Effect.gen(function* () {
    const to = yield* Schema.decodeUnknown(MessageAddressInput)(input);
    const rejected = yield* canonicalMessageAddress(to, "agent-1").pipe(
      Effect.flip,
    );
    expect(rejected).toMatchObject({ reason: "membership-invalid" });
  }),
);
