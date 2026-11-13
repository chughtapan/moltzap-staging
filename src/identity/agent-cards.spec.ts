/** @file Canonical agent identifiers and names. */

import { Either, Schema } from "effect";
import * as fc from "fast-check";
import { describe, expect, it } from "vitest";
import { AgentId, AgentName, PrincipalId } from "./index.js";

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
  it("round-trips every canonical identifier spelling", () => {
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

  it.each([
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
  it.each(["abc", "a".repeat(32), "agent-7"])(
    "accepts the AgentName %s without normalization",
    (value) => {
      const decoded = Schema.decodeUnknownSync(AgentName)(value);
      expect(Schema.encodeSync(AgentName)(decoded)).toBe(value);
    },
  );

  it.each([
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
