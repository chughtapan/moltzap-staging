/** @file Exact request bytes for adversarial Registry process tests. */

import { AgentPublicKey } from "@moltzap/social-harness";
import { Effect, Schema } from "effect";
import { createHash } from "node:crypto";

const sortedJsonObject = (...[, value]: [string, unknown]): unknown => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value).sort(([left], [right]) => {
      if (left < right) {
        return -1;
      }
      return Number(left > right);
    }),
  );
};

/** Fixture bodies contain JSON values and use lexicographically ordered keys. */
export const encodeCanonicalJson = (value: unknown) =>
  Effect.sync(() =>
    new TextEncoder().encode(JSON.stringify(value, sortedJsonObject)),
  );

/** Digest of the exact bytes sent by a deliberately constructed request. */
export const contentDigest = (bodyBytes: Uint8Array): string =>
  `sha-256=:${createHash("sha256").update(bodyBytes).digest("base64")}:`;

/** RFC 7638 key id derived from the public key's public Schema encoding. */
export const ed25519PublicKeyThumbprintUri = (publicKey: AgentPublicKey) =>
  Schema.encode(AgentPublicKey)(publicKey).pipe(
    Effect.flatMap(encodeCanonicalJson),
    Effect.map(
      (bytes) =>
        `urn:ietf:params:oauth:jwk-thumbprint:sha-256:${createHash("sha256").update(bytes).digest("base64url")}`,
    ),
  );
