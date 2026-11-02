/**
 * @file Fixed FIRST_MESSAGE and MESSAGE records keep canonical bytes and hashes and verify
 * under the current wire policy with deterministic synthetic test keys.
 * Reproduce with pnpm nx run workspace:generate-client-record-golden.
 */

import { live as it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { describe, expect } from "vitest";

import {
  decodeCanonical,
  encodeCanonical,
} from "../../encoding/canonical-json.js";
import { AgentPublicKey } from "../../identity/keys.js";
import { hashAction, hashRecord } from "./encoding.js";
import golden from "./record-golden.json" with { type: "json" };
import {
  ActionCertifiedRecord,
  CertifiedRecord,
  DirectPacket,
  MembershipDescriptor,
} from "./schemas.js";
import {
  verifyActionCertifiedRecord,
  verifyCertifiedRecord,
  verifyMembershipDescriptor,
} from "./verification.js";

const utf8 = new TextEncoder();

/** Each fixture holds the Registry key and membership needed to verify it offline. */
function membershipOf(form: (typeof golden.forms)[number]) {
  return Effect.gen(function* () {
    const key = yield* Schema.decodeUnknown(AgentPublicKey)(
      form.registrySignerPublicKey,
    );
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptor)(
      form.membership,
    );
    return yield* verifyMembershipDescriptor(descriptor, key);
  });
}

describe("canonical record golden bytes", () => {
  it.each(
    golden.forms.filter((form) => form.kind === "action_certified_record"),
  )("preserves $name", (form) =>
    Effect.gen(function* () {
      const bytes = utf8.encode(form.bytes);
      const record = yield* decodeCanonical(ActionCertifiedRecord, bytes);
      expect(yield* encodeCanonical(DirectPacket, record)).toEqual(bytes);
      expect(yield* hashRecord(record.recordCore)).toBe(form.recordHash);
      expect(yield* hashAction(record.recordCore.action)).toBe(form.actionHash);
      yield* verifyActionCertifiedRecord({
        record,
        membership: yield* membershipOf(form),
      });
    }),
  );
  it.each(golden.forms.filter((form) => form.kind === "certified_record"))(
    "preserves $name",
    (form) =>
      Effect.gen(function* () {
        const bytes = utf8.encode(form.bytes);
        const record = yield* decodeCanonical(CertifiedRecord, bytes);
        expect(yield* encodeCanonical(DirectPacket, record)).toEqual(bytes);
        expect(yield* hashRecord(record.actionCertifiedRecord.recordCore)).toBe(
          form.recordHash,
        );
        expect(
          yield* hashAction(record.actionCertifiedRecord.recordCore.action),
        ).toBe(form.actionHash);
        yield* verifyCertifiedRecord({
          record,
          membership: yield* membershipOf(form),
        });
      }),
  );
});
