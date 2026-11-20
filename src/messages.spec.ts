/** @file A message identifier encodes 32 bytes, and a message is created only with well-formed text within the size limit. */

import { effect as it } from "@effect/vitest";
import { Effect, Encoding, FastCheck as fc, Schema } from "effect";
import { expect, test } from "vitest";
import { AgentName, Message, MsgId } from "./messages.js";

test("accepts a message identifier exactly when it encodes 32 bytes", () => {
  fc.assert(
    fc.property(fc.uint8Array({ maxLength: 48 }), (bytes) => {
      expect(Schema.is(MsgId)(`msg_${Encoding.encodeBase64Url(bytes)}`)).toBe(
        bytes.length === 32,
      );
    }),
  );
});

it.prop(
  "creates a message exactly when its text is well formed and at most 32,768 bytes as JSON text",
  [
    fc.oneof(
      fc.integer({ min: 32_700, max: 32_800 }).map((size) => "a".repeat(size)),
      fc.constantFrom("", "\ud800", "a\udc00", "\u{1F600}"),
    ),
  ],
  ([text]) =>
    Effect.gen(function* () {
      const created = yield* Message.create({
        author: AgentName.make("bob"),
        recipient: AgentName.make("alice"),
        text,
      }).pipe(Effect.either);

      expect(created._tag).toBe(
        text.length + 2 <= 32_768 && text !== "\ud800" && text !== "a\udc00"
          ? "Right"
          : "Left",
      );
    }),
);
