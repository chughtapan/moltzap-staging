/** @file A message identifier encodes 32 bytes, and a message is created only with well-formed text for another agent. */

import { effect as it } from "@effect/vitest";
import { Effect, Encoding, FastCheck as fc, Schema } from "effect";
import { describe, expect, it as vitestIt } from "vitest";
import { digest, testAgentCard } from "./__tests__/agents.js";
import { Message, MsgId } from "./messages.js";

const alice = testAgentCard("alice");
const bob = testAgentCard("bob");
const msgId = Schema.decodeUnknownSync(MsgId)(digest("msg_", 1));

describe("identifiers", () => {
  vitestIt(
    "accepts a message identifier exactly when it encodes 32 bytes",
    () => {
      fc.assert(
        fc.property(fc.uint8Array({ maxLength: 48 }), (bytes) => {
          expect(
            Schema.is(MsgId)(`msg_${Encoding.encodeBase64Url(bytes)}`),
          ).toBe(bytes.length === 32);
        }),
      );
    },
  );
});

describe("Message", () => {
  vitestIt("rejects the author as its recipient", () => {
    expect(
      Schema.is(Message)({ msgId, author: alice, recipient: alice, text: "" }),
    ).toBe(false);
  });

  it.prop(
    "creates a message exactly when its text is well formed and at most 32,768 bytes as JSON text",
    [
      fc.oneof(
        fc
          .integer({ min: 32_700, max: 32_800 })
          .map((size) => "a".repeat(size)),
        fc.constantFrom("", "\ud800", "a\udc00", "\u{1F600}"),
      ),
    ],
    ([text]) =>
      Effect.gen(function* () {
        const created = yield* Message.create({
          author: bob,
          recipient: alice,
          text,
        }).pipe(Effect.either);

        expect(created._tag).toBe(
          text.length + 2 <= 32_768 && text !== "\ud800" && text !== "a\udc00"
            ? "Right"
            : "Left",
        );
      }),
  );

  it("gives each created message its own message id", () =>
    Effect.gen(function* () {
      const create = Message.create({
        author: bob,
        recipient: alice,
        text: "hello",
      });

      expect((yield* create).msgId).not.toBe((yield* create).msgId);
    }));
});
