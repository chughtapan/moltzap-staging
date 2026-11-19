/** @file Message contracts preserve public values and content. */

import { effect as it } from "@effect/vitest";
import { Effect, Encoding, Exit, FastCheck as fc, Schema } from "effect";
import { expect, it as vitestIt } from "vitest";
import { digest, testAgentCard } from "../__tests__/agents.js";
import { Content, Message, messageWithId, MsgId } from "./messages.js";

const alice = testAgentCard("alice");

const bob = testAgentCard("bob");

const msgId = Schema.decodeUnknownSync(MsgId)(digest("msg_", 1));

const content = [{ type: "text", text: "Choose a meeting time." }] as const;

it("rejects the author as a recipient", () =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknown(Message)({
      msgId,
      author: alice,
      recipients: [alice],
      content,
    }).pipe(Effect.exit);

    expect(Exit.isFailure(decoded)).toBe(true);
  }));

it("captures nested content independently of the caller's objects", () =>
  Effect.gen(function* () {
    const data = { times: ["monday"] };
    const message = yield* messageWithId(alice, [bob], { msgId }, [
      { type: "data", value: data },
    ]);
    data.times.push("tuesday");
    expect(message.content).toEqual([
      { type: "data", value: { times: ["monday"] } },
    ]);
  }));

vitestIt(
  "keeps the reply reference its sender named through storage, and none when none was named",
  () => {
    fc.assert(
      fc.property(
        fc.option(fc.integer({ min: 0, max: 255 }), { nil: undefined }),
        (byte) => {
          const replyToMsgId =
            byte === undefined
              ? undefined
              : Schema.decodeUnknownSync(MsgId)(digest("msg_", byte));
          const message = Effect.runSync(
            messageWithId(alice, [bob], { msgId, replyToMsgId }, content),
          );
          const stored = Schema.encodeSync(Schema.parseJson(Message))(message);
          const read = Schema.decodeUnknownSync(Schema.parseJson(Message))(
            stored,
          );
          expect(read.replyToMsgId).toBe(replyToMsgId);
          expect(Object.hasOwn(read, "replyToMsgId")).toBe(byte !== undefined);
        },
      ),
    );
  },
);

vitestIt(
  "accepts content exactly when its JSON text is at most 32,768 bytes",
  () => {
    fc.assert(
      fc.property(fc.integer({ min: 32_700, max: 32_800 }), (length) => {
        const content = [{ type: "text", text: "a".repeat(length) }];

        expect(Schema.is(Content)(content)).toBe(
          JSON.stringify(content).length <= 32_768,
        );
      }),
    );
  },
);

it("gives each created message its own message id", () =>
  Effect.gen(function* () {
    const create = Message.create({
      author: bob,
      recipients: [alice],
      content,
    });
    const first = yield* create;
    const second = yield* create;

    expect(first.msgId).not.toBe(second.msgId);
  }));

const decodingFails = (
  schema: Schema.Schema.AnyNoContext,
  value: unknown,
): boolean => !Schema.is(schema)(value);

vitestIt(
  "accepts a message identifier exactly when it encodes 32 bytes",
  () => {
    fc.assert(
      fc.property(fc.uint8Array({ maxLength: 48 }), (bytes) => {
        expect(
          decodingFails(MsgId, `msg_${Encoding.encodeBase64Url(bytes)}`),
        ).toBe(bytes.length !== 32);
      }),
    );
  },
);
