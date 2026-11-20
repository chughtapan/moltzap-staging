/** @file Agent identifiers and names are canonical, and a message keeps what its sender gave it. */

import { effect as it } from "@effect/vitest";
import { Effect, Encoding, FastCheck as fc, Schema } from "effect";
import { describe, expect, it as vitestIt } from "vitest";
import { digest, testAgentCard } from "./__tests__/agents.js";
import { AgentId, AgentName, Message, MsgId, PrincipalId } from "./messages.js";

const alice = testAgentCard("alice");
const bob = testAgentCard("bob");
const msgId = Schema.decodeUnknownSync(MsgId)(digest("msg_", 1));

describe("identifiers", () => {
  vitestIt("accepts every canonical agent and principal identifier", () => {
    fc.assert(
      fc.property(fc.uint8Array({ minLength: 16, maxLength: 16 }), (bytes) => {
        const payload = Buffer.from(bytes).toString("base64url");
        expect(Schema.is(AgentId)(`agt_${payload}`)).toBe(true);
        expect(Schema.is(PrincipalId)(`prn_${payload}`)).toBe(true);
      }),
    );
  });

  vitestIt.each([
    { reason: "wrong prefix", value: `prn_${"A".repeat(22)}` },
    { reason: "short payload", value: `agt_${"A".repeat(21)}` },
    { reason: "long payload", value: `agt_${"A".repeat(23)}` },
    { reason: "padding", value: `agt_${"A".repeat(22)}=` },
    { reason: "standard alphabet plus", value: `agt_${"A".repeat(21)}+` },
    { reason: "standard alphabet slash", value: `agt_${"A".repeat(21)}/` },
    { reason: "whitespace", value: `agt_${"A".repeat(21)} ` },
    { reason: "nonzero trailing bits", value: `agt_${"A".repeat(21)}B` },
  ])("rejects a noncanonical AgentId with $reason", ({ value }) => {
    expect(Schema.is(AgentId)(value)).toBe(false);
  });

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

  vitestIt("mints identifiers its own schema accepts", () => {
    expect(Schema.is(AgentId)(AgentId.mint())).toBe(true);
    expect(Schema.is(PrincipalId)(PrincipalId.mint())).toBe(true);
    expect(Schema.is(MsgId)(MsgId.mint())).toBe(true);
  });
});

describe("AgentName", () => {
  vitestIt.each(["abc", "a".repeat(32), "agent-7"])(
    "accepts the AgentName %s",
    (value) => {
      expect(Schema.is(AgentName)(value)).toBe(true);
    },
  );

  vitestIt.each([
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
    expect(Schema.is(AgentName)(value)).toBe(false);
  });
});

describe("Message", () => {
  vitestIt("rejects the author as its recipient", () => {
    expect(
      Schema.is(Message)({ msgId, author: alice, recipient: alice, text: "" }),
    ).toBe(false);
  });

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
            const stored = Schema.encodeSync(Schema.parseJson(Message))({
              msgId,
              author: alice,
              recipient: bob,
              text: "Choose a meeting time.",
              ...(replyToMsgId === undefined ? {} : { replyToMsgId }),
            });
            const read = Schema.decodeUnknownSync(Schema.parseJson(Message))(
              stored,
            );
            expect(read.replyToMsgId).toBe(replyToMsgId);
            expect(Object.hasOwn(read, "replyToMsgId")).toBe(
              byte !== undefined,
            );
          },
        ),
      );
    },
  );

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
