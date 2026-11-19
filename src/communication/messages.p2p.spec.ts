/** @file A message to several recipients names each once, and a group address lists its members in name order. */

import { effect as it } from "@effect/vitest";
import {
  Array as Arr,
  Effect,
  Exit,
  FastCheck as fc,
  Order,
  Schema,
} from "effect";
import { expect, it as vitestIt } from "vitest";
import { digest, testAgentCard } from "../__tests__/agents.js";
import { GroupAddress, Message, MessageAddressInput } from "./messages.js";

const bob = testAgentCard("bob");

it.each([
  { reason: "a repeated recipient", recipients: [bob, bob] },
  {
    reason: "one identity under two recipient names",
    recipients: [bob, { ...testAgentCard("carol"), agentId: bob.agentId }],
  },
])("rejects $reason", ({ recipients }) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknown(Message)({
      msgId: digest("msg_", 1),
      author: testAgentCard("alice"),
      recipients,
      content: [{ type: "text", text: "Choose a meeting time." }],
    }).pipe(Effect.exit);

    expect(Exit.isFailure(decoded)).toBe(true);
  }),
);

const decodingFails = (
  schema: Schema.Schema.AnyNoContext,
  value: unknown,
): boolean => !Schema.is(schema)(value);

vitestIt(
  "accepts a group address of 3 to 32 members only in name order",
  () => {
    fc.assert(
      fc.property(
        fc.uniqueArray(fc.stringMatching(/^[a-z0-9]{3,8}$/), {
          minLength: 2,
          maxLength: 34,
        }),
        (names) => {
          const ordered = Arr.sort(names, Order.string);
          const sized = names.length >= 3 && names.length <= 32;
          const reversed = `group:${ordered.toReversed().join(",")}`;

          expect(
            decodingFails(GroupAddress, `group:${ordered.join(",")}`),
          ).toBe(!sized);
          expect(decodingFails(GroupAddress, reversed)).toBe(true);
          expect(decodingFails(MessageAddressInput, reversed)).toBe(false);
        },
      ),
    );
  },
);
