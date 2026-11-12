/** @file A destination a host states becomes one canonical conversation address. */

import {
  Array as Arr,
  Effect,
  Either,
  FastCheck as fc,
  Order,
  Schema,
} from "effect";
import { expect, it } from "vitest";
import { testAgentCard } from "../__tests__/agents.js";
import {
  maximumMembers,
  MessageAddressInput,
} from "../communication/messages.js";
import { canonicalMessageAddress } from "./message-format.js";

const localAgentName = testAgentCard("agent:agent-1").agentName;

it("addresses one peer directly, 3 to 32 members as a group in name order, and refuses any other membership", () => {
  fc.assert(
    fc.property(
      fc.uniqueArray(fc.stringMatching(/^[a-z0-9]{3,8}$/), {
        maxLength: maximumMembers + 1,
      }),
      fc.boolean(),
      fc.boolean(),
      fc.boolean(),
      (others, namesItself, repeats, direct) => {
        const [first] = others;
        const repeated = repeats && first !== undefined ? [first] : [];
        const listed = [
          ...(namesItself || first === undefined ? [localAgentName] : []),
          ...others,
          ...repeated,
        ];
        const input =
          direct && listed.length === 1
            ? `agent:${listed.join("")}`
            : `group:${listed.join(",")}`;
        const members = Arr.sort([localAgentName, ...others], Order.string);
        const group =
          repeated.length === 0 && members.length <= maximumMembers
            ? `group:${members.join(",")}`
            : "membership-invalid";
        const expected =
          others.length === 1 && repeated.length === 0
            ? `agent:${others.join("")}`
            : group;

        expect(
          Either.match(
            Effect.runSync(
              Effect.either(
                canonicalMessageAddress(
                  Schema.decodeUnknownSync(MessageAddressInput)(input),
                  localAgentName,
                ),
              ),
            ),
            {
              onLeft: ({ reason }) => reason,
              onRight: ({ address }) => address,
            },
          ),
        ).toBe(others.length === 0 ? "membership-invalid" : expected);
      },
    ),
  );
});
