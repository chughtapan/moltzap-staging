/** @file A peer's address names one agent or a group, and a group address lists its members in name order. */

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
import { maximumMembers } from "../communication/index.js";
import { GroupAddress } from "./groups.js";
import { canonicalMessageAddress, MessageAddressInput } from "./tools.js";

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

const decodingFails = (
  schema: Schema.Schema.AnyNoContext,
  value: unknown,
): boolean => !Schema.is(schema)(value);

it("accepts a group address of 3 to 32 members only in name order", () => {
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

        expect(decodingFails(GroupAddress, `group:${ordered.join(",")}`)).toBe(
          !sized,
        );
        expect(decodingFails(GroupAddress, reversed)).toBe(true);
        expect(decodingFails(MessageAddressInput, reversed)).toBe(false);
      },
    ),
  );
});
