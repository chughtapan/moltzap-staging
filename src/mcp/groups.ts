/** @file Group addresses: an address that lists the members of a conversation of three or more agents. Everything that knows the `group:` grammar is here. */

import { Schema } from "effect";
import { maximumMembers } from "../communication/index.js";
import { compareAscii, isStrictlyAscending } from "../encoding.js";
import { AgentName } from "../identity.js";

/** The scheme of an address listing a group's members. */
const GROUP_ADDRESS_PREFIX = "group:";

const isAgentName = Schema.is(AgentName);

/** The names a `group:` address lists, in the order given, or undefined when the value is not a group address. */
export const groupMemberNames = (value: string): AgentName[] | undefined => {
  if (!value.startsWith(GROUP_ADDRESS_PREFIX)) {
    return undefined;
  }
  const names = value.slice(GROUP_ADDRESS_PREFIX.length).split(",");
  return names.every(isAgentName) ? names : undefined;
};

/** A complete fixed-member group address in unsigned ASCII name order. */
export const GroupAddress = Schema.String.pipe(
  Schema.brand("MessageAddressInput"),
  Schema.filter((value) => {
    const names = groupMemberNames(value);
    return (
      names !== undefined &&
      names.length >= 3 &&
      names.length <= maximumMembers &&
      isStrictlyAscending(names, compareAscii)
    );
  }),
  Schema.brand("GroupAddress"),
  Schema.annotations({ identifier: "GroupAddress" }),
);
/**
 * A validated canonical group address.
 * @useDeclaredType
 */
export type GroupAddress = typeof GroupAddress.Type;

/** The canonical address of the group with these members. */
export const groupAddressOf = (names: readonly AgentName[]): GroupAddress =>
  GroupAddress.make(
    `${GROUP_ADDRESS_PREFIX}${[...names].sort(compareAscii).join(",")}`,
  );
