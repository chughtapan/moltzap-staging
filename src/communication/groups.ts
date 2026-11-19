/** @file Group addresses: an address that lists the members of a conversation of three or more agents. Everything that knows the `group:` grammar is here. */

import { Effect, Schema } from "effect";
import { compareAscii, isStrictlyAscending } from "../encoding.js";
import { AgentName } from "../identity.js";
import { SendError } from "./errors.js";

/** The scheme of an address listing a group's members. */
export const GROUP_ADDRESS_PREFIX = "group:";

/** The largest membership of one conversation. */
export const maximumMembers = 32;

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

/**
 * The members a `group:` destination names, with the local agent among them.
 * A name given twice, no agent but the local one, and more than
 * `maximumMembers` members are refused.
 */
export function groupMembers(
  to: string,
  localAgentName: AgentName,
): Effect.Effect<readonly AgentName[], SendError> {
  const names = groupMemberNames(to);
  if (names === undefined) {
    return Effect.fail(new SendError({ reason: "invalid-address" }));
  }
  const refused = (detail: string) =>
    Effect.fail(new SendError({ reason: "membership-invalid", detail }));
  if (new Set(names).size !== names.length) {
    return refused("the address names an agent more than once");
  }
  const members = names.includes(localAgentName)
    ? names
    : [...names, localAgentName];
  if (members.length === 1) {
    return refused("the address names only you");
  }
  return members.length > maximumMembers
    ? refused(`a group has at most ${String(maximumMembers)} members`)
    : Effect.succeed(members);
}
