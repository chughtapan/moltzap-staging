/**
 * @file Agent identifiers and names are nominal: a plain string is none of
 * them and one cannot stand in for another, so a name is never read as an
 * identifier or an agent as its principal.
 */

import type { AgentId, AgentName, PrincipalId } from "./agent-cards.js";

/**
 * True when a value of either type is assignable to the other, that is, when
 * one identifier can cross into the other's position.
 */
type Crosses<Left, Right> = [Left] extends [Right]
  ? true
  : [Right] extends [Left]
    ? true
    : false;
type Expect<Value extends true> = Value;
type ExpectFalse<Value extends false> = Value;

type IdentityValue = AgentId | PrincipalId | AgentName;

type RawStringsCannotConstructValues = ExpectFalse<
  string extends IdentityValue ? true : false
>;
type AgentAndPrincipalDiffer = ExpectFalse<Crosses<AgentId, PrincipalId>>;
type AgentAndNameDiffer = ExpectFalse<Crosses<AgentId, AgentName>>;
type PrincipalAndNameDiffer = ExpectFalse<Crosses<PrincipalId, AgentName>>;
type RefinedValuesRemainStrings = Expect<
  IdentityValue extends string ? true : false
>;

/** Compile-time witnesses for the package's public identity-value invariants. */
export type IdentityValueCanaries = [
  RawStringsCannotConstructValues,
  AgentAndPrincipalDiffer,
  AgentAndNameDiffer,
  PrincipalAndNameDiffer,
  RefinedValuesRemainStrings,
];
