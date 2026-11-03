/** @file Identity values remain nominal and cannot cross semantic positions. */

import type { AgentIdentityCardIssuedAt } from "./__tests__/authentication-fixtures.js";
import type {
  AgentId,
  AgentIdentityCardDigest,
  AgentName,
  PrincipalId,
} from "./agent-cards.js";
import type { EnvelopeId } from "./message-security.js";
import type { RegistrationId } from "./registry.js";

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

type IdentityValue =
  | AgentId
  | PrincipalId
  | AgentName
  | RegistrationId
  | EnvelopeId
  | AgentIdentityCardDigest
  | AgentIdentityCardIssuedAt;

type RawStringsCannotConstructValues = ExpectFalse<
  string extends IdentityValue ? true : false
>;

type AgentAndPrincipalDiffer = ExpectFalse<Crosses<AgentId, PrincipalId>>;
type AgentAndOperationDiffer = ExpectFalse<Crosses<AgentId, RegistrationId>>;
type AgentAndEnvelopeDiffer = ExpectFalse<Crosses<AgentId, EnvelopeId>>;
type AgentAndCardDigestDiffer = ExpectFalse<
  Crosses<AgentId, AgentIdentityCardDigest>
>;
type AgentAndNameDiffer = ExpectFalse<Crosses<AgentId, AgentName>>;
type PrincipalAndNameDiffer = ExpectFalse<Crosses<PrincipalId, AgentName>>;
type PrincipalAndOperationDiffer = ExpectFalse<
  Crosses<PrincipalId, RegistrationId>
>;
type PrincipalAndEnvelopeDiffer = ExpectFalse<Crosses<PrincipalId, EnvelopeId>>;
type PrincipalAndCardDigestDiffer = ExpectFalse<
  Crosses<PrincipalId, AgentIdentityCardDigest>
>;
type NameAndOperationDiffer = ExpectFalse<Crosses<AgentName, RegistrationId>>;
type NameAndEnvelopeDiffer = ExpectFalse<Crosses<AgentName, EnvelopeId>>;
type NameAndCardDigestDiffer = ExpectFalse<
  Crosses<AgentName, AgentIdentityCardDigest>
>;
type OperationAndEnvelopeDiffer = ExpectFalse<
  Crosses<RegistrationId, EnvelopeId>
>;
type OperationAndCardDigestDiffer = ExpectFalse<
  Crosses<RegistrationId, AgentIdentityCardDigest>
>;
type EnvelopeAndCardDigestDiffer = ExpectFalse<
  Crosses<EnvelopeId, AgentIdentityCardDigest>
>;
type IssuedAtAndAgentDiffer = ExpectFalse<
  Crosses<AgentIdentityCardIssuedAt, AgentId>
>;
type IssuedAtAndPrincipalDiffer = ExpectFalse<
  Crosses<AgentIdentityCardIssuedAt, PrincipalId>
>;
type IssuedAtAndNameDiffer = ExpectFalse<
  Crosses<AgentIdentityCardIssuedAt, AgentName>
>;
type IssuedAtAndOperationDiffer = ExpectFalse<
  Crosses<AgentIdentityCardIssuedAt, RegistrationId>
>;
type IssuedAtAndEnvelopeDiffer = ExpectFalse<
  Crosses<AgentIdentityCardIssuedAt, EnvelopeId>
>;
type IssuedAtAndCardDigestDiffer = ExpectFalse<
  Crosses<AgentIdentityCardIssuedAt, AgentIdentityCardDigest>
>;
type RefinedValuesRemainStrings = Expect<
  IdentityValue extends string ? true : false
>;

/** Compile-time witnesses for the package's public identity-value invariants. */
export type IdentityValueCanaries = [
  RawStringsCannotConstructValues,
  AgentAndPrincipalDiffer,
  AgentAndOperationDiffer,
  AgentAndEnvelopeDiffer,
  AgentAndCardDigestDiffer,
  AgentAndNameDiffer,
  PrincipalAndNameDiffer,
  PrincipalAndOperationDiffer,
  PrincipalAndEnvelopeDiffer,
  PrincipalAndCardDigestDiffer,
  NameAndOperationDiffer,
  NameAndEnvelopeDiffer,
  NameAndCardDigestDiffer,
  OperationAndEnvelopeDiffer,
  OperationAndCardDigestDiffer,
  EnvelopeAndCardDigestDiffer,
  IssuedAtAndAgentDiffer,
  IssuedAtAndPrincipalDiffer,
  IssuedAtAndNameDiffer,
  IssuedAtAndOperationDiffer,
  IssuedAtAndEnvelopeDiffer,
  IssuedAtAndCardDigestDiffer,
  RefinedValuesRemainStrings,
];
