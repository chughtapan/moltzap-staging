/** @file Host sends name a canonical conversation and return its accepted work identity. */

import { Effect, type ParseResult, Schema } from "effect";
import { SendError } from "../communication/conversations/errors.js";
import {
  AcceptResponse,
  CollectiveId,
  DeadlineSeconds,
  DeclineResponse,
  RequestedSchema,
} from "../communication/index.js";
import {
  AGENT_ADDRESS_PREFIX,
  AgentAddress,
  GROUP_ADDRESS_PREFIX,
  GroupAddress,
  maximumMembers,
  MessageAddressInput,
  MsgId,
  wellFormedString,
} from "../communication/records/values.js";
import { exactStruct } from "../encoding/canonical-json.js";
import { compareAscii } from "../encoding/identifier.js";
import { checkedGuidanceSchema } from "../firewall/check.js";
import { MessageAddress } from "../harness/inbox.js";
import { AgentName } from "../identity/agent.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect schemas share the public names of their derived value types. */

const multicast = exactStruct({
  op: Schema.optionalWith(Schema.Literal("multicast"), { exact: true }),
});
const question = exactStruct({
  op: Schema.Literal("gather", "all_gather"),
  deadline: DeadlineSeconds,
  requestedSchema: RequestedSchema,
});
const response = Schema.Union(AcceptResponse, DeclineResponse);

/** An ordinary message may explicitly select multicast. */
export const MessageSendRequest = exactStruct({
  to: MessageAddressInput,
  text: wellFormedString,
  collective: Schema.optionalWith(multicast, { exact: true }),
});
/** A question supplies its response form and relative deadline together. */
export const QuestionSendRequest = exactStruct({
  to: MessageAddressInput,
  text: wellFormedString,
  collective: question,
});
/** An answer names the request it addresses. */
export const AnswerSendRequest = exactStruct({
  to: MessageAddressInput,
  collectiveResponse: response,
});
/** Host addresses are resolved by SocialHarness before communication preparation. */
export const SendRequest = Schema.Union(
  MessageSendRequest,
  QuestionSendRequest,
  AnswerSendRequest,
).annotations({ identifier: "SendRequest" });
/** Question deadlines include policy assessment time.
 * @useDeclaredType
 */
export type SendRequest = typeof SendRequest.Type;
/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore ordinary symbol naming. */

/* eslint-disable @typescript-eslint/naming-convention -- Effect schemas share the public names of their derived value types. */

/** Accepted ordinary messages, replies and questions share one receipt shape. */
export const SendResultSchema = Schema.Struct({
  id: Schema.Union(MsgId, CollectiveId),
  address: MessageAddress,
  guidance: Schema.optionalWith(checkedGuidanceSchema, { exact: true }),
}).annotations({ identifier: "SendResult" });

/** Accepted work continues independently of the caller's lifetime. */
export type SendResult = typeof SendResultSchema.Type;

/* eslint-enable @typescript-eslint/naming-convention -- Restore ordinary symbol naming. */

/** A destination in canonical form, before any Registry lookup. */
type CanonicalMessageAddress =
  | {
      readonly kind: "direct";
      readonly address: AgentAddress;
      readonly remoteName: AgentName;
    }
  | {
      readonly kind: "group";
      readonly address: GroupAddress;
      readonly memberNames: readonly AgentName[];
    };

/**
 * Put one destination in canonical form, the rule every send shares. An
 * `agent:` address names one other agent. A `group:` address names each agent
 * once and the local agent is added when absent. Naming one other agent makes
 * it that agent's direct address, since the two-member set is the direct
 * conversation; otherwise the complete group has 3 to `maximumMembers` members
 * in unsigned ASCII name order.
 * @param to The validated destination input.
 * @param localAgentName The local agent's Registry name.
 * @returns The canonical address with its names, local agent included for a
 *   group, or `invalid-address` or `membership-invalid`.
 */
export function canonicalMessageAddress(
  to: MessageAddressInput,
  localAgentName: string,
): Effect.Effect<CanonicalMessageAddress, SendError> {
  return to.startsWith(AGENT_ADDRESS_PREFIX)
    ? canonicalDirect(to, localAgentName)
    : canonicalGroup(to, localAgentName);
}

/**
 * Render a fixed membership's group address: every member's Registry name,
 * the local agent's included, in unsigned ASCII order. This is the one place
 * that orders a group's names, so the names it returns list the members in
 * the address's order.
 * @param names The members' names, in any order.
 * @returns The canonical group address and the names in its order, or the
 *   parse error when the names cannot form one.
 */
function renderGroupAddress<Name extends string>(
  names: readonly Name[],
): Effect.Effect<
  { readonly address: GroupAddress; readonly names: readonly Name[] },
  ParseResult.ParseError
> {
  const ordered = [...names].sort(compareAscii);
  return Schema.decodeUnknown(GroupAddress)(
    `${GROUP_ADDRESS_PREFIX}${ordered.join(",")}`,
  ).pipe(Effect.map((address) => ({ address, names: ordered })));
}

function invalidAddress(): SendError {
  return new SendError({ reason: "invalid-address" });
}

function invalidMembership(detail: string): SendError {
  return new SendError({ reason: "membership-invalid", detail });
}

function decodeAgentName(value: string): Effect.Effect<AgentName, SendError> {
  return Schema.decodeUnknown(AgentName)(value).pipe(
    Effect.mapError(invalidAddress),
  );
}

function canonicalDirect(
  to: MessageAddressInput,
  localAgentName: string,
): Effect.Effect<CanonicalMessageAddress, SendError> {
  return Effect.gen(function* () {
    const remoteName = yield* decodeAgentName(
      to.slice(AGENT_ADDRESS_PREFIX.length),
    );
    if (remoteName === localAgentName) {
      return yield* invalidMembership("the address names only you");
    }
    const address = yield* Schema.decodeUnknown(AgentAddress)(to).pipe(
      Effect.mapError(invalidAddress),
    );
    return { kind: "direct", address, remoteName };
  });
}

function canonicalGroup(
  to: MessageAddressInput,
  localAgentName: string,
): Effect.Effect<CanonicalMessageAddress, SendError> {
  return Effect.gen(function* () {
    const explicitNames = yield* Effect.forEach(
      to.slice(GROUP_ADDRESS_PREFIX.length).split(","),
      decodeAgentName,
      { concurrency: 1 },
    );
    if (new Set(explicitNames).size !== explicitNames.length) {
      return yield* invalidMembership(
        "the address names an agent more than once",
      );
    }
    const localName = yield* decodeAgentName(localAgentName);
    const memberNames = explicitNames.includes(localName)
      ? explicitNames.slice()
      : [...explicitNames, localName];
    const others = memberNames.filter((name) => name !== localName);
    const [onlyOther] = others;
    if (onlyOther === undefined) {
      return yield* invalidMembership("the address names only you");
    }
    if (others.length === 1) {
      const address = yield* Schema.decodeUnknown(AgentAddress)(
        `${AGENT_ADDRESS_PREFIX}${onlyOther}`,
      ).pipe(Effect.mapError(invalidAddress));
      return { kind: "direct", address, remoteName: onlyOther };
    }
    if (memberNames.length > maximumMembers) {
      return yield* invalidMembership(
        `a group has at most ${String(maximumMembers)} members`,
      );
    }
    const group = yield* renderGroupAddress(memberNames).pipe(
      Effect.mapError(invalidAddress),
    );
    return { kind: "group", address: group.address, memberNames: group.names };
  });
}
