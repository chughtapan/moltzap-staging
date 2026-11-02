/** @file Message values every layer above the wire encodes: addresses, message ids, content, and member limits and order. */

import canonicalize from "canonicalize";
import { Either, Schema } from "effect";
import { exactStruct } from "../../encoding/canonical-json.js";
import {
  compareAgentIds as compareAgentIdBytes,
  compareAscii,
  isCanonicalIdentifier,
  isStrictlyAscending,
} from "../../encoding/identifier.js";
import { AGENT_NAME_RULE, AgentId, AgentName } from "../../identity/agent.js";
import { WIRE_VERSION } from "../../identity/wire-version.js";

/** Order validated agent identities by their decoded identifier bytes. */
export const compareAgentIds: (left: AgentId, right: AgentId) => number =
  compareAgentIdBytes;

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their domain names with the nominal values they decode. */

/** Client hashes, content bounds and extension keys share one retained profile. */
export const wireProfiles = {
  [WIRE_VERSION]: {
    clientHashDomain: "org.social-harness/client/v2/",
    collectiveDataKey: "org.social-harness/collective",
    maximumContentBytes: 32_768,
    maximumMembers: 32,
  },
} as const;

/** The canonical JSON byte limit of one message's content. */
export const maximumContentBytes =
  wireProfiles[WIRE_VERSION].maximumContentBytes;
const utf8Encoder = new TextEncoder();

/**
 * One author-scoped message identity. Retrying a persisted message reuses it;
 * a new message receives a fresh identity.
 */
export const MsgId = Schema.String.pipe(
  Schema.filter((value) => isCanonicalIdentifier("msg_", value), {
    identifier: "MsgId",
    description: "Canonical author-scoped message identity",
  }),
  Schema.brand("MsgId"),
  Schema.annotations({ identifier: "MsgId" }),
);
/**
 * A validated author-scoped message identity.
 * @useDeclaredType
 */
export type MsgId = typeof MsgId.Type;

/** The author and its message id identify the same message through retries and recovery. */
export const MessageReference = exactStruct({
  authorAgentId: AgentId,
  msgId: MsgId,
});
/**
 * An author's message id identifies the same message through delivery and restart.
 * @useDeclaredType
 */
export type MessageReference = typeof MessageReference.Type;

/** A string without lone surrogates, so it survives canonical JSON. */
export const wellFormedString = Schema.String.pipe(
  Schema.filter(hasWellFormedUnicode, {
    identifier: "WellFormedUnicodeString",
  }),
);

/* eslint-disable agent-code-guard/no-nullish-type-aliases -- JSON includes null as a first-class value. */
/** A value accepted by the closed semantic content boundary. */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };
/* eslint-enable agent-code-guard/no-nullish-type-aliases -- Restore the absence rule outside JSON values. */

/** Runtime validation for the closed recursive JSON value. */
export const JsonValue: Schema.Schema<JsonValue> = Schema.suspend(() =>
  Schema.Union(
    Schema.Null,
    Schema.Boolean,
    Schema.JsonNumber,
    wellFormedString,
    Schema.Array(JsonValue),
    Schema.Record({ key: wellFormedString, value: JsonValue }),
  ),
).annotations({ identifier: "JsonValue" });

/** One exact semantic part of a message. */
export const ContentPart = Schema.Union(
  exactStruct({ type: Schema.Literal("text"), text: wellFormedString }),
  exactStruct({ type: Schema.Literal("data"), value: JsonValue }),
).annotations({ identifier: "ContentPart" });
/**
 * A validated semantic message part.
 * @useDeclaredType
 */
export type ContentPart = typeof ContentPart.Type;

const contentStructure = Schema.NonEmptyArray(ContentPart);

/** Nonempty semantic content whose canonical JSON is at most 32,768 bytes. */
export const Content = contentStructure.pipe(
  Schema.filter(contentFits),
  Schema.annotations({ identifier: "Content" }),
);
/** Validated nonempty semantic content. */
export type Content = typeof Content.Type;

function hasWellFormedUnicode(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (isTrailingSurrogate(codeUnit)) {
      return false;
    }
    if (!isLeadingSurrogate(codeUnit)) {
      continue;
    }
    index += 1;
    if (!isTrailingSurrogate(value.charCodeAt(index))) {
      return false;
    }
  }
  return true;
}

function isLeadingSurrogate(codeUnit: number): boolean {
  return codeUnit >= 0xd800 && codeUnit <= 0xdbff;
}

function isTrailingSurrogate(codeUnit: number): boolean {
  return codeUnit >= 0xdc00 && codeUnit <= 0xdfff;
}

function contentFits(
  content: readonly [ContentPart, ...ContentPart[]],
): boolean {
  return Either.match(
    Either.try(() => canonicalize(content)),
    {
      onLeft: () => false,
      onRight: (canonical) =>
        canonical !== undefined &&
        utf8Encoder.encode(canonical).byteLength <= maximumContentBytes,
    },
  );
}

/** The largest group membership a conversation address or descriptor admits. */
export const maximumMembers = wireProfiles[WIRE_VERSION].maximumMembers;

/** The scheme of an address naming one agent. */
export const AGENT_ADDRESS_PREFIX = "agent:";
/** The scheme of an address listing a group's members. */
export const GROUP_ADDRESS_PREFIX = "group:";

const isAgentName = Schema.is(AgentName);

/**
 * The registry name an `agent:` address names.
 * @param value A candidate address.
 * @returns The name, or undefined when the value is not an agent address.
 */
const parseAgentAddress = (value: string): string | undefined => {
  if (!value.startsWith(AGENT_ADDRESS_PREFIX)) {
    return undefined;
  }
  const name = value.slice(AGENT_ADDRESS_PREFIX.length);
  return isAgentName(name) ? name : undefined;
};

/**
 * The names a `group:` address lists, in the order given.
 * @param value A candidate address.
 * @returns The names, or undefined when the value is not a group address.
 */
const parseGroupAddress = (value: string): readonly string[] | undefined => {
  if (!value.startsWith(GROUP_ADDRESS_PREFIX)) {
    return undefined;
  }
  const names = value.slice(GROUP_ADDRESS_PREFIX.length).split(",");
  return names.length > 0 && names.every((name) => isAgentName(name))
    ? names
    : undefined;
};

const addressInput = Schema.String.pipe(
  Schema.filter(
    (value) =>
      parseAgentAddress(value) !== undefined ||
      parseGroupAddress(value) !== undefined,
    {
      identifier: "MessageAddressInput",
      description: `agent:<name>, or group: then agent names separated by commas, with no spaces and no agent: prefix; a name is ${AGENT_NAME_RULE}`,
    },
  ),
  Schema.brand("MessageAddressInput"),
);

/** An explicit direct destination using one canonical Registry name. */
export const AgentAddress = addressInput.pipe(
  Schema.filter((value) => parseAgentAddress(value) !== undefined),
  Schema.brand("AgentAddress"),
  Schema.annotations({ identifier: "AgentAddress" }),
);
/**
 * A validated direct destination.
 * @useDeclaredType
 */
export type AgentAddress = typeof AgentAddress.Type;

/** The direct destination of an agent with a validated immutable Registry name. */
export const agentAddressOf = (agent: {
  readonly agentName: AgentName;
}): AgentAddress =>
  AgentAddress.make(`${AGENT_ADDRESS_PREFIX}${agent.agentName}`);

/** A complete fixed-member group address in unsigned ASCII name order. */
export const GroupAddress = addressInput.pipe(
  Schema.filter(isCanonicalGroupAddress),
  Schema.brand("GroupAddress"),
  Schema.annotations({ identifier: "GroupAddress" }),
);
/**
 * A validated canonical complete group destination.
 * @useDeclaredType
 */
export type GroupAddress = typeof GroupAddress.Type;

/** Either accepted destination input, including noncanonical group order. */
export const MessageAddressInput = addressInput;
/**
 * A validated explicit destination input.
 * @useDeclaredType
 */
export type MessageAddressInput = typeof MessageAddressInput.Type;

function isCanonicalGroupAddress(value: string): boolean {
  const names = parseGroupAddress(value);
  if (
    names === undefined ||
    names.length < 3 ||
    names.length > maximumMembers
  ) {
    return false;
  }
  return isStrictlyAscending(names, compareAscii);
}

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore the package naming rules after the Schema/type pairs. */
