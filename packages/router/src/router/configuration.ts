/** @file Router process configuration, validation, defaults, and operator feedback. */

import { AgentPublicKey, SignedMessage } from "@moltzap/social-harness";
import { routerContract } from "@moltzap/social-harness/internal";
import { Config, type ConfigError, Effect, Match, Schema } from "effect";

/** ConfigError trees retain every missing field without printing rejected inputs. */
export function configurationMessage(error: ConfigError.ConfigError): string {
  return Match.value(error).pipe(
    Match.discriminatorsExhaustive("_op")({
      And: (cause) =>
        `${configurationMessage(cause.left)}; ${configurationMessage(cause.right)}`,
      Or: (cause) =>
        `${configurationMessage(cause.left)} or ${configurationMessage(cause.right)}`,
      MissingData: (cause) => `${Array.from(cause.path).join("_")} is required`,
      InvalidData: invalidConfigurationMessage,
      SourceUnavailable: (cause) =>
        `${Array.from(cause.path).join("_")} configuration source unavailable`,
      Unsupported: (cause) =>
        `${Array.from(cause.path).join("_")} configuration source unavailable`,
    }),
  );
}

const MAXIMUM_PROCESS_INTEGER = 2_147_483_647;
const canonicalUnsignedDecimal = Schema.String.pipe(
  Schema.pattern(/^(?:0|[1-9]\d*)$/),
);
const processInteger = canonicalUnsignedDecimal.pipe(
  Schema.compose(Schema.NumberFromString),
  Schema.int(),
  Schema.between(1, MAXIMUM_PROCESS_INTEGER),
);
const port = canonicalUnsignedDecimal.pipe(
  Schema.compose(Schema.NumberFromString),
  Schema.int(),
  Schema.between(1, 65_535),
);
const bindHost = Schema.String.pipe(Schema.minLength(1));

const isSerializedRegistryOrigin = (value: string): boolean => {
  if (!URL.canParse(value)) {
    return false;
  }
  const parsed = new URL(value);
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return false;
  }
  return value === parsed.origin;
};

const registryOriginText = Schema.String.pipe(
  Schema.filter(isSerializedRegistryOrigin),
  Schema.annotations({
    identifier: "serialized HTTP(S) origin without a path",
  }),
);
const registryOrigin = registryOriginText.pipe(Schema.compose(Schema.URL));
const compactPublicKeyJson = Schema.String.pipe(
  Schema.pattern(/^\{"crv":"Ed25519","kty":"OKP","x":"[A-Za-z0-9_-]{43}"\}$/),
  Schema.compose(Schema.parseJson(AgentPublicKey)),
);

const configurationFields = {
  host: setting("SHARNESS_ROUTER_HOST", bindHost, "127.0.0.1"),
  port: setting("SHARNESS_ROUTER_PORT", port),
  registryOrigin: setting("SHARNESS_ROUTER_REGISTRY_ORIGIN", registryOrigin),
  registrySignerPublicKey: setting(
    "SHARNESS_ROUTER_REGISTRY_SIGNER_PUBLIC_KEY",
    compactPublicKeyJson,
  ),
  retainedMessageCapacity: setting(
    "SHARNESS_ROUTER_RETAINED_MESSAGE_CAPACITY",
    processInteger,
    4_096,
  ),
  retainedMessageByteCapacity: setting(
    "SHARNESS_ROUTER_RETAINED_MESSAGE_BYTE_CAPACITY",
    processInteger,
    67_108_864,
  ),
  pollMessageLimit: setting(
    "SHARNESS_ROUTER_POLL_MESSAGE_LIMIT",
    processInteger,
    128,
  ),
  pollResponseByteLimit: setting(
    "SHARNESS_ROUTER_POLL_RESPONSE_BYTE_LIMIT",
    processInteger,
    1_048_576,
  ),
  requestConcurrencyLimit: setting(
    "SHARNESS_ROUTER_REQUEST_CONCURRENCY_LIMIT",
    processInteger,
    512,
  ),
  heldPollCapacity: setting(
    "SHARNESS_ROUTER_HELD_POLL_CAPACITY",
    processInteger,
    256,
  ),
  liveNonceCapacity: setting(
    "SHARNESS_ROUTER_LIVE_NONCE_CAPACITY",
    processInteger,
    100_000,
  ),
  identityCardCacheCapacity: setting(
    "SHARNESS_ROUTER_AGENT_CARD_CACHE_CAPACITY",
    processInteger,
    10_000,
  ),
  registryLookupConcurrencyLimit: setting(
    "SHARNESS_ROUTER_REGISTRY_LOOKUP_CONCURRENCY_LIMIT",
    processInteger,
    32,
  ),
  registryLookupTimeoutMs: setting(
    "SHARNESS_ROUTER_REGISTRY_LOOKUP_TIMEOUT_MS",
    processInteger,
    5_000,
  ),
};

const configuredValues = Config.all(configurationFields);

/** Usage is generated from the same fields consumed by the process. */
export const processHelp = [
  "Usage: sharness-router [--help]",
  "",
  "Environment:",
  ...Object.values(configurationFields).map((field) => `  ${field.usage}`),
].join("\n");

/** Complete validated private process configuration. */
export type RouterConfiguration = Readonly<
  Config.Config.Success<typeof configuredValues>
>;

const retentionFits = (candidate: RouterConfiguration): boolean =>
  candidate.retainedMessageCapacity >= 1 &&
  candidate.retainedMessageByteCapacity >=
    SignedMessage.maximumEncodedByteLength;

const pollBatchFits = (candidate: RouterConfiguration): boolean =>
  candidate.pollMessageLimit >= 1 &&
  candidate.pollResponseByteLimit >=
    routerContract.routerRepresentationLimits.oneMessageBatchBytes;

const heldPollsFit = (candidate: RouterConfiguration): boolean =>
  candidate.heldPollCapacity < candidate.requestConcurrencyLimit;

const resourceLimitMessages = {
  retention: `SHARNESS_ROUTER_RETAINED_MESSAGE_BYTE_CAPACITY must be at least ${SignedMessage.maximumEncodedByteLength} to retain one maximum-size message`,
  pollBatch: `SHARNESS_ROUTER_POLL_RESPONSE_BYTE_LIMIT must be at least ${routerContract.routerRepresentationLimits.oneMessageBatchBytes} to return one maximum-size message`,
  heldPolls:
    "SHARNESS_ROUTER_HELD_POLL_CAPACITY must be less than SHARNESS_ROUTER_REQUEST_CONCURRENCY_LIMIT",
};

/** Loads and cross-validates the complete private Router process configuration. */
export const loadRouterConfiguration = configuredValues.pipe(
  Config.validate({
    message: resourceLimitMessages.retention,
    validation: retentionFits,
  }),
  Config.validate({
    message: resourceLimitMessages.pollBatch,
    validation: pollBatchFits,
  }),
  Config.validate({
    message: resourceLimitMessages.heldPolls,
    validation: heldPollsFit,
  }),
  Effect.withSpan("loadRouterConfiguration"),
);

/** Config and operator text share the name, schema and default at declaration. */
function setting<A, D = never>(
  name: string,
  schema: Schema.Schema<A, string>,
  ...defaults: [] | [D]
) {
  const config = Schema.Config(name, schema);
  return Object.assign(
    defaults.length === 0
      ? config
      : config.pipe(Config.withDefault(defaults[0])),
    {
      name,
      reason: `must satisfy ${schema.ast.toString()}`,
      usage: `${name}: ${settingUsage(defaults)}`,
    },
  );
}

/** Distinguishes absent defaults from explicitly unset or empty defaults in operator help. */
function settingUsage(defaults: readonly unknown[]): string {
  if (defaults.length === 0) {
    return "required; default: none";
  }
  if (defaults[0] === undefined) {
    return "optional; default: unset";
  }
  if (defaults[0] === "") {
    return "optional; default: none";
  }
  return `optional; default: ${JSON.stringify(defaults[0])}`;
}

/** Only declared setting constraints may appear in invalid-value diagnostics. */
function invalidConfigurationMessage(error: ConfigError.InvalidData): string {
  if (Object.values(resourceLimitMessages).includes(error.message)) {
    return error.message;
  }
  const name = Array.from(error.path).join("_");
  const field = Object.values(configurationFields).find(
    (candidate) => candidate.name === name,
  );
  return field === undefined
    ? "environment configuration is invalid"
    : `${name} ${field.reason}`;
}
