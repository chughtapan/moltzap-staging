/** @file Loads daemon settings, credentials and optional span export. */

import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { diag, DiagConsoleLogger, DiagLogLevel } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import {
  Config,
  type ConfigError,
  Data,
  Effect,
  Either,
  Layer,
  Match,
  Option,
  Redacted,
  Schema,
} from "effect";
import type { MessagePolicyConfigurationFailure } from "../firewall/rules.js";
import type { RegistrationRequest } from "../identity/service.js";
import { AgentName, PrincipalId } from "../identity/agent-cards.js";
import { AgentPublicKey, AgentSigningAuthority } from "../identity/keys.js";
import { RegistrationId } from "../identity/registry.js";
import { packageVersion } from "../version.js";

/** Identifies the setting that prevents startup without exposing its value. */
export type DaemonConfigurationFailure =
  | "environment"
  | "agent-private-key-file"
  | "agent-private-key"
  | "admission-credential-file"
  | "admission-credential"
  | "mcp-credential-file"
  | "mcp-credential"
  | MessagePolicyConfigurationFailure;

/** Daemon configuration failure with a safe operator message. */
export class DaemonConfigurationError extends Data.TaggedError(
  "DaemonConfigurationError",
)<{
  readonly reason: DaemonConfigurationFailure;
  readonly message: string;
}> {}

/** Exact non-secret values and redacted secret-file locations for one daemon. */
export interface DaemonProcessConfiguration {
  readonly registration?: RegistrationRequest;
  readonly stateDirectory: string;
  readonly mcpPort: number;
  readonly registryOrigin: URL;
  readonly registrySignerPublicKey: AgentPublicKey;
  readonly routerOrigin: URL;
  readonly agentPrivateKeyFile: Redacted.Redacted;
  /**
   * Required only before registration. Registered daemons never read it.
   * Empty and unset values both omit the file.
   */
  readonly admissionCredentialFile?: Redacted.Redacted;
  /** Optional JSON-lines file for delivered items and send outcomes. */
  readonly historyExport?: string;
  readonly mcpCredentialFile: Redacted.Redacted;
  /** Omitting the policy file disables the firewall. */
  readonly messagePolicyFile?: string;
  readonly decisionProviderKeyFile?: Redacted.Redacted;
  /** An evaluation hides `group:` addresses from every send. */
  readonly hideGroups: boolean;
  /** An evaluation hides gather, all_gather and answers from every send. */
  readonly hideCollectives: boolean;
}

/** Loaded private authority required by daemon registration and network calls. */
export interface DaemonBootstrap {
  readonly configuration: DaemonProcessConfiguration;
  readonly signingAuthority: AgentSigningAuthority;
  /**
   * Reads and validates the admission credential file on first use, then
   * replays that outcome. Loading the bootstrap never reads the file.
   */
  readonly admissionCredential: Effect.Effect<
    Redacted.Redacted,
    DaemonConfigurationError
  >;
  readonly mcpCredential: Redacted.Redacted;
}

/**
 * Distinguishes unreadable UTF-8 from rejected credential text. Paths, bytes
 * and decoder details stay private.
 */
export class CredentialError extends Data.TaggedError("CredentialError")<{
  readonly stage: "file" | "value";
}> {}

/** Accepts admission tokens exactly as stored, without surrounding whitespace. */
const credentialText = Schema.String.pipe(
  Schema.minLength(8),
  Schema.maxLength(512),
  Schema.pattern(/^[A-Za-z0-9\-._~+/]+=*$/u),
);

const utf8Decoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: true,
});

const fileFailure = (): CredentialError =>
  new CredentialError({ stage: "file" });

const valueFailure = (): CredentialError =>
  new CredentialError({ stage: "value" });

const readExactUtf8 = (
  path: Redacted.Redacted,
): Effect.Effect<string, CredentialError> =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((filesystem) => filesystem.readFile(Redacted.value(path))),
    Effect.provide(NodeFileSystem.layer),
    Effect.mapError(fileFailure),
    Effect.flatMap((bytes) =>
      Effect.try({
        try: () => utf8Decoder.decode(bytes),
        catch: fileFailure,
      }),
    ),
  );

/** Reads exact UTF-8, validates the whole text with `grammar`, and redacts it. */
export const readCredential = (
  path: Redacted.Redacted,
  grammar: Schema.Schema<string>,
): Effect.Effect<Redacted.Redacted, CredentialError> =>
  readExactUtf8(path).pipe(
    Effect.flatMap((text) =>
      Schema.decodeUnknown(grammar)(text).pipe(Effect.mapError(valueFailure)),
    ),
    Effect.map(Redacted.make),
  );

/** Reads a PKCS#8 key and returns opaque Ed25519 signing authority. */
const loadSigningAuthority = (
  path: Redacted.Redacted,
): Effect.Effect<AgentSigningAuthority, CredentialError> =>
  readExactUtf8(path).pipe(
    Effect.flatMap((privateKey) =>
      AgentSigningAuthority.fromPkcs8(Redacted.make(privateKey)).pipe(
        Effect.mapError(valueFailure),
      ),
    ),
  );

const loadAdmissionCredential = (
  path: Redacted.Redacted,
): Effect.Effect<Redacted.Redacted, CredentialError> =>
  readCredential(path, credentialText);

const canonicalUnsignedDecimal = Schema.String.pipe(
  Schema.pattern(/^(?:0|[1-9]\d*)$/u),
);

const port = canonicalUnsignedDecimal.pipe(
  Schema.compose(Schema.NumberFromString),
  Schema.int(),
  Schema.between(1, 65_535),
);

const configuredPath = Schema.String.pipe(
  Schema.minLength(1),
  Schema.filter((value) => !value.includes("\u0000")),
);

const optionalConfiguredPath = Schema.Union(Schema.Literal(""), configuredPath);

const isSerializedOrigin = (value: string): boolean => {
  if (!URL.canParse(value)) {
    return false;
  }
  const parsed = new URL(value);
  return (
    (parsed.protocol === "http:" || parsed.protocol === "https:") &&
    value === parsed.origin
  );
};

const origin = Schema.String.pipe(
  Schema.filter(isSerializedOrigin),
  Schema.annotations({
    identifier: "serialized HTTP(S) origin without a path",
  }),
  Schema.compose(Schema.URL),
);

const publicKeyJson = Schema.parseJson(AgentPublicKey);

const experimentSwitch = Schema.String.pipe(
  Schema.compose(
    Schema.transformLiterals(
      ["true", true],
      ["yes", true],
      ["on", true],
      ["1", true],
      ["false", false],
      ["no", false],
      ["off", false],
      ["0", false],
    ),
  ),
  Schema.annotations({ identifier: "true, yes, on, 1, false, no, off or 0" }),
);

const configurationFields = {
  registrationAgentName: setting(
    "SHARNESS_REGISTRATION_AGENT_NAME",
    AgentName,
    undefined,
  ),
  registrationOperationId: setting(
    "SHARNESS_REGISTRATION_OPERATION_ID",
    RegistrationId,
    undefined,
  ),
  registrationPrincipalId: setting(
    "SHARNESS_REGISTRATION_PRINCIPAL_ID",
    PrincipalId,
    undefined,
  ),
  stateDirectory: setting("SHARNESS_STATE_DIRECTORY", configuredPath),
  mcpPort: setting("SHARNESS_MCP_PORT", port),
  registryOrigin: setting("SHARNESS_REGISTRY_ORIGIN", origin),
  registrySignerPublicKey: setting(
    "SHARNESS_REGISTRY_SIGNER_PUBLIC_KEY",
    publicKeyJson,
  ),
  routerOrigin: setting("SHARNESS_ROUTER_ORIGIN", origin),
  agentPrivateKeyFile: setting(
    "SHARNESS_AGENT_PRIVATE_KEY_FILE",
    configuredPath,
  ),
  admissionCredentialFile: setting(
    "SHARNESS_ADMISSION_CREDENTIAL_FILE",
    optionalConfiguredPath,
    "",
  ),
  historyExport: setting("SHARNESS_HISTORY_EXPORT", configuredPath, undefined),
  mcpCredentialFile: setting("SHARNESS_MCP_CREDENTIAL_FILE", configuredPath),
  messagePolicyFile: setting(
    "SHARNESS_MESSAGE_POLICY_FILE",
    configuredPath,
    undefined,
  ),
  decisionProviderKeyFile: setting(
    "SHARNESS_DECISION_PROVIDER_KEY_FILE",
    configuredPath,
    undefined,
  ),
  hideGroups: setting(
    "SHARNESS_EXPERIMENT_HIDE_GROUPS",
    experimentSwitch,
    false,
  ),
  hideCollectives: setting(
    "SHARNESS_EXPERIMENT_HIDE_COLLECTIVES",
    experimentSwitch,
    false,
  ),
};

const configuredValues = Config.all(configurationFields);

/** Usage is generated from the same fields consumed by the process. */
export const processHelp = [
  "Usage: sharnessd [--help]",
  "",
  "Environment:",
  "  Admission credential: required only before registration; MCP credential file: required.",
  "  Message policy: unset, no firewall; the decision-provider key applies only with it.",
  ...Object.values(configurationFields).map((field) => `  ${field.usage}`),
  "",
  "Docs: https://docs.moltzap.xyz/reference/spec/daemon#process-and-configuration",
].join("\n");

type ProcessConfigurationFailure = Exclude<
  DaemonConfigurationFailure,
  MessagePolicyConfigurationFailure
>;

const RUN_SHARNESSD = "https://docs.moltzap.xyz/guides/run-an-agent-daemon";

type SecretFileFailure = Exclude<ProcessConfigurationFailure, "environment">;

const secretFileFailures: Readonly<
  Record<SecretFileFailure, { readonly text: string; readonly docs: string }>
> = {
  "agent-private-key-file": {
    text: "SHARNESS_AGENT_PRIVATE_KEY_FILE must name a readable file",
    docs: `${RUN_SHARNESSD}#agent-private-key-file`,
  },
  "agent-private-key": {
    text: "SHARNESS_AGENT_PRIVATE_KEY_FILE must contain an Ed25519 PKCS#8 private key",
    docs: `${RUN_SHARNESSD}#agent-private-key`,
  },
  "admission-credential-file": {
    text: "SHARNESS_ADMISSION_CREDENTIAL_FILE is required for an unregistered daemon and must name a readable file",
    docs: `${RUN_SHARNESSD}#admission-credential-file`,
  },
  "admission-credential": {
    text: "SHARNESS_ADMISSION_CREDENTIAL_FILE must contain a valid admission credential",
    docs: `${RUN_SHARNESSD}#admission-credential`,
  },
  "mcp-credential-file": {
    text: "SHARNESS_MCP_CREDENTIAL_FILE must name a readable file",
    docs: `${RUN_SHARNESSD}#mcp-credential-file`,
  },
  "mcp-credential": {
    text: "SHARNESS_MCP_CREDENTIAL_FILE must contain a valid token of at least 32 characters",
    docs: `${RUN_SHARNESSD}#mcp-credential`,
  },
};

const linked = (text: string, docs: string): string => `${text}. Docs: ${docs}`;

const configurationError = (
  reason: SecretFileFailure,
): DaemonConfigurationError =>
  new DaemonConfigurationError({
    reason,
    message: linked(
      secretFileFailures[reason].text,
      secretFileFailures[reason].docs,
    ),
  });

/** Validates daemon settings and keeps secret-file locations redacted. */
export const loadDaemonProcessConfiguration: Effect.Effect<
  DaemonProcessConfiguration,
  DaemonConfigurationError
> = configuredValues.pipe(
  Effect.flatMap((values) => {
    const {
      registrationAgentName,
      registrationOperationId,
      registrationPrincipalId,
      ...configuration
    } = values;
    return registrationRequest({
      registrationAgentName,
      registrationOperationId,
      registrationPrincipalId,
    }).pipe(
      Effect.map(
        (registration): DaemonProcessConfiguration => ({
          ...configuration,
          ...(registration === undefined ? {} : { registration }),
          agentPrivateKeyFile: Redacted.make(configuration.agentPrivateKeyFile),
          admissionCredentialFile:
            configuration.admissionCredentialFile === ""
              ? undefined
              : Redacted.make(configuration.admissionCredentialFile),
          mcpCredentialFile: Redacted.make(configuration.mcpCredentialFile),
          decisionProviderKeyFile:
            configuration.decisionProviderKeyFile === undefined
              ? undefined
              : Redacted.make(configuration.decisionProviderKeyFile),
        }),
      ),
    );
  }),
  Effect.mapError((cause) =>
    cause instanceof DaemonConfigurationError
      ? cause
      : new DaemonConfigurationError({
          reason: "environment",
          message: linked(
            configurationMessage(cause),
            `${RUN_SHARNESSD}#setting`,
          ),
        }),
  ),
  Effect.withSpan("loadDaemonProcessConfiguration"),
);

/** Requires all registration fields together so partial input cannot choose an identity. */
function registrationRequest(
  input: Pick<
    Config.Config.Success<typeof configuredValues>,
    | "registrationAgentName"
    | "registrationOperationId"
    | "registrationPrincipalId"
  >,
): Effect.Effect<
  DaemonProcessConfiguration["registration"],
  DaemonConfigurationError
> {
  const {
    registrationAgentName: agentName,
    registrationOperationId: operationId,
    registrationPrincipalId: principalId,
  } = input;
  if (
    agentName === undefined &&
    operationId === undefined &&
    principalId === undefined
  ) {
    return Effect.succeed(undefined);
  }
  if (
    agentName === undefined ||
    operationId === undefined ||
    principalId === undefined
  ) {
    return Effect.fail(
      new DaemonConfigurationError({
        reason: "environment",
        message: linked(
          "SHARNESS_REGISTRATION_AGENT_NAME, SHARNESS_REGISTRATION_OPERATION_ID and SHARNESS_REGISTRATION_PRINCIPAL_ID must all be supplied",
          `${RUN_SHARNESSD}#registration-settings`,
        ),
      }),
    );
  }
  return Effect.succeed({ agentName, operationId, principalId });
}

const credentialFailure =
  (fileReason: SecretFileFailure, valueReason: SecretFileFailure) =>
  (error: CredentialError): DaemonConfigurationError =>
    configurationError(error.stage === "file" ? fileReason : valueReason);

const mcpCredential = credentialText.pipe(Schema.minLength(32));

const loadMcpCredential = (configuration: DaemonProcessConfiguration) =>
  readCredential(configuration.mcpCredentialFile, mcpCredential).pipe(
    Effect.mapError(credentialFailure("mcp-credential-file", "mcp-credential")),
  );

const loadConfiguredAdmissionCredential = (
  configuration: DaemonProcessConfiguration,
): Effect.Effect<Redacted.Redacted, DaemonConfigurationError> =>
  configuration.admissionCredentialFile === undefined
    ? Effect.fail(configurationError("admission-credential-file"))
    : loadAdmissionCredential(configuration.admissionCredentialFile).pipe(
        Effect.mapError(
          credentialFailure(
            "admission-credential-file",
            "admission-credential",
          ),
        ),
      );

/**
 * Loads signing authority and the MCP credential. Defers admission-credential
 * loading until registration requires it.
 */
export const loadDaemonBootstrap = (
  configuration: DaemonProcessConfiguration,
): Effect.Effect<DaemonBootstrap, DaemonConfigurationError> =>
  Effect.gen(function* () {
    const signingAuthority = yield* loadSigningAuthority(
      configuration.agentPrivateKeyFile,
    ).pipe(
      Effect.mapError(
        credentialFailure("agent-private-key-file", "agent-private-key"),
      ),
    );
    const admissionCredential = yield* Effect.cached(
      loadConfiguredAdmissionCredential(configuration),
    );
    const mcpCredential = yield* loadMcpCredential(configuration);
    return Object.freeze({
      configuration,
      signingAuthority,
      admissionCredential,
      mcpCredential,
    });
  }).pipe(Effect.withSpan("loadDaemonBootstrap"));

/** Keeps each setting's parser, default and help text together. */
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

/** ConfigError trees retain every missing field without printing rejected inputs. */
function configurationMessage(error: ConfigError.ConfigError): string {
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

/** Only declared setting constraints may appear in invalid-value diagnostics. */
function invalidConfigurationMessage(error: ConfigError.InvalidData): string {
  const name = Array.from(error.path).join("_");
  const field = Object.values(configurationFields).find(
    (candidate) => candidate.name === name,
  );
  return field === undefined
    ? "environment configuration is invalid"
    : `${name} ${field.reason}`;
}

const otelVariable = (name: string) =>
  Config.option(Config.string(name)).pipe(
    Config.map((value) =>
      value.pipe(
        Option.map((text) => text.trim()),
        Option.filter((text) => text !== ""),
      ),
    ),
  );

const spanExportSettings = Config.all({
  endpoint: otelVariable("OTEL_EXPORTER_OTLP_ENDPOINT"),
  tracesEndpoint: otelVariable("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
  tracesProtocol: otelVariable("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"),
  protocol: otelVariable("OTEL_EXPORTER_OTLP_PROTOCOL"),
  serviceName: otelVariable("OTEL_SERVICE_NAME"),
  resourceAttributes: otelVariable("OTEL_RESOURCE_ATTRIBUTES"),
});

type SpanExportSettings = Config.Config.Success<typeof spanExportSettings>;

/**
 * Uses `OTEL_SERVICE_NAME`, then the last `service.name` resource attribute,
 * then `sharnessd`, matching OpenTelemetry's resource precedence.
 */
function spanServiceName(settings: SpanExportSettings): string {
  const attributed = Option.getOrElse(settings.resourceAttributes, () => "")
    .split(",")
    .map((attribute) => attribute.split("="))
    .filter(
      (parts) => parts.length === 2 && parts[0]?.trim() === "service.name",
    )
    .at(-1)?.[1]
    ?.trim();
  return Option.getOrElse(
    settings.serviceName,
    () => attributed ?? "sharnessd",
  );
}

/**
 * Appends `v1/traces` while preserving the query. Invalid endpoints pass through
 * to URL validation.
 */
function tracesUnder(endpoint: string): string {
  if (!URL.canParse(endpoint)) {
    return endpoint;
  }
  const url = new URL(endpoint);
  url.pathname = `${url.pathname.replace(/\/$/u, "")}/v1/traces`;
  return url.href;
}

/**
 * Selects the traces endpoint or appends `v1/traces` to the base endpoint.
 * Rejects non-HTTP(S) URLs and protocols other than `http/protobuf` before the
 * exporter can substitute a default.
 */
function spanExportUrl(
  endpoint: string,
  settings: SpanExportSettings,
): Either.Either<string, DaemonConfigurationError> {
  const protocol = settings.tracesProtocol.pipe(
    Option.orElse(() => settings.protocol),
    Option.getOrElse(() => "http/protobuf"),
  );
  if (protocol !== "http/protobuf") {
    return Either.left(
      new DaemonConfigurationError({
        reason: "environment",
        message:
          "The OTLP trace protocol, OTEL_EXPORTER_OTLP_TRACES_PROTOCOL or else OTEL_EXPORTER_OTLP_PROTOCOL, must be http/protobuf. Docs: https://opentelemetry.io/docs/specs/otel/protocol/exporter/#specify-protocol",
      }),
    );
  }
  const url = Option.getOrElse(settings.tracesEndpoint, () =>
    tracesUnder(endpoint),
  );
  return URL.canParse(url) && /^https?:$/u.test(new URL(url).protocol)
    ? Either.right(url)
    : Either.left(
        new DaemonConfigurationError({
          reason: "environment",
          message:
            "The OTLP traces URL, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or else OTEL_EXPORTER_OTLP_ENDPOINT, must be an http or https URL. Docs: https://opentelemetry.io/docs/specs/otel/protocol/exporter/#endpoint-urls-for-otlphttp",
        }),
      );
}

/**
 * Installs a process-global stderr logger for OpenTelemetry warnings and failures.
 * The logger remains installed after export closes.
 */
const exportDiagnostics = Layer.effectDiscard(
  Effect.sync(() =>
    diag.setLogger(new DiagConsoleLogger(), {
      logLevel: DiagLogLevel.WARN,
      suppressOverrideMessage: true,
    }),
  ),
);

/**
 * Exports spans as OTLP/HTTP protobuf when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
 * Invalid endpoints and unsupported protocols fail startup. OpenTelemetry reads
 * its remaining exporter, batch and resource settings from standard variables.
 */
// eslint-disable-next-line agent-code-guard/require-span-on-exported-effect -- The exporter installs tracing; tracing its acquisition changes the daemon's exported spans.
export const spanExport: Layer.Layer<never, DaemonConfigurationError> =
  Layer.unwrapEffect(
    Effect.gen(function* () {
      const settings = yield* spanExportSettings.pipe(Effect.orDie);
      if (Option.isNone(settings.endpoint)) {
        return Layer.empty;
      }
      const url = yield* spanExportUrl(settings.endpoint.value, settings);
      return NodeSdk.layer(() => ({
        resource: {
          serviceName: spanServiceName(settings),
          serviceVersion: packageVersion,
        },
        spanProcessor: new BatchSpanProcessor(new OTLPTraceExporter({ url })),
      })).pipe(Layer.discard, Layer.provide(exportDiagnostics));
    }),
  );
