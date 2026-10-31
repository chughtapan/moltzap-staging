/** @file Loads the daemon process configuration and its redacted bootstrap material from the environment. */

import {
  Config,
  type ConfigError,
  Effect,
  Match,
  Redacted,
  Schema,
} from "effect";
import type { MessagePolicyConfigurationFailure } from "../firewall/config.js";
import type { DaemonOAuth } from "./mcp/index.js";
import { AgentName, PrincipalId } from "../identity/identifiers.js";
import { AgentSigningAuthority, Ed25519PublicKey } from "../identity/keys.js";
import { OperationId } from "../identity/registry/contract.js";
import {
  type DaemonBootstrap,
  DaemonConfigurationError,
  type DaemonConfigurationFailure,
  type DaemonProcessConfiguration,
} from "./bootstrap.js";
import {
  type CredentialError,
  credentialText,
  loadAdmissionCredential,
  loadSigningAuthority,
  readCredential,
} from "./credentials.js";

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
const compactPublicKeyJson = Schema.String.pipe(
  Schema.pattern(/^\{"crv":"Ed25519","kty":"OKP","x":"[A-Za-z0-9_-]{43}"\}$/u),
  Schema.compose(Schema.parseJson(Ed25519PublicKey)),
);
/** `Config.boolean`'s spellings, read case-sensitively. */
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
    OperationId,
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
    compactPublicKeyJson,
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
  mcpCredentialFile: setting(
    "SHARNESS_MCP_CREDENTIAL_FILE",
    configuredPath,
    undefined,
  ),
  mcpBindHost: setting("SHARNESS_MCP_BIND_HOST", configuredPath, "127.0.0.1"),
  mcpPublicUrl: setting("SHARNESS_MCP_PUBLIC_URL", configuredPath, undefined),
  oauthIssuer: setting("SHARNESS_OAUTH_ISSUER", configuredPath, undefined),
  oauthOwnerSubject: setting(
    "SHARNESS_OAUTH_OWNER_SUBJECT",
    configuredPath,
    undefined,
  ),
  oauthRequiredScope: setting(
    "SHARNESS_OAUTH_REQUIRED_SCOPE",
    configuredPath,
    undefined,
  ),
  oauthAllowedOrigins: setting(
    "SHARNESS_OAUTH_ALLOWED_ORIGINS",
    Schema.parseJson(Schema.Array(Schema.String)),
    [],
  ),
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
  "  Admission credential: required only before registration; MCP: local bearer or complete OAuth settings.",
  "  Message policy: unset, no firewall; the decision-provider key applies only with it.",
  ...Object.values(configurationFields).map((field) => `  ${field.usage}`),
  "",
  "Docs: https://docs.moltzap.xyz/reference/spec/daemon/daemon#process-and-configuration",
].join("\n");

/** The process settings' failures; message-policy failures carry their own messages. */
type ProcessConfigurationFailure = Exclude<
  DaemonConfigurationFailure,
  MessagePolicyConfigurationFailure
>;

const RUN_SHARNESSD = "https://docs.moltzap.xyz/guides/run-sharnessd";

/** A secret-file failure of the process settings. */
type SecretFileFailure = Exclude<ProcessConfigurationFailure, "environment">;

/** Each secret-file failure's operator text and its troubleshooting entry in Run sharnessd. */
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

/** An operator message followed by its one troubleshooting link. */
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

/** Loads explicit daemon inputs with optional export and MCP authority separation. */
export const loadDaemonProcessConfiguration: Effect.Effect<
  DaemonProcessConfiguration,
  DaemonConfigurationError
> = configuredValues.pipe(
  Effect.flatMap((values) => {
    const {
      registrationAgentName,
      registrationOperationId,
      registrationPrincipalId,
      mcpPublicUrl,
      oauthIssuer,
      oauthOwnerSubject,
      oauthRequiredScope,
      oauthAllowedOrigins,
      ...configuration
    } = values;
    const oauth = oauthConfiguration({
      mcpPublicUrl,
      oauthIssuer,
      oauthOwnerSubject,
      oauthRequiredScope,
      oauthAllowedOrigins,
      credential: configuration.mcpCredentialFile,
      host: configuration.mcpBindHost,
    });
    return oauth.pipe(
      Effect.flatMap((oauth) =>
        registrationRequest({
          registrationAgentName,
          registrationOperationId,
          registrationPrincipalId,
        }).pipe(
          Effect.map((registration) =>
            processConfiguration(configuration, { oauth, registration }),
          ),
        ),
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

/** A startup request is complete or omitted; partial registration cannot choose an identity. */
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
  configuration.mcpCredentialFile === undefined
    ? Effect.succeed(undefined)
    : readCredential(configuration.mcpCredentialFile, mcpCredential).pipe(
        Effect.mapError(
          credentialFailure("mcp-credential-file", "mcp-credential"),
        ),
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
 * Reads the agent private key and constructs the configured Ed25519 authority.
 *
 * @param configuration Validated process configuration and redacted credential paths.
 * @returns Opaque signing authority, the MCP credential, and deferred admission credential.
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
      agentPublicKey: AgentSigningAuthority.publicKey(signingAuthority),
      admissionCredential,
      mcpCredential,
    });
  }).pipe(Effect.withSpan("loadDaemonBootstrap"));

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

function processConfiguration(
  configuration: Omit<
    Effect.Effect.Success<typeof configuredValues>,
    | "mcpPublicUrl"
    | "oauthIssuer"
    | "oauthOwnerSubject"
    | "oauthRequiredScope"
    | "oauthAllowedOrigins"
    | "registrationAgentName"
    | "registrationOperationId"
    | "registrationPrincipalId"
  >,
  authority: {
    readonly oauth?: DaemonOAuth;
    readonly registration?: DaemonProcessConfiguration["registration"];
  },
): DaemonProcessConfiguration {
  return {
    ...configuration,
    ...(authority.oauth === undefined ? {} : { oauth: authority.oauth }),
    ...(authority.registration === undefined
      ? {}
      : { registration: authority.registration }),
    agentPrivateKeyFile: Redacted.make(configuration.agentPrivateKeyFile),
    admissionCredentialFile:
      configuration.admissionCredentialFile === ""
        ? undefined
        : Redacted.make(configuration.admissionCredentialFile),
    mcpCredentialFile:
      configuration.mcpCredentialFile === undefined
        ? undefined
        : Redacted.make(configuration.mcpCredentialFile),
    decisionProviderKeyFile:
      configuration.decisionProviderKeyFile === undefined
        ? undefined
        : Redacted.make(configuration.decisionProviderKeyFile),
  };
}

interface OAuthInputs {
  readonly mcpPublicUrl?: string;
  readonly oauthIssuer?: string;
  readonly oauthOwnerSubject?: string;
  readonly oauthRequiredScope?: string;
  readonly oauthAllowedOrigins: readonly string[];
  readonly credential?: string;
  readonly host: string;
}

/** Public authority is complete and exclusive, and never derives from request headers. */
function oauthConfiguration(
  input: OAuthInputs,
): Effect.Effect<DaemonOAuth | undefined, DaemonConfigurationError> {
  const absent = [
    input.mcpPublicUrl,
    input.oauthIssuer,
    input.oauthOwnerSubject,
    input.oauthRequiredScope,
  ].every((value) => value === undefined);
  if (!absent) {
    return publicOAuthConfiguration(input);
  }
  return input.credential !== undefined &&
    input.host === "127.0.0.1" &&
    input.oauthAllowedOrigins.length === 0
    ? Effect.succeed(undefined)
    : Effect.fail(invalidOAuthConfiguration());
}

function publicOAuthConfiguration(
  input: OAuthInputs,
): Effect.Effect<DaemonOAuth, DaemonConfigurationError> {
  if (
    !completeOAuth(input) ||
    !secureUrl(input.mcpPublicUrl) ||
    !secureUrl(input.oauthIssuer)
  ) {
    return Effect.fail(invalidOAuthConfiguration());
  }
  const resource = new URL(input.mcpPublicUrl);
  if (
    !validResource(resource, input.mcpPublicUrl) ||
    !validScope(input.oauthRequiredScope) ||
    !validOrigins(input.oauthAllowedOrigins)
  ) {
    return Effect.fail(invalidOAuthConfiguration());
  }
  return Effect.succeed({
    resource,
    issuer: input.oauthIssuer,
    ownerSubject: input.oauthOwnerSubject,
    scope: input.oauthRequiredScope,
    allowedOrigins: input.oauthAllowedOrigins,
  });
}

function completeOAuth(
  input: OAuthInputs,
): input is OAuthInputs &
  Required<
    Pick<
      OAuthInputs,
      | "mcpPublicUrl"
      | "oauthIssuer"
      | "oauthOwnerSubject"
      | "oauthRequiredScope"
    >
  > {
  return (
    [
      input.mcpPublicUrl,
      input.oauthIssuer,
      input.oauthOwnerSubject,
      input.oauthRequiredScope,
    ].every((value) => value !== undefined) && input.credential === undefined
  );
}

function validResource(url: URL, supplied: string): boolean {
  return url.pathname === "/mcp" && url.href === supplied;
}

function validScope(value: string): boolean {
  return /^[\x21\x23-\x5B\x5D-\x7E]+$/u.test(value);
}

function validOrigins(values: readonly string[]): boolean {
  return values.every(
    (value) => isSerializedOrigin(value) && value.startsWith("https://"),
  );
}

function invalidOAuthConfiguration(): DaemonConfigurationError {
  return new DaemonConfigurationError({
    reason: "environment",
    message: linked(
      "Configure either loopback MCP bearer access or complete public OAuth settings with a canonical HTTPS /mcp resource",
      `${RUN_SHARNESSD}#setting`,
    ),
  });
}

function secureUrl(value: string): boolean {
  if (!URL.canParse(value) || value.includes("?") || value.includes("#")) {
    return false;
  }
  const url = new URL(value);
  return (
    url.protocol === "https:" &&
    [url.username, url.password, url.search, url.hash].every(
      (component) => component === "",
    )
  );
}
