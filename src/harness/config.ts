/** @file Reads the settings file and the files it names into the daemon's settings. */
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { FileSystem } from "@effect/platform";
import { diag, DiagConsoleLogger, DiagLogLevel } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import {
  Config,
  Data,
  Effect,
  Either,
  Layer,
  Option,
  Redacted,
  Schema,
  Struct,
} from "effect";
import { dirname, resolve } from "node:path";
import { exactStruct } from "../encoding.js";
import {
  installedMessagePolicy,
  loadMessagePolicy,
  readPolicyCredential,
} from "../firewall/index.js";
import { AgentName } from "../identity.js";
import {
  initialNormsHaveRules,
  installedNorms,
  loadNorms,
  type NormsConfigurationError,
} from "../norms/index.js";
import { packageVersion } from "../version.js";

/** Messages identify the failed operation without disclosing documents or credentials. */
export class DaemonConfigurationError extends Data.TaggedError(
  "DaemonConfigurationError",
)<{
  readonly reason:
    | "environment"
    | "settings-file"
    | "policy"
    | "norms"
    | "database"
    | "issuer"
    | "listener";
  readonly message: string;
}> {}

/** A startup refusal whose message names the setting to change. */
export const refused = (
  reason: DaemonConfigurationError["reason"],
  message: string,
) =>
  new DaemonConfigurationError({
    reason,
    message,
  });

const httpsUrl = Schema.String.pipe(
  Schema.filter((value) => {
    if (!URL.canParse(value)) {
      return false;
    }
    const url = new URL(value);
    return (
      url.protocol === "https:" &&
      [url.username, url.password, url.search, url.hash].every(
        (part) => part === "",
      )
    );
  }),
);

/** Plaintext discovery is limited to local issuer fixtures; hosted providers require HTTPS. */
const oauthIssuer = Schema.Union(
  httpsUrl,
  Schema.String.pipe(
    Schema.filter((value) => {
      if (!URL.canParse(value)) {
        return false;
      }
      const url = new URL(value);
      return (
        url.protocol === "http:" &&
        url.hostname === "127.0.0.1" &&
        [url.username, url.password, url.search, url.hash].every(
          (part) => part === "",
        )
      );
    }),
  ),
);

const publicOAuth = exactStruct({
  kind: Schema.Literal("oauth"),
  resource: httpsUrl.pipe(
    Schema.filter(
      (value) =>
        new URL(value).pathname === "/mcp" && new URL(value).href === value,
    ),
    Schema.compose(Schema.URL),
  ),
  issuer: oauthIssuer,
  scope: Schema.String.pipe(Schema.pattern(/^[\x21\x23-\x5B\x5D-\x7E]+$/u)),
  allowedOrigins: Schema.optionalWith(
    Schema.Array(
      httpsUrl.pipe(Schema.filter((value) => new URL(value).origin === value)),
    ),
    { default: () => [] },
  ),
});
/** A secret a caller presents as an HTTP bearer. */
const bearerSecret = Schema.String.pipe(
  Schema.minLength(32),
  Schema.maxLength(512),
  Schema.pattern(/^[A-Za-z0-9\-._~+/]+=*$/u),
);
const localBearer = exactStruct({
  kind: Schema.Literal("local"),
  credential: Schema.Redacted(bearerSecret),
});

const commonFields = {
  port: Schema.Int.pipe(Schema.between(1, 65535)),
  host: Schema.NonEmptyString,
  authentication: Schema.Union(localBearer, publicOAuth),
  serverAgentName: Schema.optional(AgentName),
  hideGroups: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  hideCollectives: Schema.optionalWith(Schema.Boolean, {
    default: () => false,
  }),
};

const daemonSettings = exactStruct({
  ...commonFields,
  messagePolicy: Schema.optional(installedMessagePolicy),
  norms: Schema.optional(installedNorms),
}).pipe(
  Schema.filter(
    (settings) =>
      settings.authentication.kind === "oauth" || settings.host === "127.0.0.1",
  ),
);

/**
 * What the daemon holds after reading the settings file and the files it
 * names: the message policy, the provider credential and the norms
 * themselves, in place of their paths.
 */
export type DaemonSettings = typeof daemonSettings.Type;

/** The settings file as an operator writes it. */
const settingsFile = exactStruct({
  ...commonFields,
  agentName: Schema.optional(AgentName),
  messagePolicyFile: Schema.optional(Schema.NonEmptyString),
  decisionProviderKeyFile: Schema.optional(Schema.NonEmptyString),
  normsDirectory: Schema.optional(Schema.NonEmptyString),
}).pipe(
  Schema.filter(
    (file) =>
      file.authentication.kind === "local" || file.agentName === undefined,
  ),
);

/** What `sharnessd --help` prints: the variables the daemon reads at every start. */
export const daemonHelp = [
  "Usage: sharnessd [--help]",
  "",
  "SHARNESS_CONFIG_FILE: JSON daemon settings file",
  "SHARNESS_DATABASE_URL: sqlite file URL or PostgreSQL connection URL",
  "SHARNESS_SERVER_AGENT_CREDENTIAL: when the settings name a server agent, the secret it signs in with",
  "Deployment prepares the database schema before startup.",
  "Standard OTEL_* variables configure telemetry.",
].join("\n");

/**
 * The paths the settings file names resolve against its own directory,
 * whatever the daemon's working directory.
 */
export const loadDaemonSettings = Effect.fn("loadDaemonSettings")(function* () {
  const settingsPath = yield* Config.string("SHARNESS_CONFIG_FILE").pipe(
    Effect.mapError(() =>
      refused(
        "environment",
        "Set SHARNESS_CONFIG_FILE to the daemon settings file",
      ),
    ),
  );
  const filesystem = yield* FileSystem.FileSystem;
  const file = yield* filesystem.readFileString(settingsPath).pipe(
    Effect.flatMap(
      Schema.decodeUnknown(Schema.parseJson(settingsFile), {
        onExcessProperty: "error",
      }),
    ),
    Effect.mapError(() =>
      refused(
        "settings-file",
        "The daemon settings file is unreadable or invalid",
      ),
    ),
  );
  const settingsDirectory = dirname(resolve(settingsPath));
  const norms =
    file.normsDirectory === undefined
      ? undefined
      : yield* configuredNorms(resolve(settingsDirectory, file.normsDirectory));
  if (norms !== undefined && file.messagePolicyFile === undefined) {
    return yield* refused(
      "norms",
      "The configured norms need a messagePolicyFile: each agent holds its norms in its editable message policy",
    );
  }
  const messagePolicy = yield* loadMessagePolicy(
    {
      messagePolicyFile:
        file.messagePolicyFile === undefined
          ? undefined
          : resolve(settingsDirectory, file.messagePolicyFile),
      decisionProviderKeyFile:
        file.decisionProviderKeyFile === undefined
          ? undefined
          : Redacted.make(
              resolve(settingsDirectory, file.decisionProviderKeyFile),
            ),
    },
    readPolicyCredential,
  ).pipe(
    Effect.mapError(() =>
      refused(
        "policy",
        "The configured message policy or provider credential is invalid",
      ),
    ),
  );
  if (
    norms !== undefined &&
    initialNormsHaveRules(norms) &&
    Option.getOrUndefined(messagePolicy)?.provider === undefined
  ) {
    return yield* refused(
      "norms",
      "The configured norms have English rules, which need a messagePolicyFile with a decisionProvider to check them",
    );
  }
  const settings = yield* Schema.validate(daemonSettings)({
    ...Struct.omit(
      file,
      "agentName",
      "messagePolicyFile",
      "decisionProviderKeyFile",
      "normsDirectory",
    ),
    messagePolicy: Option.getOrUndefined(messagePolicy),
    ...(norms === undefined ? {} : { norms }),
  }).pipe(
    Effect.mapError(() =>
      refused(
        "settings-file",
        "Local bearer access requires the 127.0.0.1 listener",
      ),
    ),
  );
  return {
    settings,
    serverAgentCredential: yield* readServerAgentCredential(settings),
    ...(file.agentName === undefined ? {} : { agentName: file.agentName }),
  };
}, Effect.withSpan("loadDaemonSettings"));

/**
 * The secret the server agent signs in with, from the environment so that it
 * is never written to a file. A daemon whose settings name a server agent
 * requires it; one without a server agent does not read it.
 */
function readServerAgentCredential(settings: DaemonSettings) {
  return Effect.gen(function* () {
    const configured = yield* Config.option(
      Config.redacted("SHARNESS_SERVER_AGENT_CREDENTIAL"),
    ).pipe(Effect.orElseSucceed(() => Option.none<Redacted.Redacted>()));
    if (settings.serverAgentName === undefined) {
      return Option.none<Redacted.Redacted>();
    }
    if (Option.isNone(configured)) {
      return yield* refused(
        "environment",
        "Set SHARNESS_SERVER_AGENT_CREDENTIAL to the secret the server agent signs in with: 32 to 512 letters, digits or -._~+/ characters",
      );
    }
    return Schema.is(bearerSecret)(Redacted.value(configured.value))
      ? configured
      : yield* refused(
          "environment",
          "SHARNESS_SERVER_AGENT_CREDENTIAL must be 32 to 512 letters, digits or -._~+/ characters",
        );
  });
}

/** The settings file's norms directory, read and checked. */
function configuredNorms(directory: string) {
  return loadNorms(directory).pipe(
    Effect.mapError((error: NormsConfigurationError) =>
      refused(
        "norms",
        `The configured norms directory is invalid: ${error.reason}`,
      ),
    ),
  );
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

const otelVariables = Config.all({
  endpoint: otelVariable("OTEL_EXPORTER_OTLP_ENDPOINT"),
  tracesEndpoint: otelVariable("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
  tracesProtocol: otelVariable("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"),
  protocol: otelVariable("OTEL_EXPORTER_OTLP_PROTOCOL"),
  serviceName: otelVariable("OTEL_SERVICE_NAME"),
  resourceAttributes: otelVariable("OTEL_RESOURCE_ATTRIBUTES"),
});

type OtelVariables = Config.Config.Success<typeof otelVariables>;

/**
 * Uses `OTEL_SERVICE_NAME`, then the last `service.name` resource attribute,
 * then `sharnessd`, matching OpenTelemetry's resource precedence.
 */
function spanServiceName(otel: OtelVariables): string {
  const attributed = Option.getOrElse(otel.resourceAttributes, () => "")
    .split(",")
    .map((attribute) => attribute.split("="))
    .filter(
      (parts) => parts.length === 2 && parts[0]?.trim() === "service.name",
    )
    .at(-1)?.[1]
    ?.trim();
  return Option.getOrElse(otel.serviceName, () => attributed ?? "sharnessd");
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
  otel: OtelVariables,
): Either.Either<string, DaemonConfigurationError> {
  const protocol = otel.tracesProtocol.pipe(
    Option.orElse(() => otel.protocol),
    Option.getOrElse(() => "http/protobuf"),
  );
  if (protocol !== "http/protobuf") {
    return Either.left(
      refused(
        "environment",
        "The OTLP trace protocol, OTEL_EXPORTER_OTLP_TRACES_PROTOCOL or else OTEL_EXPORTER_OTLP_PROTOCOL, must be http/protobuf",
      ),
    );
  }
  const url = Option.getOrElse(otel.tracesEndpoint, () =>
    tracesUnder(endpoint),
  );
  return URL.canParse(url) && /^https?:$/u.test(new URL(url).protocol)
    ? Either.right(url)
    : Either.left(
        refused(
          "environment",
          "The OTLP traces URL, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or else OTEL_EXPORTER_OTLP_ENDPOINT, must be an http or https URL",
        ),
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
      const otel = yield* otelVariables.pipe(Effect.orDie);
      if (Option.isNone(otel.endpoint)) {
        return Layer.empty;
      }
      const url = yield* spanExportUrl(otel.endpoint.value, otel);
      return NodeSdk.layer(() => ({
        resource: {
          serviceName: spanServiceName(otel),
          serviceVersion: packageVersion,
        },
        spanProcessor: new BatchSpanProcessor(new OTLPTraceExporter({ url })),
      })).pipe(Layer.discard, Layer.provide(exportDiagnostics));
    }),
  );
