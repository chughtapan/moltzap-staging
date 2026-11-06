/** @file Loads runtime daemon settings and resolves their policy and norms references from one file. */
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
import { exactStruct } from "../encoding/canonical-json.js";
import {
  installedMessagePolicy,
  loadMessagePolicy,
  readPolicyCredential,
} from "../firewall/rules.js";
import {
  AgentName,
  AgentPublicKey,
  PrincipalId,
} from "../identity/agent-cards.js";
import { RegistrationId } from "../identity/registry.js";
import {
  initialNormsHaveRules,
  installedNorms,
  loadNorms,
  type NormsConfigurationError,
} from "../norms/configuration.js";
import { packageVersion } from "../version.js";

const configurationDocs =
  "https://docs.moltzap.xyz/reference/spec/daemon#process-and-configuration";

/** Messages identify the failed operation without disclosing documents or credentials. */
export class DaemonConfigurationError extends Data.TaggedError(
  "DaemonConfigurationError",
)<{
  readonly reason: "environment" | "document" | "policy" | "norms";
  readonly message: string;
}> {}

const refused = (reason: DaemonConfigurationError["reason"], message: string) =>
  new DaemonConfigurationError({
    reason,
    message: `${message}. Docs: ${configurationDocs}`,
  });

const origin = Schema.String.pipe(
  Schema.filter((value) => {
    if (!URL.canParse(value)) {
      return false;
    }
    const url = new URL(value);
    return ["http:", "https:"].includes(url.protocol) && value === url.origin;
  }),
  Schema.transform(Schema.URLFromSelf, {
    strict: true,
    decode: (value) => new URL(value),
    encode: (value) => value.origin,
  }),
);

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

const credential = Schema.Redacted(
  Schema.String.pipe(
    Schema.minLength(8),
    Schema.maxLength(512),
    Schema.pattern(/^[A-Za-z0-9\-._~+/]+=*$/u),
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
const localBearer = exactStruct({
  kind: Schema.Literal("local"),
  credential: Schema.Redacted(
    Schema.String.pipe(
      Schema.minLength(32),
      Schema.maxLength(512),
      Schema.pattern(/^[A-Za-z0-9\-._~+/]+=*$/u),
    ),
  ),
});

const commonFields = {
  port: Schema.Int.pipe(Schema.between(1, 65535)),
  host: Schema.NonEmptyString,
  authentication: Schema.Union(localBearer, publicOAuth),
  registryOrigin: origin,
  registrySignerPublicKey: AgentPublicKey,
  routerOrigin: origin,
  admissionCredential: Schema.optional(credential),
  hideGroups: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  hideCollectives: Schema.optionalWith(Schema.Boolean, {
    default: () => false,
  }),
};

const settings = exactStruct({
  ...commonFields,
  messagePolicy: Schema.optional(installedMessagePolicy),
  norms: Schema.optional(installedNorms),
}).pipe(
  Schema.filter(
    (value) =>
      value.authentication.kind === "oauth" || value.host === "127.0.0.1",
  ),
);

/** Process settings contain resolved policy and credential values for this startup. */
export type DaemonSettings = typeof settings.Type;

const seed = exactStruct({
  agentName: AgentName,
  principalId: PrincipalId,
  operationId: RegistrationId,
  privateKey: Schema.Redacted(Schema.NonEmptyString),
});
const setup = exactStruct({
  ...commonFields,
  agent: Schema.optional(seed),
  messagePolicyFile: Schema.optional(Schema.NonEmptyString),
  decisionProviderKeyFile: Schema.optional(Schema.NonEmptyString),
  normsDirectory: Schema.optional(Schema.NonEmptyString),
}).pipe(
  Schema.filter(
    (value) =>
      value.authentication.kind === "local" || value.agent === undefined,
  ),
);

/** The document and its referenced files are read on every daemon start. */
export const processHelp = [
  "Usage: sharnessd [--help]",
  "",
  "SHARNESS_CONFIG_FILE: JSON daemon settings file",
  "SHARNESS_DATABASE_URL: sqlite file URL or PostgreSQL connection URL",
  "SHARNESS_DATABASE_KEY: base64url-encoded 32-byte encryption key",
  "Deployment prepares the database schema and encryption verifier before startup.",
  "Standard OTEL_* variables configure telemetry.",
  "",
  `Docs: ${configurationDocs}`,
].join("\n");

/** Policy and norms references resolve against the runtime document, independent of the process directory. */
export const loadDaemonSettings = Effect.fn("loadDaemonSettings")(function* (
  filename?: string,
) {
  const selected =
    filename ??
    (yield* Config.string("SHARNESS_CONFIG_FILE").pipe(
      Effect.mapError(() =>
        refused(
          "environment",
          "Set SHARNESS_CONFIG_FILE to the daemon settings document",
        ),
      ),
    ));
  const filesystem = yield* FileSystem.FileSystem;
  const document = yield* filesystem.readFileString(selected).pipe(
    Effect.flatMap(
      Schema.decodeUnknown(Schema.parseJson(setup), {
        onExcessProperty: "error",
      }),
    ),
    Effect.mapError(() =>
      refused(
        "document",
        "The daemon settings document is unreadable or invalid",
      ),
    ),
  );
  const root = dirname(resolve(selected));
  const norms =
    document.normsDirectory === undefined
      ? undefined
      : yield* configuredNorms(resolve(root, document.normsDirectory));
  if (norms !== undefined && document.messagePolicyFile === undefined) {
    return yield* refused(
      "norms",
      "The configured norms need a messagePolicyFile: each agent holds its norms in its editable message policy",
    );
  }
  const messagePolicy = yield* loadMessagePolicy(
    {
      messagePolicyFile:
        document.messagePolicyFile === undefined
          ? undefined
          : resolve(root, document.messagePolicyFile),
      decisionProviderKeyFile:
        document.decisionProviderKeyFile === undefined
          ? undefined
          : Redacted.make(resolve(root, document.decisionProviderKeyFile)),
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
  const values = Struct.omit(
    document,
    "agent",
    "messagePolicyFile",
    "decisionProviderKeyFile",
    "normsDirectory",
  );
  const captured = yield* Schema.validate(settings)({
    ...values,
    messagePolicy: Option.getOrUndefined(messagePolicy),
    ...(norms === undefined ? {} : { norms }),
  }).pipe(
    Effect.mapError(() =>
      refused(
        "document",
        "Local bearer access requires the 127.0.0.1 listener",
      ),
    ),
  );
  return {
    settings: captured,
    ...(document.agent === undefined ? {} : { agent: document.agent }),
  };
}, Effect.withSpan("loadDaemonSettings"));

/** The settings document's norms directory, read and checked. */
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
