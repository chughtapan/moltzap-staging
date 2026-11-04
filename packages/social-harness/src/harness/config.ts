/** @file Persists validated daemon settings and leaves only SQL access and unlocking authority outside SQL. */
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { FileSystem } from "@effect/platform";
import { SqlClient } from "@effect/sql";
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
import { daemonDatabaseFormat, migrateDaemon } from "../database/migrations.js";
import { openCredential, sealCredential } from "../database/secrets.js";
import {
  databaseConnection,
  executeQuery,
  makeQueries,
  prepareDatabaseDirectory,
} from "../database/sql.js";
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
import { packageVersion } from "../version.js";
import { getOrCreateOwnedAgent, localOwner } from "./ownership.js";

const configurationDocs =
  "https://docs.moltzap.xyz/reference/spec/daemon#process-and-configuration";

/** Messages identify the failed operation without disclosing documents or credentials. */
export class DaemonConfigurationError extends Data.TaggedError(
  "DaemonConfigurationError",
)<{
  readonly reason: "environment" | "document" | "missing" | "stored" | "policy";
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
  historyExport: Schema.optional(Schema.NonEmptyString),
  hideGroups: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  hideCollectives: Schema.optionalWith(Schema.Boolean, {
    default: () => false,
  }),
};

const settings = exactStruct({
  ...commonFields,
  messagePolicy: Schema.optional(installedMessagePolicy),
}).pipe(
  Schema.filter(
    (value) =>
      value.authentication.kind === "oauth" || value.host === "127.0.0.1",
  ),
);

/** The saved model contains resolved policy and credential values, never setup-file references. */
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
}).pipe(
  Schema.filter(
    (value) =>
      value.authentication.kind === "local" || value.agent === undefined,
  ),
);

/** The process needs only the database locator and one canonical 32-byte unlock key. */
export const databaseAccess = Config.all({
  locator: Config.redacted("SHARNESS_DATABASE_URL"),
  encodedKey: Config.redacted("SHARNESS_DATABASE_KEY"),
}).pipe(
  Effect.flatMap(({ locator, encodedKey }) =>
    Schema.decodeUnknown(
      Schema.Uint8ArrayFromBase64Url.pipe(
        Schema.filter((bytes) => bytes.byteLength === 32),
      ),
    )(Redacted.value(encodedKey)).pipe(
      Effect.map((bytes) => ({ locator, unlockKey: Redacted.make(bytes) })),
    ),
  ),
  Effect.mapError(() =>
    refused(
      "environment",
      "Set SHARNESS_DATABASE_URL and a base64url-encoded 32-byte SHARNESS_DATABASE_KEY",
    ),
  ),
  Effect.withSpan("databaseAccess"),
);

/** Setup reads a JSON document once; ordinary startup reads the encrypted SQL record. */
export const processHelp = [
  "Usage: sharnessd configure <settings.json> | sharnessd [--help]",
  "",
  "SHARNESS_DATABASE_URL: sqlite file URL or PostgreSQL connection URL",
  "SHARNESS_DATABASE_KEY: base64url-encoded 32-byte encryption key",
  "Standard OTEL_* variables configure telemetry.",
  "",
  `Docs: ${configurationDocs}`,
].join("\n");

interface SettingsTable {
  // eslint-disable-next-line @typescript-eslint/naming-convention -- SQL owns the physical table name.
  readonly daemon_settings: {
    readonly singleton: number;
    readonly settings: string;
  };
}
const settingsRecord = "daemon/settings";

/** Incompatible SQLite files are rejected before migration can alter their contents. */
export const prepareDatabase = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.onDialectOrElse({
    sqlite: () =>
      Effect.gen(function* () {
        // eslint-disable-next-line @typescript-eslint/naming-convention -- SQLite owns the PRAGMA result name.
        const [version] = yield* sql.unsafe<{ readonly user_version: number }>(
          "PRAGMA user_version",
        );
        const [tables] = yield* sql<{
          readonly count: number;
        }>`SELECT COUNT(*) AS count FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`;
        if (
          !(version?.user_version === 0 && tables?.count === 0) &&
          version?.user_version !== daemonDatabaseFormat
        ) {
          return yield* refused(
            "stored",
            "Use a fresh database for this reset release",
          );
        }
        yield* sql.unsafe("PRAGMA foreign_keys = ON");
        yield* sql.unsafe("PRAGMA journal_mode = WAL");
        yield* sql.unsafe("PRAGMA synchronous = FULL");
      }),
    orElse: () => Effect.void,
  });
}).pipe(Effect.withSpan("prepareDatabase"));

/** Configuration and the optional self-hosted identity seed commit together. */
export const configureDaemon = Effect.fn("configureDaemon")(
  function* (filename: string) {
    const filesystem = yield* FileSystem.FileSystem;
    const access = yield* databaseAccess;
    const document = yield* filesystem.readFileString(filename).pipe(
      Effect.flatMap(
        Schema.decodeUnknown(Schema.parseJson(setup), {
          onExcessProperty: "error",
        }),
      ),
      Effect.mapError(() =>
        refused(
          "document",
          "The daemon setup document is unreadable or invalid",
        ),
      ),
    );
    const root = dirname(resolve(filename));
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
    const values = Struct.omit(
      document,
      "agent",
      "messagePolicyFile",
      "decisionProviderKeyFile",
    );
    const captured = yield* Schema.validate(settings)({
      ...values,
      messagePolicy: Option.getOrUndefined(messagePolicy),
    }).pipe(
      Effect.mapError(() =>
        refused(
          "document",
          "Local bearer access requires the 127.0.0.1 listener",
        ),
      ),
    );
    yield* prepareDatabaseDirectory(access.locator).pipe(
      Effect.catchTag("DatabaseLocatorError", () =>
        Effect.fail(refused("environment", "The database locator is invalid")),
      ),
    );
    return yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* prepareDatabase;
      yield* migrateDaemon;
      const queries = yield* makeQueries<SettingsTable>();
      const prior = (yield* executeQuery(
        queries
          .selectFrom("daemon_settings")
          .select("settings")
          .where("singleton", "=", 1),
      ))[0];
      if (prior !== undefined) {
        yield* openCredential(
          prior.settings,
          settingsRecord,
          access.unlockKey,
        ).pipe(
          Effect.mapError(() =>
            refused("stored", "Stored daemon configuration cannot be unlocked"),
          ),
        );
      }
      const encoded = yield* Schema.encode(Schema.parseJson(settings))(
        captured,
      );
      const sealed = yield* sealCredential(
        Redacted.make(encoded),
        settingsRecord,
        access.unlockKey,
      );
      yield* sql.withTransaction(
        Effect.gen(function* () {
          if (captured.authentication.kind === "local") {
            yield* getOrCreateOwnedAgent(
              localOwner,
              access.unlockKey,
              document.agent,
            );
          }
          yield* executeQuery(
            queries
              .insertInto("daemon_settings")
              .values({ singleton: 1, settings: sealed })
              .onConflict((conflict) =>
                conflict.column("singleton").doUpdateSet({ settings: sealed }),
              ),
          );
        }),
      );
    }).pipe(Effect.provide(databaseConnection(access.locator)));
  },
  Effect.scoped,
  Effect.withSpan("configureDaemon"),
);

/** Stored settings are decoded after authenticated decryption, with safe failures at the process boundary. */
export const loadDaemonSettings = Effect.fn("loadDaemonSettings")(function* (
  unlockKey: Redacted.Redacted<Uint8Array>,
) {
  const queries = yield* makeQueries<SettingsTable>();
  const rows = yield* executeQuery(
    queries
      .selectFrom("daemon_settings")
      .select("settings")
      .where("singleton", "=", 1),
  );
  const row = rows[0];
  if (row === undefined) {
    return yield* refused(
      "missing",
      "Run sharnessd configure with the daemon setup document first",
    );
  }
  return yield* openCredential(row.settings, settingsRecord, unlockKey).pipe(
    Effect.flatMap((value) =>
      Schema.decodeUnknown(Schema.parseJson(settings), {
        onExcessProperty: "error",
      })(Redacted.value(value)),
    ),
    Effect.mapError(() =>
      refused(
        "stored",
        "Stored daemon configuration cannot be unlocked or decoded",
      ),
    ),
  );
});

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
