/** @file Reads the settings file and the files it names into the daemon's settings. */
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { FileSystem } from "@effect/platform";
import { diag, DiagConsoleLogger, DiagLogLevel } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { Config, Data, Effect, Layer, Option, Schema, Struct } from "effect";
import { dirname, resolve } from "node:path";
import { exactStruct, plainUrl } from "../encoding.js";
import {
  type LoadedMessagePolicy,
  loadMessagePolicy,
} from "../firewall/index.js";
import { AgentName } from "../identity.js";
import {
  initialNormsHaveRules,
  type InstalledNorms,
  loadNorms,
  type NormsConfigurationError,
} from "../norms/index.js";
import { packageVersion } from "../version.js";

/**
 * Messages identify the failed operation without disclosing documents or credentials.
 * Exported so dependent declarations can name the inferred Effect failure.
 * @public
 */
export class DaemonConfigurationError extends Data.TaggedError(
  "DaemonConfigurationError",
)<{
  readonly message: string;
}> {}

/** A startup refusal whose message names the setting to change. */
export const refused = (message: string) =>
  new DaemonConfigurationError({ message });

const httpsUrl = Schema.String.pipe(
  Schema.filter((value) => plainUrl(value, ["https:"])?.search === ""),
);

/** Plaintext discovery is limited to local issuer fixtures; hosted providers require HTTPS. */
const oauthIssuer = Schema.Union(
  httpsUrl,
  Schema.String.pipe(
    Schema.filter((value) => {
      const url = plainUrl(value, ["http:"]);
      return url?.search === "" && url.hostname === "127.0.0.1";
    }),
  ),
);

/** The OAuth resource the daemon is, and the issuer whose tokens it admits. */
const authentication = exactStruct({
  resource: httpsUrl.pipe(
    Schema.filter(
      (value) =>
        new URL(value).pathname === "/mcp" && new URL(value).href === value,
    ),
    Schema.compose(Schema.URL),
  ),
  issuer: oauthIssuer,
  scope: Schema.String.pipe(Schema.pattern(/^[\x21\x23-\x5B\x5D-\x7E]+$/u)),
});
/** The settings file as an operator writes it. */
const settingsFile = exactStruct({
  port: Schema.Int.pipe(Schema.between(1, 65535)),
  host: Schema.NonEmptyString,
  authentication,
  serverAgent: Schema.optional(
    exactStruct({ name: AgentName, subject: Schema.NonEmptyString }),
  ),
  hideGroups: Schema.optionalWith(Schema.Boolean, { default: () => false }),
  messagePolicyFile: Schema.optional(Schema.NonEmptyString),
  normsDirectory: Schema.optional(Schema.NonEmptyString),
  maximumHistoryRecords: Schema.optional(
    Schema.Int.pipe(Schema.between(1, 1024)),
  ),
});

/**
 * What the daemon holds after reading the settings file and the files it
 * names: the message policy, the provider credential and the norms
 * themselves, in place of their paths.
 */
export type DaemonSettings = Omit<
  typeof settingsFile.Type,
  "messagePolicyFile" | "normsDirectory"
> & {
  readonly messagePolicy?: LoadedMessagePolicy;
  readonly norms?: InstalledNorms;
};

/**
 * The paths the settings file names resolve against its own directory,
 * whatever the daemon's working directory.
 */
export const loadDaemonSettings = Effect.fn("loadDaemonSettings")(function* () {
  const settingsPath = yield* Config.string("SHARNESS_CONFIG_FILE").pipe(
    Effect.mapError(() =>
      refused("Set SHARNESS_CONFIG_FILE to the daemon settings file"),
    ),
  );
  const filesystem = yield* FileSystem.FileSystem;
  const file = yield* filesystem.readFileString(settingsPath).pipe(
    Effect.flatMap(Schema.decodeUnknown(Schema.parseJson(settingsFile))),
    Effect.mapError(() =>
      refused("The daemon settings file is unreadable or invalid"),
    ),
  );
  const settingsDirectory = dirname(resolve(settingsPath));
  const norms =
    file.normsDirectory === undefined
      ? undefined
      : yield* configuredNorms(resolve(settingsDirectory, file.normsDirectory));
  if (norms !== undefined && file.messagePolicyFile === undefined) {
    return yield* refused(
      "The configured norms need a messagePolicyFile: the message policy checks them",
    );
  }
  const messagePolicy = yield* loadMessagePolicy(
    file.messagePolicyFile === undefined
      ? undefined
      : resolve(settingsDirectory, file.messagePolicyFile),
  ).pipe(
    Effect.mapError(() =>
      refused(
        "The configured message policy is invalid, or it names a decisionProvider and SHARNESS_DECISION_PROVIDER_KEY does not hold its key",
      ),
    ),
  );
  if (
    norms !== undefined &&
    initialNormsHaveRules(norms) &&
    Option.getOrUndefined(messagePolicy)?.provider === undefined
  ) {
    return yield* refused(
      "The configured norms have English rules, which need a messagePolicyFile with a decisionProvider to check them",
    );
  }
  const settings: DaemonSettings = {
    ...Struct.omit(file, "messagePolicyFile", "normsDirectory"),
    messagePolicy: Option.getOrUndefined(messagePolicy),
    ...(norms === undefined ? {} : { norms }),
  };
  return settings;
});

/** The settings file's norms directory, read and checked. */
function configuredNorms(directory: string) {
  return loadNorms(directory).pipe(
    Effect.mapError((error: NormsConfigurationError) =>
      refused(`The configured norms directory is invalid: ${error.reason}`),
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
 * OpenTelemetry reads the endpoint and its remaining exporter, batch and
 * resource settings from the standard variables, and warns of one it cannot use.
 */
// eslint-disable-next-line agent-code-guard/require-span-on-exported-effect -- The exporter installs tracing; tracing its acquisition changes the daemon's exported spans.
export const spanExport = Layer.unwrapEffect(
  Effect.gen(function* () {
    const otel = yield* otelVariables.pipe(Effect.orDie);
    if (Option.isNone(otel.endpoint)) {
      return Layer.empty;
    }
    return NodeSdk.layer(() => ({
      resource: {
        serviceName: spanServiceName(otel),
        serviceVersion: packageVersion,
      },
      spanProcessor: new BatchSpanProcessor(new OTLPTraceExporter()),
    })).pipe(Layer.discard, Layer.provide(exportDiagnostics));
  }),
);
