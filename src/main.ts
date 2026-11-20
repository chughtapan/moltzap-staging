#!/usr/bin/env node
/**
 * @file The `sharnessd` executable: it reads the settings file, builds the
 * server agent's firewall, and serves the MCP server of `server.ts` over
 * HTTP behind the MCP SDK's bearer authentication. `--help` prints the
 * usage, no argument serves until the process is stopped, and any other
 * arguments print the usage to standard error. A startup failure prints one
 * line and exits unsuccessfully.
 */
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { FileSystem } from "@effect/platform";
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node";
import { discoverAuthorizationServerMetadata } from "@modelcontextprotocol/client";
import { toNodeHandler } from "@modelcontextprotocol/node";
import {
  createMcpHandler,
  getOAuthProtectedResourceMetadataUrl,
  hostHeaderValidationResponse,
  type McpServerFactory,
  OAuthError,
  OAuthErrorCode,
  oauthMetadataResponse,
  originValidationResponse,
  requireBearerAuth,
} from "@modelcontextprotocol/server";
import { diag, DiagConsoleLogger, DiagLogLevel } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import {
  Config,
  Console,
  Data,
  Effect,
  Layer,
  Option,
  Schema,
  Struct,
} from "effect";
import { createRemoteJWKSet, jwtVerify } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- The MCP SDK's Node handler serves on a node:http server.
import { createServer } from "node:http";
import { dirname, resolve } from "node:path";
import {
  type LoadedMessagePolicy,
  loadMessagePolicy,
  makeFirewall,
} from "./firewall.js";
import { AgentName } from "./messages.js";
import {
  initialNormsHaveRules,
  type InstalledNorms,
  loadNorms,
  type NormsConfigurationError,
  structureCheck,
} from "./norms/index.js";
import { databaseUrl, packageVersion, serverFactory } from "./server.js";

/**
 * Messages identify the failed operation without disclosing documents or credentials.
 */
class SettingsError extends Data.TaggedError("SettingsError")<{
  readonly message: string;
}> {}

/** A startup refusal whose message names the setting to change. */
const refused = (message: string) => new SettingsError({ message });

/**
 * The URL `value` names when it parses, uses one of `protocols` (each with its
 * colon) and carries no userinfo and no fragment; otherwise `undefined`.
 */
const plainUrl = (
  value: string | URL,
  protocols: readonly string[],
): URL | undefined => {
  if (!URL.canParse(value)) {
    return undefined;
  }
  const url = new URL(value);
  return protocols.includes(url.protocol) &&
    [url.username, url.password, url.hash].every((part) => part === "")
    ? url
    : undefined;
};

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

/** The OAuth resource the server is, and the issuer whose tokens it admits. */
const authentication = Schema.Struct({
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
const settingsFile = Schema.Struct({
  port: Schema.Int.pipe(Schema.between(1, 65535)),
  host: Schema.NonEmptyString,
  authentication,
  serverAgent: Schema.Struct({
    name: AgentName,
    subject: Schema.NonEmptyString,
  }),
  messagePolicyFile: Schema.optional(Schema.NonEmptyString),
  normsDirectory: Schema.optional(Schema.NonEmptyString),
  maximumHistoryRecords: Schema.optional(
    Schema.Int.pipe(Schema.between(1, 1024)),
  ),
});

/**
 * What the process holds after reading the settings file and the files it
 * names: the message policy, the provider credential and the norms
 * themselves, in place of their paths.
 */
type Settings = Omit<
  typeof settingsFile.Type,
  "messagePolicyFile" | "normsDirectory"
> & {
  readonly messagePolicy?: LoadedMessagePolicy;
  readonly norms?: InstalledNorms;
};

/**
 * The paths the settings file names resolve against its own directory,
 * whatever the working directory.
 */
const loadSettings = Effect.fn("loadSettings")(function* () {
  const settingsPath = yield* Config.string("SHARNESS_CONFIG_FILE").pipe(
    Effect.mapError(() =>
      refused("Set SHARNESS_CONFIG_FILE to the server settings file"),
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
      refused("The server settings file is unreadable or invalid"),
    ),
  );
  const settingsDirectory = dirname(resolve(settingsPath));
  const norms =
    file.normsDirectory === undefined
      ? undefined
      : yield* loadNorms(resolve(settingsDirectory, file.normsDirectory)).pipe(
          Effect.mapError((error: NormsConfigurationError) =>
            refused(
              `The configured norms directory is invalid: ${error.reason}`,
            ),
          ),
        );
  if (norms !== undefined && file.messagePolicyFile === undefined) {
    return yield* refused(
      "The configured norms need a messagePolicyFile: the message policy checks them",
    );
  }
  const messagePolicy = yield* loadMessagePolicy(
    file.messagePolicyFile === undefined
      ? undefined
      : resolve(settingsDirectory, file.messagePolicyFile),
  ).pipe(Effect.mapError((error) => refused(error.message)));
  if (
    norms !== undefined &&
    initialNormsHaveRules(norms) &&
    Option.getOrUndefined(messagePolicy)?.provider === undefined
  ) {
    return yield* refused(
      "The configured norms have English rules, which need a messagePolicyFile with a decisionProvider to check them",
    );
  }
  const settings: Settings = {
    ...Struct.omit(file, "messagePolicyFile", "normsDirectory"),
    messagePolicy: Option.getOrUndefined(messagePolicy),
    ...(norms === undefined ? {} : { norms }),
  };
  return settings;
});

/**
 * Listens on the settings' host and port and answers each request as the MCP
 * SDK's example of a bearer-authenticated server does. The SDK validates the
 * Host and Origin headers against the resource's hostname, serves the OAuth
 * discovery documents, and checks the bearer token's scope and expiry; a
 * refusal is the SDK's response. Any path but `/mcp` answers 404. A token is
 * a JWT the issuer signed for the resource with a subject, verified against
 * the key set the issuer's metadata names. Returns once the listener accepts
 * connections.
 */
const listen = Effect.fn("listen")(function* (
  { host, port, authentication }: Settings,
  factory: McpServerFactory,
) {
  const { issuer, resource, scope } = authentication;
  const metadata = yield* Effect.tryPromise({
    try: () => discoverAuthorizationServerMetadata(issuer),
    catch: () => undefined,
  }).pipe(Effect.orElseSucceed(() => undefined));
  if (typeof metadata?.jwks_uri !== "string") {
    return yield* refused(
      `The OAuth issuer ${issuer} did not answer with its configuration. Check authentication.issuer and that the server can reach it.`,
    );
  }
  const keys = createRemoteJWKSet(new URL(metadata.jwks_uri));
  const hostnames = [resource.hostname];
  const discovery = {
    oauthMetadata: metadata,
    resourceServerUrl: resource,
    scopesSupported: [scope],
  };
  const gate = requireBearerAuth({
    requiredScopes: [scope],
    resourceMetadataUrl: getOAuthProtectedResourceMetadataUrl(resource),
    verifier: {
      // eslint-disable-next-line agent-code-guard/async-keyword -- The MCP SDK's authorization API is promise-based.
      verifyAccessToken: async (token) => {
        const verified = await jwtVerify(token, keys, {
          issuer,
          audience: resource.href,
          algorithms: ["RS256"],
        }).catch(() => undefined);
        if (!verified?.payload.sub) {
          throw new OAuthError(OAuthErrorCode.InvalidToken, "Invalid token");
        }
        const { sub, scope: granted, exp } = verified.payload;
        return {
          token,
          clientId: "",
          scopes: typeof granted === "string" ? granted.split(" ") : [],
          expiresAt: exp,
          extra: { issuer, subject: sub },
        };
      },
    },
  });
  const handler = createMcpHandler(factory);
  // eslint-disable-next-line agent-code-guard/async-keyword -- The MCP SDK's Node handler takes a promise-based fetch function.
  const fetch = async (request: Request) => {
    const answered =
      hostHeaderValidationResponse(request, hostnames) ??
      originValidationResponse(request, hostnames) ??
      oauthMetadataResponse(request, discovery);
    if (answered !== undefined) {
      return answered;
    }
    if (new URL(request.url).pathname !== "/mcp") {
      return new Response(null, { status: 404 });
    }
    const authInfo = await gate(request);
    return authInfo instanceof Response
      ? authInfo
      : await handler.fetch(request, { authInfo });
  };
  // eslint-disable-next-line @typescript-eslint/no-misused-promises -- The MCP SDK's Node handler is a request listener that returns a promise it never rejects.
  const listener = createServer(toNodeHandler({ fetch }));
  yield* Effect.async<undefined, SettingsError>((resume) => {
    listener.once("error", () => {
      resume(
        refused(
          `The server could not listen on ${host}:${port}. Stop the process that uses the port, or set another port.`,
        ),
      );
    });
    listener.listen(port, host, () => {
      resume(Effect.succeed(undefined));
    });
  });
});

/** What `--help` prints: the variables read at every start. */
const HELP_MESSAGE = [
  "Usage: sharnessd [--help]",
  "",
  "SHARNESS_CONFIG_FILE: JSON server settings file",
  "SHARNESS_DATABASE_URL: PostgreSQL connection URL",
  "SHARNESS_DECISION_PROVIDER_KEY: API key of the message policy's decision provider",
  "Standard OTEL_* variables configure telemetry.",
].join("\n");

/** Printed for a startup failure that names no setting of its own. */
const UNEXPLAINED_FAILURE =
  "The server could not start. Check SHARNESS_CONFIG_FILE and SHARNESS_DATABASE_URL; run sharnessd --help for what each must hold.";

/** Prints one line to standard error and makes the process exit unsuccessfully. */
const exitUnsuccessfully = (line: string) =>
  Console.error(line).pipe(
    Effect.andThen(
      Effect.sync(() => {
        process.exitCode = 1;
      }),
    ),
  );

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
 * Exports spans as OTLP/HTTP protobuf when `OTEL_EXPORTER_OTLP_ENDPOINT` is
 * set, under the service name `OTEL_SERVICE_NAME` gives, or `sharnessd`.
 * OpenTelemetry reads the endpoint and its remaining exporter, batch and
 * resource settings from the standard variables, and warns of one it cannot use.
 */
const spanExport = Layer.unwrapEffect(
  Effect.gen(function* () {
    const endpoint = yield* Config.option(
      Config.nonEmptyString("OTEL_EXPORTER_OTLP_ENDPOINT"),
    );
    if (Option.isNone(endpoint)) {
      return Layer.empty;
    }
    const serviceName = yield* Config.nonEmptyString("OTEL_SERVICE_NAME").pipe(
      Config.withDefault("sharnessd"),
    );
    return NodeSdk.layer(() => ({
      resource: { serviceName, serviceVersion: packageVersion },
      spanProcessor: new BatchSpanProcessor(new OTLPTraceExporter()),
    })).pipe(Layer.discard, Layer.provide(exportDiagnostics));
  }).pipe(Effect.orDie),
);

/** Serves until the process is stopped. Fails if it cannot start. */
const serve = Effect.gen(function* () {
  const url = yield* databaseUrl;
  const settings = yield* loadSettings();
  const { authentication, norms } = settings;
  const factory = yield* serverFactory(url, {
    issuer: authentication.issuer,
    subject: settings.serverAgent.subject,
    name: settings.serverAgent.name,
    firewall: makeFirewall({
      ...(settings.messagePolicy === undefined
        ? {}
        : { source: settings.messagePolicy }),
      me: settings.serverAgent.name,
      historyLimit: settings.maximumHistoryRecords ?? 256,
      notice: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${norms === undefined ? "" : " Call read_norms to see which messages it accepts."}`,
      ...(norms === undefined ? {} : { structureCheck: structureCheck(norms) }),
    }),
    ...(norms === undefined
      ? {}
      : {
          norms: norms.map((norm) => ({
            name: norm.name,
            description: norm.description,
            document: norm.source,
          })),
        }),
  });
  yield* listen(settings, factory);
  yield* Console.log(
    `sharnessd listening on http://${settings.host}:${settings.port}/mcp (package ${packageVersion})`,
  );
  return yield* Effect.never;
}).pipe(
  Effect.scoped,
  Effect.withSpan("serve"),
  Effect.provide(Layer.merge(NodeFileSystem.layer, spanExport)),
);

const args = process.argv.slice(2);
NodeRuntime.runMain(
  Effect.suspend(() => {
    if (args.length === 1 && args[0] === "--help") {
      return Console.log(HELP_MESSAGE);
    }
    if (args.length === 0) {
      return serve;
    }
    return exitUnsuccessfully(HELP_MESSAGE);
  }).pipe(
    Effect.catchTags({
      SettingsError: ({ message }) => exitUnsuccessfully(message),
      DatabaseUrlMissingError: ({ message }) => exitUnsuccessfully(message),
      DatabasePreparationError: ({ message }) => exitUnsuccessfully(message),
    }),
    Effect.catchAll(() => exitUnsuccessfully(UNEXPLAINED_FAILURE)),
  ),
);
