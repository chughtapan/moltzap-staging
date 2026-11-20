#!/usr/bin/env node
/**
 * @file The `sharnessd` executable: the settings file, OAuth admission, the
 * HTTP listener and start-up around the daemon `server.ts` is. `--help`
 * prints the usage, no argument runs the daemon until it stops, and any other
 * arguments print the usage to standard error. A startup failure prints one
 * line. Every outcome but the usage and a stopped daemon exits
 * unsuccessfully. Importing the file starts nothing.
 */
import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import {
  FileSystem,
  HttpApp,
  HttpBody,
  HttpServerRequest,
  HttpServerResponse,
} from "@effect/platform";
import {
  NodeFileSystem,
  NodeHttpServer,
  NodeRuntime,
} from "@effect/platform-node";
import { discoverAuthorizationServerMetadata } from "@modelcontextprotocol/client";
import {
  getOAuthProtectedResourceMetadataUrl,
  hostHeaderValidationResponse,
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
  Match,
  Option,
  Schema,
  type Scope,
  Stream,
  Struct,
} from "effect";
import { createRemoteJWKSet, jwtVerify } from "jose";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer accepts the native server constructor.
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
import {
  type Daemon,
  databaseUrl,
  makeMcpHandler,
  openDaemon,
  packageVersion,
} from "./server.js";

/**
 * Messages identify the failed operation without disclosing documents or credentials.
 */
class DaemonConfigurationError extends Data.TaggedError(
  "DaemonConfigurationError",
)<{
  readonly message: string;
}> {}

/** A startup refusal whose message names the setting to change. */
const refused = (message: string) => new DaemonConfigurationError({ message });

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

/** The OAuth resource the daemon is, and the issuer whose tokens it admits. */
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
 * What the daemon holds after reading the settings file and the files it
 * names: the message policy, the provider credential and the norms
 * themselves, in place of their paths.
 */
type DaemonSettings = Omit<
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
    Effect.flatMap(
      Schema.decodeUnknown(Schema.parseJson(settingsFile), {
        onExcessProperty: "error",
      }),
    ),
    Effect.mapError(() =>
      refused("The daemon settings file is unreadable or invalid"),
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
  const settings: DaemonSettings = {
    ...Struct.omit(file, "messagePolicyFile", "normsDirectory"),
    messagePolicy: Option.getOrUndefined(messagePolicy),
    ...(norms === undefined ? {} : { norms }),
  };
  return settings;
});

/** The issuer's metadata could not be read, names another issuer or names no key set. */
class AuthenticationRefused extends Data.TaggedError("AuthenticationRefused") {}

/** Returns after listening. Scope closure disconnects streaming requests before the listener closes. */
export const serveMcp = Effect.fn("Mcp.serve")(function* (
  port: number,
  app: HttpApp.Default<never, Scope.Scope>,
  host: string,
) {
  const listener = yield* Effect.sync(createServer);
  const http = yield* NodeHttpServer.make(() => listener, { host, port });
  yield* Effect.addFinalizer(() =>
    Effect.sync(() => {
      listener.closeAllConnections();
    }),
  );
  yield* http.serve(app);
});

/**
 * The app the listener serves. The MCP SDK validates the Host and Origin
 * headers against the resource's hostname, serves the OAuth discovery
 * documents, and checks the bearer token's scope and expiry; a refusal is the
 * SDK's response. Any path but `/mcp` answers 404. A token is a JWT the issuer
 * signed for the resource with a subject, verified against the key set the
 * issuer's metadata names. The daemon's MCP handler answers an admitted
 * request as the agent of the token's subject, on a stream that ends when the
 * token expires.
 */
export const makeMcpHttpApp = Effect.fn("makeMcpHttpApp")(function* (
  { issuer, resource, scope }: DaemonSettings["authentication"],
  daemon: Daemon,
) {
  const answerAs = yield* makeMcpHandler(daemon);
  const metadata = yield* Effect.tryPromise({
    try: () => discoverAuthorizationServerMetadata(issuer),
    catch: () => new AuthenticationRefused(),
  });
  if (typeof metadata?.jwks_uri !== "string") {
    return yield* new AuthenticationRefused();
  }
  const keys = createRemoteJWKSet(new URL(metadata.jwks_uri));
  const hostnames = [resource.hostname];
  const discovery = {
    oauthMetadata: metadata,
    resourceServerUrl: resource,
    scopesSupported: [scope],
  };
  const bearer = requireBearerAuth({
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
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    const web = new Request(new URL(request.url, resource), {
      method: request.method,
      headers: request.headers,
    });
    const answered =
      hostHeaderValidationResponse(web, hostnames) ??
      originValidationResponse(web, hostnames) ??
      oauthMetadataResponse(web, discovery);
    if (answered !== undefined) {
      return HttpServerResponse.fromWeb(answered);
    }
    if (new URL(web.url).pathname !== "/mcp") {
      return HttpServerResponse.empty({ status: 404 });
    }
    // eslint-disable-next-line agent-code-guard/effect-promise -- The SDK's gate resolves to its refusal and does not reject.
    const authInfo = yield* Effect.promise(() => bearer(web));
    if (authInfo instanceof Response) {
      return HttpServerResponse.fromWeb(authInfo);
    }
    const response = yield* HttpApp.fromWebHandler(
      yield* answerAs(authInfo),
    ).pipe(Effect.interruptible, Effect.orDie);
    return yield* limitResponseStream(
      response,
      Stream.interruptAfter(
        Math.max(0, (authInfo.expiresAt ?? 0) * 1000 - Date.now()),
      ),
    );
  });
});

/** Ends a streamed response body when `limit` says so. Other bodies are returned as they are. */
function limitResponseStream(
  response: HttpServerResponse.HttpServerResponse,
  limit: <A, E>(body: Stream.Stream<A, E>) => Stream.Stream<A, E>,
) {
  return Match.value(response.body).pipe(
    Match.tag("Stream", (body) =>
      HttpServerResponse.setBody(
        response,
        HttpBody.stream(limit(body.stream), body.contentType),
      ),
    ),
    Match.orElse(() => response),
  );
}

/** What `--help` prints: the variables the daemon reads at every start. */
const HELP_MESSAGE = [
  "Usage: sharnessd [--help]",
  "",
  "SHARNESS_CONFIG_FILE: JSON daemon settings file",
  "SHARNESS_DATABASE_URL: PostgreSQL connection URL",
  "SHARNESS_DECISION_PROVIDER_KEY: API key of the message policy's decision provider",
  "Standard OTEL_* variables configure telemetry.",
].join("\n");

/** Printed for a startup failure that names no setting of its own. */
const UNEXPLAINED_FAILURE =
  "The daemon could not start. Check SHARNESS_CONFIG_FILE and SHARNESS_DATABASE_URL; run sharnessd --help for what each must hold.";

/** Prints one line to standard error and makes the daemon exit unsuccessfully. */
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

/**
 * Runs the daemon until it is interrupted: installs the server agent's
 * firewall and answers each authenticated request from its owner's agent.
 * Fails if it cannot start.
 */
const runDaemon = Effect.gen(function* () {
  const url = yield* databaseUrl;
  const settings = yield* loadDaemonSettings();
  const { authentication, norms } = settings;
  const daemon = yield* openDaemon(url, {
    owner: {
      issuer: authentication.issuer,
      subject: settings.serverAgent.subject,
    },
    name: settings.serverAgent.name,
    firewall: (me) =>
      makeFirewall({
        ...(settings.messagePolicy === undefined
          ? {}
          : { source: settings.messagePolicy }),
        me,
        historyLimit: settings.maximumHistoryRecords ?? 256,
        notice: `Your message was not accepted: it breaks a rule this agent applies to incoming messages.${norms === undefined ? "" : " Call read_norms to see which messages it accepts."}`,
        ...(norms === undefined
          ? {}
          : { structureCheck: structureCheck(norms) }),
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
  }).pipe(
    Effect.catchTag("OwnershipError", () =>
      refused(
        "This database's server agent has another name than serverAgent.name in the settings, or another agent has that name. Set serverAgent.name to the name the database was first started with, or prepare a new database.",
      ),
    ),
  );
  const app = yield* makeMcpHttpApp(authentication, daemon).pipe(
    Effect.catchTag("AuthenticationRefused", () =>
      refused(
        `The OAuth issuer ${authentication.issuer} did not answer with its configuration. Check authentication.issuer and that the daemon can reach it.`,
      ),
    ),
  );
  yield* serveMcp(settings.port, app, settings.host).pipe(
    Effect.catchTag("ServeError", () =>
      refused(
        `The daemon could not listen on ${settings.host}:${settings.port}. Stop the process that uses the port, or set another port.`,
      ),
    ),
  );
  yield* Console.log(
    `sharnessd listening on http://${settings.host}:${settings.port}/mcp (package ${packageVersion})`,
  );
  return yield* Effect.never;
}).pipe(
  Effect.scoped,
  Effect.withSpan("runDaemon"),
  Effect.provide(Layer.merge(NodeFileSystem.layer, spanExport)),
);

if (import.meta.main) {
  const args = process.argv.slice(2);
  NodeRuntime.runMain(
    Effect.suspend(() => {
      if (args.length === 1 && args[0] === "--help") {
        return Console.log(HELP_MESSAGE);
      }
      if (args.length === 0) {
        return runDaemon;
      }
      return exitUnsuccessfully(HELP_MESSAGE);
    }).pipe(
      Effect.catchTags({
        DaemonConfigurationError: ({ message }) => exitUnsuccessfully(message),
        DatabaseUrlMissingError: ({ message }) => exitUnsuccessfully(message),
        DatabasePreparationError: ({ message }) => exitUnsuccessfully(message),
      }),
      Effect.catchAll(() => exitUnsuccessfully(UNEXPLAINED_FAILURE)),
    ),
  );
}
