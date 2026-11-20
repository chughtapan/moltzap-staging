/** @file Reads the settings file and the files it names into the daemon's settings. */
import { FileSystem } from "@effect/platform";
import { Config, Data, Effect, Option, Schema, Struct } from "effect";
import { dirname, resolve } from "node:path";
import { type LoadedMessagePolicy, loadMessagePolicy } from "../firewall.js";
import { AgentName } from "../messages.js";
import {
  initialNormsHaveRules,
  type InstalledNorms,
  loadNorms,
  type NormsConfigurationError,
} from "../norms/index.js";

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
