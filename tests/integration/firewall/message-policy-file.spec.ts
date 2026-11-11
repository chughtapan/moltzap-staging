/** @file The stock daemon loads a message policy file, and refuses to start with an invalid one until its reference is removed. */

import { FileSystem } from "@effect/platform";
import {
  Client,
  type ClientOptions,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { Effect, Redacted, Schema } from "effect";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  type Daemon,
  sharedDaemon,
  stopProcess,
} from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import { arrayContaining, objectContaining } from "../test-matchers.js";
import { markedPolicy, writeMessagePolicy } from "./policy-fixture.js";

/**
 * A 2025-11-25 client: the SDK pins only 2026-07-28 and later, so this one
 * runs the `initialize` handshake offering and accepting only 2025-11-25.
 */
const OLDER_CLIENT: ClientOptions = {
  versionNegotiation: { mode: "legacy" },
  supportedProtocolVersions: ["2025-11-25"],
};

const ownerPrivacy = {
  id: "owner-privacy",
  applies: "outgoing",
  text: "Keep the owner's private information confidential.",
} as const;

const fixedRulesSchema = Schema.Struct({
  fixedRules: Schema.Array(Schema.Struct({ id: Schema.String })),
});

const olderMcpClient = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  yield* daemon.configure(
    yield* writeMessagePolicy(
      daemon.directory,
      markedPolicy({ fixedRules: [ownerPrivacy] }),
    ),
  );
  yield* daemon.start();
  const client = yield* connect(daemon, OLDER_CLIENT);
  const catalog = yield* Effect.tryPromise(() => client.listTools());
  expect(catalog.tools).toEqual(
    arrayContaining([objectContaining({ name: "read_policy" })]),
  );

  const policy = yield* call(client, "read_policy").pipe(
    Effect.flatMap(Schema.decodeUnknown(fixedRulesSchema)),
  );
  expect(policy.fixedRules).toEqual(
    arrayContaining([objectContaining({ id: ownerPrivacy.id })]),
  );
}).pipe(Effect.scoped);

/** What the daemon prints when it cannot use its message policy file. */
const INVALID_POLICY =
  "The configured message policy or provider credential is invalid";

const invalidDocument = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  const original = yield* daemon.start();
  const { agent } = yield* daemon.signInAs("carol");
  yield* stopProcess(original);
  const fs = yield* FileSystem.FileSystem;
  const messagePolicyFile = join(daemon.directory, "invalid-policy.json");
  yield* fs.writeFileString(
    messagePolicyFile,
    JSON.stringify({ ...markedPolicy({}), mode: "off" }),
  );
  yield* daemon.configure({ messagePolicyFile });
  const failure = yield* daemon.startupFailure();
  expect(failure.exitCode).not.toBe(0);
  expect(failure.logs).toContain(INVALID_POLICY);
  yield* daemon.configure({ messagePolicyFile: undefined });
  yield* daemon.start();
  expect((yield* daemon.signInAs("carol")).agent).toEqual(agent);
});

describe("stock sharnessd with a message-policy file", () => {
  it(
    "lists tools and reads policy with an MCP 2025-11-25 client",
    processTrace(olderMcpClient),
    60_000,
  );

  it(
    "rejects an invalid runtime policy file and recovers after its reference is removed",
    processTrace(invalidDocument),
    60_000,
  );
});

/** Connects as the owner `alice` with a client that negotiates as `negotiation` says. */
function connect(daemon: Daemon, negotiation: ClientOptions) {
  return Effect.acquireRelease(
    Effect.gen(function* () {
      const bearer = yield* daemon.bearer("alice");
      const client = new Client(
        { name: "message-policy-file", version: "1" },
        { capabilities: {}, ...negotiation },
      );
      yield* Effect.tryPromise(() =>
        client.connect(
          new StreamableHTTPClientTransport(daemon.endpoint, {
            requestInit: {
              headers: {
                Authorization: `Bearer ${Redacted.value(bearer)}`,
              },
              redirect: "error",
            },
          }),
        ),
      );
      return client;
    }),
    (client) => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
  );
}

function call(
  client: Client,
  name: string,
  args: Readonly<Record<string, unknown>> = {},
) {
  return Effect.tryPromise(() =>
    client.callTool({ name, arguments: args }),
  ).pipe(
    Effect.filterOrFail(
      (result) => result.isError !== true,
      (result) => result,
    ),
    Effect.map((result) => result.structuredContent),
  );
}
