/** @file A configured text policy loads through the stock daemon and holds sends when its provider is unavailable. */

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
  acquireDaemonManagementClient,
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  awaitDaemonStartupFailure,
  configureDaemonFixture,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  makeRegistrationRequest,
  stopProcess,
} from "../daemon/daemon-fixture.js";
import { unavailablePolicyRemedy } from "../expected-contracts.js";
import { processTrace } from "../scenario-fixture.js";
import { arrayContaining, objectContaining } from "../test-matchers.js";

const FORMAT = "org.social-harness/message-policy/v2";

const CURRENT_CLIENT: ClientOptions = {
  versionNegotiation: { mode: { pin: "2026-07-28" } },
};
/**
 * A 2025-11-25 client: the SDK pins only 2026-07-28 and later, so this one
 * runs the `initialize` handshake offering and accepting only 2025-11-25.
 */
const OLDER_CLIENT: ClientOptions = {
  versionNegotiation: { mode: "legacy" },
  supportedProtocolVersions: ["2025-11-25"],
};

const acquirePolicyFileScenario = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const bob = yield* makeDaemonProcessFixture(infrastructure, "bob");
  yield* acquireDaemonProcess(bob);
  const bobManagement = yield* acquireDaemonManagementClient(bob);
  yield* bobManagement.register(makeRegistrationRequest(bob));
  const stock = yield* makeDaemonProcessFixture(infrastructure, "alice");
  const fs = yield* FileSystem.FileSystem;
  const directory = join(infrastructure.temporaryDirectory, "alice-policy");
  yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
  const policyFile = join(directory, "message-policy.json");
  yield* fs.writeFileString(
    policyFile,
    JSON.stringify({
      format: FORMAT,
      mode: "enforcement",
      decisionProvider: { kind: "unavailable" },
      text: {
        fixedRules: [
          {
            id: "owner-privacy",
            applies: "outgoing",
            text: "Keep the owner's private information confidential.",
          },
        ],
      },
    }),
    { mode: 0o600 },
  );
  const alice = yield* configureDaemonFixture(stock, {
    messagePolicyFile: policyFile,
  });
  yield* acquireDaemonProcess(alice);
  const management = yield* acquireDaemonManagementClient(alice);
  yield* management.register(makeRegistrationRequest(alice));
  const client = yield* connect(alice);
  return { client, fixture: alice };
});

const fixedRulesSchema = Schema.Struct({
  fixedRules: Schema.Array(Schema.Struct({ id: Schema.String })),
});

const unavailableTextPolicy = Effect.gen(function* () {
  const scenario = yield* acquirePolicyFileScenario;
  yield* expectDocumentRulesPublished(scenario.client);
  yield* expectUnavailableSendHeld(scenario.client);
  const feed = yield* call(scenario.client, "firewall_decisions");
  expect(feed).toMatchObject({
    decisions: [
      {
        direction: "outgoing",
        outcome: "hold",
        disposition: "withheld",
        reason: "check-unavailable",
        decidedBy: [{ ruleId: "owner-privacy", from: "fixed" }],
      },
    ],
  });
});

const olderMcpClient = Effect.gen(function* () {
  const scenario = yield* acquirePolicyFileScenario;
  const client = yield* connect(scenario.fixture, OLDER_CLIENT);
  const catalog = yield* Effect.tryPromise(() => client.listTools());
  expect(catalog.tools).toEqual(
    arrayContaining([objectContaining({ name: "policy_read" })]),
  );

  const policy = yield* call(client, "policy_read").pipe(
    Effect.flatMap(Schema.decodeUnknown(fixedRulesSchema)),
  );
  expect(policy.fixedRules).toEqual(
    arrayContaining([objectContaining({ id: "owner-privacy" })]),
  );
});

const invalidDocument = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const stock = yield* makeDaemonProcessFixture(infrastructure, "carol");
  const original = yield* acquireDaemonProcess(stock);
  const before = yield* acquireDaemonManagementClient(stock);
  const status = yield* before.status();
  yield* stopProcess(original);
  const fs = yield* FileSystem.FileSystem;
  const policyFile = join(infrastructure.temporaryDirectory, "carol.json");
  yield* fs.writeFileString(
    policyFile,
    JSON.stringify({ format: FORMAT, mode: "off" }),
    {
      mode: 0o600,
    },
  );
  const failure = yield* configureDaemonFixture(stock, {
    messagePolicyFile: policyFile,
  }).pipe(Effect.flip);
  expect(failure).toMatchObject({ reason: "policy" });
  expect((yield* awaitDaemonStartupFailure(stock)).exitCode).not.toBe(0);
  yield* configureDaemonFixture(stock, { messagePolicyFile: undefined });
  yield* acquireDaemonProcess(stock);
  const management = yield* acquireDaemonManagementClient(stock);
  expect(yield* management.status()).toEqual(status);
});

describe("stock sharnessd with a message-policy file", () => {
  it(
    "lists tools and reads policy with an MCP 2025-11-25 client",
    processTrace(olderMcpClient.pipe(Effect.scoped)),
    60_000,
  );

  it(
    "holds a send when the configured decision provider is unavailable",
    processTrace(unavailableTextPolicy.pipe(Effect.scoped)),
    60_000,
  );

  it(
    "rejects an invalid runtime policy file and recovers after its reference is removed",
    processTrace(invalidDocument.pipe(Effect.scoped)),
    60_000,
  );
});

function expectDocumentRulesPublished(client: Client) {
  return Effect.gen(function* () {
    const policy = yield* call(client, "policy_read").pipe(
      Effect.flatMap(Schema.decodeUnknown(fixedRulesSchema)),
    );
    expect(policy.fixedRules).toEqual(
      arrayContaining([objectContaining({ id: "owner-privacy" })]),
    );
  });
}

function expectUnavailableSendHeld(client: Client) {
  return Effect.gen(function* () {
    const refused = yield* call(client, "send_message", {
      input: { to: "agent:bob", text: "Can we meet on Tuesday?" },
    }).pipe(Effect.flip);
    expect(JSON.stringify(refused)).toContain(unavailablePolicyRemedy);
  });
}

function connect(fixture: DaemonProcessFixture, negotiation = CURRENT_CLIENT) {
  return Effect.acquireRelease(
    Effect.gen(function* () {
      const client = new Client(
        { name: "message-policy-file", version: "1" },
        { capabilities: {}, ...negotiation },
      );
      yield* Effect.tryPromise(() =>
        client.connect(
          new StreamableHTTPClientTransport(fixture.endpoint, {
            requestInit: {
              headers: {
                Authorization: `Bearer ${Redacted.value(fixture.mcpCredential)}`,
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
