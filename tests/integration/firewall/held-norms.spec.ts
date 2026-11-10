/**
 * @file One shared daemon serves OAuth agents and the norms they hold.
 *
 * In P2P mode two agents hold different norms.
 * Each agent starts with the norms directory's initial norms and changes its
 * own through its MCP context, and its firewall applies the norms it holds:
 * an agent that stops holding `chat` answers plain text with `not-understood`
 * and refuses its own, while the other agent on the same daemon reads it.
 *
 * In client/server mode only the server agent holds norms. A client reads the
 * server agent's norms, sends without a check of its own, and is answered when
 * the server agent does not accept its message.
 */

import type { Client } from "@modelcontextprotocol/client";
import { FileSystem } from "@effect/platform";
import {
  AgentAddress,
  AgentIdentityCard,
  MAXIMUM_WAIT_SECONDS,
} from "@social-harness/social-harness";
import { Effect, Schema } from "effect";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { callTool as call } from "../../../plugins/shared/mcp-client.js";
import {
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  makeDaemonProcessFixture,
  reservePort,
} from "../daemon/daemon-fixture.js";
import {
  connect,
  issuerFixture,
  requiredScope,
} from "../daemon/oauth-fixture.js";
import { processTrace } from "../scenario-fixture.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Norm names, rule ids and the not-understood act are the norms contract under test. */

const shipped = fileURLToPath(
  new URL("../../../packages/social-harness/config/norms/", import.meta.url),
);

/** The test starts PGlite, the Registry, the Router and one shared daemon. */
const HELD_NORMS_TEST_TIMEOUT_MS = 180_000;

/** A norms directory whose initial norms are `chat` and `office-hours`. */
function normsDirectory(root: string) {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = join(root, "held-norms");
    yield* fs.makeDirectory(directory, { recursive: true });
    yield* Effect.forEach(
      ["examples/chat.lisp", "examples/office-hours.lisp", "rules.json"],
      (file) =>
        fs.copyFile(join(shipped, file), join(directory, basename(file))),
      { concurrency: 1 },
    );
    return directory;
  });
}

/** The address of `client`'s registered agent. */
function addressOf(client: Client) {
  return Effect.gen(function* () {
    const status = yield* call(client, { name: "status", arguments: {} });
    if (status.kind !== "registered") {
      return yield* Effect.dieMessage("expected a registered agent");
    }
    const card = yield* Schema.decodeUnknown(AgentIdentityCard)(
      status.identityCard,
    );
    return AgentAddress.make(`agent:${card.agentName}`);
  });
}

/**
 * The text of the first item in `client`'s inbox. One call waits for it: the
 * daemon answers as soon as the item arrives.
 */
function firstDelivery(client: Client) {
  return call(client, {
    name: "receive_messages",
    arguments: { wait: MAXIMUM_WAIT_SECONDS },
  }).pipe(
    Effect.map(({ items }) => {
      const item = items[0]?.item;
      return item !== undefined && "message" in item
        ? item.message.content
            .map((part) => (part.type === "text" ? part.text : ""))
            .join("")
        : "";
    }),
    Effect.timeout("60 seconds"),
  );
}

const heldNorms = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const issuer = yield* issuerFixture;
  const port = yield* reservePort;
  const resource = new URL(`https://127.0.0.1:${port}/mcp`);
  const fixture = yield* makeDaemonProcessFixture(
    infrastructure,
    "held-norms",
    true,
    {
      settings: {
        port,
        authentication: {
          kind: "oauth",
          resource: resource.href,
          issuer: issuer.issuer,
          scope: requiredScope,
        },
        agentName: undefined,
        normsDirectory: yield* normsDirectory(
          infrastructure.temporaryDirectory,
        ),
      },
    },
  );
  yield* acquireDaemonProcess(fixture);
  const [alice, bob] = yield* Effect.forEach(
    ["alice", "bob"],
    (subject) =>
      issuer
        .token(subject, resource.href)
        .pipe(Effect.flatMap((token) => connect(fixture.endpoint, token))),
    { concurrency: 1 },
  );
  if (alice === undefined || bob === undefined) {
    return yield* Effect.dieMessage("expected two agents");
  }
  const [aliceAddress, bobAddress] = yield* Effect.all([
    addressOf(alice),
    addressOf(bob),
  ]);

  yield* call(alice, {
    name: "update_policy",
    arguments: { remove: [{ norm: "chat" }] },
  });
  const [alicePolicy, bobPolicy] = yield* Effect.all([
    call(alice, { name: "read_policy", arguments: {} }),
    call(bob, { name: "read_policy", arguments: {} }),
  ]);
  const aliceNorms = yield* call(alice, { name: "read_norms", arguments: {} });
  yield* call(bob, {
    name: "send_message",
    arguments: { to: aliceAddress, text: "Can we meet today?" },
  });
  const answer = yield* firstDelivery(bob);
  const refusal = yield* call(alice, {
    name: "send_message",
    arguments: { to: bobAddress, text: "Yes, today." },
  }).pipe(Effect.flip);

  expect(alicePolicy.norms).toEqual(["office-hours"]);
  expect(bobPolicy.norms).toEqual(["chat", "office-hours"]);
  expect(alicePolicy.fixedRules.map((rule) => rule.id)).toContain(
    "office-hours-rule-1",
  );
  expect(aliceNorms.norms.map((norm) => norm.name)).toEqual([
    "feedback",
    "norm-agreement",
    "norm-discovery",
    "office-hours",
  ]);
  expect(answer).toMatch(
    /^\(not-understood :ontology feedback .*:reason "plain-text"/u,
  );
  expect(refusal).toMatchObject({ refusal: "rule-violation" });
});

it(
  "lets two agents of one daemon hold different norms, each applied by its own firewall",
  processTrace(heldNorms),
  HELD_NORMS_TEST_TIMEOUT_MS,
);

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore default assertion rules. */

/** The names of the tools the daemon offers `client`. */
function toolNames(client: Client) {
  return Effect.tryPromise(() => client.listTools()).pipe(
    Effect.map(({ tools }) => tools.map((tool) => tool.name)),
  );
}

const policyTool = "update_policy";
const droppedNorm = "chat";

const serverOwnsNorms = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const issuer = yield* issuerFixture;
  const port = yield* reservePort;
  const resource = new URL(`https://127.0.0.1:${port}/mcp`);
  const fixture = yield* makeDaemonProcessFixture(
    infrastructure,
    "server-norms",
    true,
    {
      settings: {
        port,
        transport: { kind: "client-server", serverAgentName: "norms-server" },
        authentication: {
          kind: "oauth",
          resource: resource.href,
          issuer: issuer.issuer,
          scope: requiredScope,
        },
        agentName: undefined,
        normsDirectory: yield* normsDirectory(
          infrastructure.temporaryDirectory,
        ),
      },
    },
  );
  const clientToken = yield* issuer.token("client", resource.href);
  yield* acquireDaemonProcess(fixture);
  const server = yield* connect(
    fixture.endpoint,
    fixture.serverAgentCredential,
  );
  const client = yield* connect(fixture.endpoint, clientToken);

  yield* call(server, {
    name: policyTool,
    arguments: { remove: [{ norm: droppedNorm }] },
  });
  const serverNorms = yield* call(server, {
    name: "read_norms",
    arguments: {},
  });
  const clientNorms = yield* Effect.tryPromise(() =>
    client.callTool({ name: "read_norms", arguments: {} }),
  );
  const sent = yield* Effect.tryPromise(() =>
    client.callTool({
      name: "send_message",
      arguments: { text: "Can we meet today?" },
    }),
  );
  const answer = yield* firstDelivery(client);

  expect(yield* toolNames(client)).not.toContain(policyTool);
  expect(serverNorms.norms.map((norm) => norm.name)).not.toContain(droppedNorm);
  expect(clientNorms.structuredContent).toEqual(serverNorms);
  expect(sent.isError).not.toBe(true);
  expect(answer).toMatch(
    /^\(not-understood :ontology feedback .*:reason "plain-text"/u,
  );
});

it(
  "in client/server mode gives a client the server agent's norms and an answer when its message is not accepted",
  processTrace(serverOwnsNorms),
  HELD_NORMS_TEST_TIMEOUT_MS,
);
