/** @file Installed contracts belong to each peer, and clients read the server's current contracts. */
import { callTool } from "@social-harness/social-harness";
import { Effect } from "effect";
import { expect, it } from "vitest";
import { send } from "../agent-fixture.js";
import { sharedDaemon } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";
import {
  exchangeDocument,
  exchangeName,
  writeNorms,
} from "./policy-fixture.js";

const policyTool = "update_policy";
const timeoutMilliseconds = 120_000;
function normsDaemon(settings: Readonly<Record<string, unknown>> = {}) {
  return Effect.gen(function* () {
    const daemon = yield* sharedDaemon({ firewall: true, settings });
    yield* daemon.configure(
      yield* writeNorms(daemon.directory, [exchangeDocument]),
    );
    yield* daemon.start();
    return daemon;
  });
}

it(
  "lets each peer edit its own contracts without changing another peer",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* normsDaemon();
      const alice = yield* daemon.signInAs("alice");
      const bob = yield* daemon.signInAs("bob");
      yield* callTool(alice.connection.client, {
        name: policyTool,
        arguments: { remove: [{ norm: exchangeName }] },
      });
      const aliceNorms = yield* callTool(alice.connection.client, {
        name: "read_norms",
        arguments: {},
      });
      const bobNorms = yield* callTool(bob.connection.client, {
        name: "read_norms",
        arguments: {},
      });
      expect(aliceNorms.norms).toEqual([]);
      expect(bobNorms.norms.map((norm) => norm.document)).toEqual([
        exchangeDocument,
      ]);
      yield* send(alice, { to: bob.address, text: "An ordinary message" });
      expect(
        yield* send(bob, {
          to: alice.address,
          text: "An ordinary message",
        }).pipe(Effect.flip),
      ).toMatchObject({ refusal: "rule-violation" });
    }),
  ),
  timeoutMilliseconds,
);

it(
  "gives clients the server's current contracts while keeping edits with the server",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* normsDaemon({ serverAgentName: "contract-server" });
      const server = yield* daemon.signIn(daemon.serverAgentBearer);
      const client = yield* daemon.signInAs("client");
      const clientNorms = yield* callTool(client.connection.client, {
        name: "read_norms",
        arguments: {},
      });
      expect(clientNorms.norms.map((norm) => norm.document)).toEqual([
        exchangeDocument,
      ]);
      const clientTools = yield* Effect.tryPromise(() =>
        client.connection.client.listTools(),
      );
      expect(clientTools.tools.map((tool) => tool.name)).not.toContain(
        policyTool,
      );
      yield* callTool(server.connection.client, {
        name: policyTool,
        arguments: { remove: [{ norm: exchangeName }] },
      });
      expect(
        (yield* callTool(client.connection.client, {
          name: "read_norms",
          arguments: {},
        })).norms,
      ).toEqual([]);
    }),
  ),
  timeoutMilliseconds,
);
