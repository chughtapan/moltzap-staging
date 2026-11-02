/**
 * @file A run's agents register at once, as a cohort starts them, and every
 * one stays a registered agent the Router delivers between: each sends one
 * message to the next around a ring and receives one. The network is the
 * PGlite server, Registry and Router a run network hosts, the Registry on the
 * single PostgreSQL connection configured by the local fixture.
 */
import {
  type Endpoint,
  type Network,
  startNetwork,
} from "@moltzap/integration-tests/process-network";
import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { startDaemonContainer } from "./daemon-container.ts";

/**
 * Enough agents registering at once that a Registry on more than one PGlite
 * connection loses some of their cards.
 */
const AGENTS = [...Array(16).keys()].map((index) => `agent-${String(index)}`);
const TEXT = "around the ring";

let network: Network;
let endpoints: readonly Endpoint[];

before(async () => {
  network = await startNetwork(startDaemonContainer);
  endpoints = await Promise.all(AGENTS.map((name) => network.endpoint(name)));
});

after(async () => {
  await network.close();
});

test("every agent of a cohort that registers at once sends and receives through the Router", async () => {
  const received = endpoints.map((endpoint) => endpoint.next(1));
  await Promise.all(
    endpoints.map((endpoint, index) =>
      endpoint.send({
        to: endpoints[(index + 1) % endpoints.length]?.address,
        text: TEXT,
      }),
    ),
  );
  const predecessors = [...endpoints.slice(-1), ...endpoints.slice(0, -1)];
  assert.partialDeepStrictEqual(
    await Promise.all(received),
    predecessors.map((sender) => [
      {
        kind: "multicast",
        message: {
          sender: sender.agent,
          content: [{ type: "text", text: TEXT }],
        },
      },
    ]),
  );
});
