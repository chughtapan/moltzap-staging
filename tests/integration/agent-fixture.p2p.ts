/** @file What only the suites of a P2P daemon use: its settings and the address of a group. */

import type { SignedIn } from "./daemon/daemon-fixture.js";

/** The fixed-member group address of `members`. */
export const groupAddress = (
  members: ReadonlyArray<Pick<SignedIn, "agent">>,
): string => `group:${members.map(({ agent }) => agent.agentName).join(",")}`;

/** The settings of a daemon in P2P mode, which names no server agent. */
export const P2P = { serverAgent: undefined };
