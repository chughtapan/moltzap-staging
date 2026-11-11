/** @file The order in which a daemon lists the members of a question. */

import type { Participant } from "../agent-fixture.js";

/**
 * The participants ordered by the names their daemon gave them. A question
 * lists its recipients, and its result their outcomes, in that order.
 */
export const inNameOrder = (
  participants: readonly Participant[],
): readonly Participant[] =>
  participants.toSorted((left, right) =>
    left.agent.agentName < right.agent.agentName ? -1 : 1,
  );
