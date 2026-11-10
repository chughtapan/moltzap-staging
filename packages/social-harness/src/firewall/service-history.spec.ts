/** @file A text check judges a draft after this agent's unordered sends, and an incoming message on ordered history alone. */

import { scopedLive as it } from "@effect/vitest";
import { Effect } from "effect";
import { describe, expect } from "vitest";
import type { DecisionProvider } from "./check.js";
import type { TextRule } from "./rules.js";
import { Message } from "../communication/messages.js";
import { makeIdentityFixture } from "../identity/__tests__/authentication-fixtures.js";
import {
  evidence,
  incoming,
  judgments,
  outgoing,
  policy,
  privacy,
  withFirewall,
} from "./__tests__/firewall-fixtures.js";
import { Firewall, type FirewallOptions } from "./service.js";

/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Decision kinds are the public contract under test. */

const plain = {
  ...privacy,
  id: "plain-language",
  text: "Write in plain language.",
} satisfies TextRule;
const earlierNote = "An earlier note to the group about the agenda.";

/** Options for `plain` whose provider records each request's state, then answers as `judgments` does. */
function recording(states: string[]): FirewallOptions {
  const configured = policy([plain]);
  const provider: DecisionProvider = {
    decide: (request) => {
      states.push(request.state);
      return judgments.decide(request);
    },
  };
  return configured.text === undefined
    ? configured
    : { ...configured, text: { ...configured.text, provider } };
}

describe("Firewall history with this agent's unordered sends", () => {
  it("judges a draft after this agent's earlier sends the Router has not ordered", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const states: string[] = [];
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const draft = yield* outgoing(self);
            const earlier = yield* Message.create({
              author: self,
              recipients: draft.recipients,
              content: [{ type: "text", text: earlierNote }],
            });

            const decision = yield* firewall.checkOutgoing(draft, {
              ...evidence,
              pendingOutgoing: [earlier],
            });

            expect(decision.kind).toBe("allowed");
            expect(states.join("\n")).toContain(earlierNote);
          }),
        (agentKey) => Firewall.layer(agentKey, recording(states)),
      );
    }));

  it("admits an incoming message while this agent has sends the Router has not ordered", () =>
    Effect.gen(function* () {
      const fixture = yield* makeIdentityFixture;
      const states: string[] = [];
      yield* withFirewall(
        ":memory:",
        fixture,
        (firewall, self) =>
          Effect.gen(function* () {
            const received = yield* incoming(self);
            const earlier = yield* Message.create({
              author: self,
              recipients: [received.message.author],
              content: [{ type: "text", text: earlierNote }],
            });

            const decision = yield* firewall.checkIncoming(received, {
              ...evidence,
              pendingOutgoing: [earlier],
            });

            expect(decision.kind).toBe("allowed");
            expect(states.join("\n")).not.toContain(earlierNote);
          }),
        (agentKey) => Firewall.layer(agentKey, recording(states)),
      );
    }));
});

/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after decision assertions. */
