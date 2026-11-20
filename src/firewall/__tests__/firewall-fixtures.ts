/** @file Shared fixtures for firewall service tests: an agent's firewall, messages, rules and a test provider. */

import { SqlClient } from "@effect/sql";
import { type Context, Effect, Encoding, Layer } from "effect";
import type { DecisionProvider, FirewallEvidence } from "../check.js";
import { openTestDatabase, type TestDatabase } from "../../__tests__/test.js";
import { Message } from "../../communication/index.js";
import {
  AgentCard,
  AgentId,
  AgentName,
  Identity,
  PrincipalId,
} from "../../identity.js";
import {
  decodeMessagePolicy,
  type LoadedMessagePolicy,
  type TextRule,
} from "../rules.js";
import { Firewall } from "../service.js";

/** A fixed rule the test provider always finds broken. */
export const privacy = {
  id: "privacy",
  applies: "both",
  text: "Do not reveal the owner's private records.",
  feedback: "Keep private records private.",
} satisfies TextRule;
/** Reads complete evidence with no history or gaps. */
export const evidence = () =>
  Effect.succeed<FirewallEvidence>({
    history: [],
    earlierHistoryOmitted: false,
    gaps: [],
  });
/** Finds `privacy` broken and every other rule followed, with certainty. */
export const certainProvider: DecisionProvider = {
  decide: (request) =>
    Effect.succeed({
      answers: Object.fromEntries(
        Object.entries(request.questions).map(([id, question]) => {
          const choices = Object.keys(question.criteria);
          const selected =
            question.ruleId === privacy.id ? "violation" : "compliant";
          return [
            id,
            {
              choice: selected,
              probabilities: Object.fromEntries(
                choices.map((choice) => [choice, Number(choice === selected)]),
              ),
            },
          ];
        }),
      ),
    }),
};

/**
 * Runs `use` with Alice's firewall built by `configure`, over `database` or a
 * new one, with its rows stored under `agentKey`. A property that builds a
 * database on each run bounds its runs.
 */
export function withFirewall<A, E, R>(
  storage: { readonly database?: TestDatabase; readonly agentKey?: string },
  use: (
    firewall: Context.Tag.Service<typeof Firewall>,
    self: AgentCard,
  ) => Effect.Effect<A, E, R>,
  configure: (agentKey: string) => ReturnType<typeof Firewall.layer>,
) {
  const self = peer("alice", 1);
  const identity = Layer.succeed(Identity, Identity.of({ me: self }));
  const services = configure(storage.agentKey ?? "alice").pipe(
    Layer.provideMerge(identity),
    Layer.provideMerge(
      Layer.effect(
        SqlClient.SqlClient,
        storage.database === undefined
          ? Effect.map(openTestDatabase(), ({ sql }) => sql)
          : Effect.succeed(storage.database.sql),
      ),
    ),
  );
  return Effect.scoped(
    Effect.gen(function* () {
      return yield* use(yield* Firewall, self);
    }).pipe(Effect.provide(services), Effect.withSpan("withFirewall")),
  );
}

/** A draft from `self` to a peer that reveals a private record. */
export function outgoing(self: AgentCard) {
  return Message.create({
    author: self,
    recipients: [peer("bob", 2)],
    content: [
      { type: "text", text: "The private record says the owner is away." },
    ],
  });
}

/** A peer's plain message to `self`. */
export function incoming(self: AgentCard) {
  return Message.create({
    author: peer("bob", 2),
    recipients: [self],
    content: [{ type: "text", text: "Hello" }],
  });
}

/** A peer card whose ids repeat `byte`. */
export function peer(name: string, byte: number): AgentCard {
  return AgentCard.make({
    agentId: AgentId.make(
      `agt_${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`,
    ),
    principalId: PrincipalId.make(
      `prn_${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`,
    ),
    agentName: AgentName.make(name),
  });
}

/** A message policy and the provider that judges its text rules in place of Jev. */
interface TestPolicy {
  readonly source: LoadedMessagePolicy;
  readonly provider: DecisionProvider;
}

/** A message policy with the given text rules, judged by `provider`. */
export function policy(
  rules: readonly TextRule[] = [],
  provider: DecisionProvider = certainProvider,
): TestPolicy {
  return {
    provider,
    source: {
      policy: Effect.runSync(
        decodeMessagePolicy(
          JSON.stringify({
            decisionProvider: { model: "test" },
            rules,
            settings: {
              minimumProbability: 0.5,
              timeoutMilliseconds: 5_000,
            },
          }),
        ),
      ),
    },
  };
}

/** The firewall of the agent `agentKey` names under `configured`. */
export function firewallLayer(agentKey: string, configured: TestPolicy) {
  return Firewall.layer(
    agentKey,
    configured.source,
    undefined,
    configured.provider,
  );
}
