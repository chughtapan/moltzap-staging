/** @file Shared fixtures for firewall service tests: an agent's firewall, messages, rules and a test provider. */

import { SqliteClient } from "@effect/sql-sqlite-node";
import { type Context, Effect, Encoding, Layer } from "effect";
import type { FirewallEvidence, Matcher } from "../chain.js";
import type { DecisionProvider } from "../check.js";
import type { TextRule } from "../rules.js";
import { Message } from "../../communication/index.js";
import { migrateDaemon } from "../../database/index.js";
import {
  AgentCard,
  AgentId,
  AgentName,
  Identity,
  PrincipalId,
} from "../../identity/index.js";
import { Firewall, type FirewallOptions } from "../service.js";

/** A fixed rule the test provider always finds broken. */
export const privacy = {
  id: "privacy",
  applies: "both",
  text: "Do not reveal the owner's private records.",
  feedback: { text: "Keep private records private." },
} satisfies TextRule;
/** Complete evidence with no history or gaps. */
export const evidence: FirewallEvidence = {
  history: [],
  earlierHistoryOmitted: false,
  gaps: [],
};
/** The raw provider text, which no decision may expose. */
export const privateProviderResponse = "private provider explanation";
/** Finds `privacy` broken and every other rule followed, with certainty. */
const judgments: DecisionProvider = {
  decide: (request) =>
    Effect.succeed({
      provider: "test",
      model: "test",
      raw: privateProviderResponse,
      usage: {},
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

/** Runs `use` with Alice's firewall built by `configure`, over the given SQLite storage. */
export function withFirewall<A, E, R>(
  storage: string | { readonly filename: string; readonly agentKey: string },
  use: (
    firewall: Context.Tag.Service<typeof Firewall>,
    self: AgentCard,
  ) => Effect.Effect<A, E, R>,
  configure: (agentKey: string) => ReturnType<typeof Firewall.layer>,
) {
  const filename = typeof storage === "string" ? storage : storage.filename;
  const agentKey = typeof storage === "string" ? storage : storage.agentKey;
  const self = peer("alice", 1);
  const identity = Layer.succeed(
    Identity,
    Identity.of({
      me: self,
      resolve: () => Effect.dieMessage("Unexpected name resolution"),
    }),
  );
  const layer = configure(agentKey);
  const services = layer.pipe(
    Layer.provideMerge(identity),
    Layer.provideMerge(
      Layer.effectDiscard(migrateDaemon).pipe(
        Layer.provideMerge(SqliteClient.layer({ filename })),
      ),
    ),
  );
  return Effect.scoped(
    Effect.gen(function* () {
      return yield* use(yield* Firewall, self);
    }).pipe(Effect.provide(services), Effect.withSpan("withFirewall")),
  );
}

/** A draft from `self` to two peers that reveals a private record. */
export function outgoing(self: AgentCard) {
  return Message.create({
    author: self,
    recipients: [peer("bob", 2), peer("carol", 3)],
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
  }).pipe(Effect.map((message) => ({ kind: "message" as const, message })));
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

/** Firewall options with the given editable seed and fixed rules, judged by `provider`. */
export function policy(
  initialRules: readonly TextRule[] = [],
  fixedRules: readonly TextRule[] = [],
  provider: DecisionProvider = judgments,
): FirewallOptions {
  return {
    facts: [],
    settings: {
      timeoutMilliseconds: 10_000,
      maximumInputBytes: 1_000_000,
      maximumResultBytes: 100_000,
    },
    text: {
      fixedRules,
      initialRules,
      provider,
      settings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 60_000,
        timeoutMilliseconds: 5_000,
      },
    },
  };
}

/** A matcher that matches every message. */
export const always: Matcher = {
  match: () => Effect.succeed({ matched: true }),
};
/** A matcher that matches no message. */
export const never: Matcher = {
  match: () => Effect.succeed({ matched: false }),
};
