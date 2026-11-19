/** @file Shared fixtures for firewall service tests: an agent's firewall, messages, rules and a test provider. */

import { SqlClient } from "@effect/sql";
import { type Context, Effect, Encoding, Layer, Schema } from "effect";
import type { DecisionProvider, FirewallEvidence } from "../check.js";
import type { TextRule } from "../rules.js";
import { Message } from "../../communication/index.js";
import { openTestDatabase, type TestDatabase } from "../../database/testing.js";
import {
  AgentCard,
  AgentId,
  AgentName,
  Identity,
  PrincipalId,
} from "../../identity.js";
import { compiledContractSchema } from "../contract.js";
import { type ComposedMessagePolicy, Firewall } from "../service.js";

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
export const certainProvider: DecisionProvider = {
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
  const identity = Layer.succeed(
    Identity,
    Identity.of({
      me: self,
      resolve: () => Effect.dieMessage("Unexpected name resolution"),
    }),
  );
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

/** A message policy with the given initial editable rules and fixed rules, judged by `provider`. */
export function policy(
  initialRules: readonly TextRule[] = [],
  fixedRules: readonly TextRule[] = [],
  provider: DecisionProvider = certainProvider,
): ComposedMessagePolicy {
  return {
    facts: [],
    limits: {
      timeoutMilliseconds: 10_000,
      maximumInputBytes: 1_000_000,
      maximumResultBytes: 100_000,
    },
    text: {
      fixedRules,
      initialRules,
      provider,
      providerSettings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 60_000,
        timeoutMilliseconds: 5_000,
      },
    },
  };
}

/** A form of one string field, `value`. */
export const valueField = {
  type: "object",
  properties: { value: { type: "string" } },
  required: ["value"],
};
/** A form with no fields. */
export const noFields = { type: "object", properties: {}, required: [] };
/** What a form declares when its act neither starts nor ends an instance and has no guards. */
export const form = {
  starts: false,
  ends: false,
  when: [],
  unless: [],
  initiates: [],
  terminates: [],
  relations: [],
};

/** A contract named `name` with roles Sender and Receiver, the given forms and whatever `rest` adds. */
export function contractOf(
  name: string,
  forms: readonly unknown[],
  rest: Readonly<Record<string, unknown>> = {},
) {
  return Schema.decodeUnknownSync(compiledContractSchema)({
    name,
    digest: "0".repeat(64),
    source: "",
    description: "",
    roles: { initiator: "Sender", participant: "Receiver" },
    forms,
    textRules: [],
    ...rest,
  });
}
