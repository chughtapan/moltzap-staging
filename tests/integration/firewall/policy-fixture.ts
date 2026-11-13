/**
 * @file Message policies, norms directories and a deterministic decision
 * provider for daemons that run the daemon's own composition. The provider
 * decides each check by the text it judges: an incoming text carrying `[in]`,
 * or an outgoing text carrying `[out]`, breaks every rule it is asked about,
 * and a text carrying `[unsure]` is left unsettled, which admits it with a reminder.
 */

import { FileSystem } from "@effect/platform";
import { daemonLayer } from "@social-harness/social-harness/service";
import {
  type DecisionProvider,
  DecisionProviderOverride,
  type DecisionRequest,
} from "@social-harness/social-harness/test";
import { ConfigProvider, Effect, Layer, Schema } from "effect";
import { join } from "node:path";

/** The marker an incoming text carries to break the rules it is checked against. */
const INCOMING_BREACH = "[in]";
/** The marker an outgoing text carries to break the rules it is checked against. */
const OUTGOING_BREACH = "[out]";
/** The marker a text carries for its judgment to be left unsettled. */
const UNSURE = "[unsure]";

/** The text rule the agents of a firewall fixture hold unless a test gives its daemon other rules. */
export const markedRule = {
  id: "marked",
  applies: "both",
  text: "Reject incoming messages containing [in] and outgoing messages containing [out].",
  guidance: "The marked message may break the rule.",
  feedback: { text: "Remove the marked text." },
} as const;

/**
 * A message policy whose text rules a provider judges. Every agent holds the
 * policy's `fixedRules`, and starts with its `initialRules` among the rules
 * it can edit. Without `text`, every agent holds `markedRule`.
 */
export function markedPolicy(
  text: {
    readonly fixedRules?: readonly unknown[];
    readonly initialRules?: readonly unknown[];
  } = { fixedRules: [markedRule] },
) {
  return {
    format: "org.social-harness/message-policy/v2",
    text,
    decisionProvider: { kind: "jev", model: "fixture" },
    textSettings: {
      minimumProbability: 0.8,
      maximumRequestBytes: 1_000_000,
      timeoutMilliseconds: 8_000,
    },
    settings: {
      timeoutMilliseconds: 10_000,
      maximumInputBytes: 1_000_000,
      maximumResultBytes: 1_000_000,
    },
  };
}

const stateSchema = Schema.parseJson(
  Schema.Struct({
    direction: Schema.Literal("incoming", "outgoing"),
    candidate: Schema.Struct({ text: Schema.optional(Schema.String) }),
  }),
);

/** Decides every question about a request by the markers in its candidate's text. */
export const markedProvider: DecisionProvider = {
  decide: (request) => Effect.succeed(markedResponse(request)),
};

/**
 * Writes `document` and a placeholder provider key into `directory`, and
 * returns the settings that name them. The provider override replaces the
 * provider the key would open.
 */
export const writeMessagePolicy = Effect.fn("writeMessagePolicy")(function* (
  directory: string,
  document: unknown,
) {
  const fs = yield* FileSystem.FileSystem;
  const messagePolicyFile = join(directory, "message-policy.json");
  const decisionProviderKeyFile = join(directory, "provider.key");
  yield* fs.writeFileString(messagePolicyFile, JSON.stringify(document));
  yield* fs.writeFileString(decisionProviderKeyFile, "fixture-provider-key");
  return { messagePolicyFile, decisionProviderKeyFile };
});

/** A synthetic contract exercises mechanism without pinning an authored scheduling policy. */
export const exchangeName = "exchange";
/** Both roles can cancel; continuations carry their parent in send metadata. */
export const exchangeDocument = `(norm ${exchangeName} :initiator Sender :participant Receiver)
(content ask :by Sender :starts true :initiates open
  (field text :type string))
(content answer :by Receiver :when open :ends true
  (field text :type string))
(content cancel :when open :ends true)
(rule :act ask :feedback "Remove the marked text." "Reject incoming [in] and outgoing [out].")`;

/** Installs exactly the given source documents into a daemon's configuration. */
export const writeNorms = Effect.fn("writeNorms")(function* (
  directory: string,
  documents: readonly string[],
) {
  const fs = yield* FileSystem.FileSystem;
  const normsDirectory = join(directory, "norms");
  yield* fs.makeDirectory(normsDirectory);
  yield* Effect.forEach(
    documents,
    (document, index) =>
      fs.writeFileString(
        join(normsDirectory, `contract-${index}.lisp`),
        document,
      ),
    { concurrency: 1 },
  );
  return { normsDirectory };
});

/**
 * Runs the daemon's own composition in this process with `environment` as its
 * configuration, judging text rules with `provider`, until the scope closes.
 */
export const launchDaemon = Effect.fn("launchDaemon")(
  (environment: Readonly<Record<string, string>>, provider: DecisionProvider) =>
    Layer.launch(daemonLayer).pipe(
      Effect.provideService(DecisionProviderOverride, provider),
      Effect.withConfigProvider(
        ConfigProvider.fromMap(new Map(Object.entries(environment))),
      ),
      Effect.forkScoped,
    ),
);

function markedResponse(request: DecisionRequest) {
  const state = Schema.decodeUnknownSync(stateSchema)(request.state);
  const text = state.candidate.text ?? "";
  const breached = text.includes(
    state.direction === "incoming" ? INCOMING_BREACH : OUTGOING_BREACH,
  );
  const selected = breached ? "violation" : "compliant";
  const probability = text.includes(UNSURE) ? 0.6 : 1;
  return {
    provider: "fixture",
    model: "deterministic",
    usage: {},
    raw: "",
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => [
        id,
        {
          choice: selected,
          probabilities: Object.fromEntries(
            Object.keys(question.criteria).map((choice) => [
              choice,
              choice === selected
                ? probability
                : (1 - probability) /
                  (Object.keys(question.criteria).length - 1),
            ]),
          ),
        },
      ]),
    ),
  };
}
