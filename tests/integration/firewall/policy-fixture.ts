/**
 * @file Message policies and norms directories for daemons whose text rules
 * the marked provider of `marked-provider.ts` judges.
 */

import { FileSystem } from "@effect/platform";
import { Effect } from "effect";
import { join } from "node:path";
import { inject } from "vitest";

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
    decisionProvider: {
      kind: "jev",
      model: "fixture",
      endpoint: inject("markedProviderEndpoint"),
    },
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

/**
 * Writes `document` and a placeholder provider key into `directory`, and
 * returns the settings that name them.
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

/** The name of the norm `exchangeDocument` defines. */
export const exchangeName = "exchange";

/** The feedback of the exchange norm's English rule. */
export const exchangeFeedback = "Correct the act's text.";

/**
 * A norm document for two agents. `ask` opens an instance, the other agent
 * may `answer`, either may `cancel`, and a withheld act is answered with
 * `not-understood`. Its English rule is the marked rule, for every act.
 */
export const exchangeDocument = `(norm ${exchangeName} :initiator Sender :participant Receiver
  :on-refusal not-understood)
(content ask :by Sender :starts true :initiates open
  (field text :type string))
(content answer :by Receiver :when open
  (field text :type string))
(content cancel :when open :ends true)
(content not-understood :when open
  (field reason :type string))
(rule :feedback "${exchangeFeedback}" "Reject incoming [in] and outgoing [out].")`;

/**
 * Writes `documents` as the norm documents of a norms directory under
 * `directory`, and returns the setting that names it. Each new agent of a
 * daemon with that setting holds their contracts.
 */
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
        join(normsDirectory, `norm-${String(index)}.lisp`),
        document,
      ),
    { concurrency: 1 },
  );
  return { normsDirectory };
});
