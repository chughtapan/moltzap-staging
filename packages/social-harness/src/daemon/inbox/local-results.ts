/** @file Collective results check peer-controlled source messages before saving a host projection. */

import * as SqlClient from "@effect/sql/SqlClient";
import { Effect, Array as EffectArray } from "effect";
import type { CheckedMessage } from "../../firewall/index.js";
import type { InboundItem } from "../../host-protocol/items.js";
import type {
  FirewallEvidence,
  IncomingEntry,
  LocalAdmission,
} from "./types.js";
import { ResultSources } from "../../communication/collectives/index.js";
import {
  messageMembers,
  replyAddress,
  type Message as SharedMessage,
} from "../../communication/message.js";
import {
  decodeRuntimeValue,
  encodeRuntimeValue,
  EndpointStoreError,
} from "../../communication/store/index.js";
import { MAXIMUM_RUNTIME_ITEM_BYTES } from "../../encoding/limits.js";
import {
  type LocalGuidance,
  refusalReason,
} from "../../host-protocol/advice.js";
import { renderOutcome } from "../../host-protocol/render.js";
import { readFirewallEvidenceSlices } from "./evidence.js";
import {
  type FirewallEnvironment,
  messageFields,
  prepareMessageCheck,
  projectHistoryMessage,
} from "./firewall-context.js";

type ResultItem = Extract<InboundItem, { readonly kind: "collectiveResult" }>;
type FailureItem = Extract<InboundItem, { readonly kind: "operationFailed" }>;
type WithheldText = Extract<
  ResultItem["question"],
  { readonly kind: "withheld" }
>;
interface LocalItemCheck {
  readonly entry: IncomingEntry;
  readonly item: ResultItem | FailureItem;
}
interface SourceText {
  readonly kind: "answer" | "question";
  readonly message: SharedMessage;
  readonly text: string;
}
interface CheckedSource extends SourceText {
  readonly checked: CheckedMessage;
}

/** Own text is trusted; all selected peer text uses one captured policy installation. */
export function checkLocalItem(
  environment: FirewallEnvironment,
  local: LocalItemCheck,
): Effect.Effect<LocalAdmission, EndpointStoreError> {
  return local.item.kind === "operationFailed"
    ? failureAdmission(local.entry)
    : resultAdmission(environment, local.entry, local.item);
}

/** Only source text actually quoted in a public result grants raw-message visibility. */
export function resultVisibility(
  item: ResultItem,
  sources: ResultSources,
): LocalAdmission["visibility"] {
  return [
    ...sources.answers.map((message) => ({ kind: "answer" as const, message })),
    ...(sources.request === undefined
      ? []
      : [{ kind: "question" as const, message: sources.request }]),
  ].map((source) => ({
    message: {
      authorAgentId: source.message.author.agentId,
      msgId: source.message.msgId,
    },
    admitted:
      source.kind === "question"
        ? typeof item.question === "string"
        : item.outcomes.some(
            (outcome) =>
              outcome.member.agentId === source.message.author.agentId &&
              (outcome.outcome.kind === "answered" ||
                outcome.outcome.kind === "declined"),
          ),
  }));
}

function failureAdmission(entry: IncomingEntry) {
  return Effect.succeed({
    queuedItem: entry.canonicalItem,
    item: {
      deliveryToken: entry.deliveryToken,
      canonicalItem: entry.canonicalItem,
    },
    completions: [],
    visibility: [],
  } satisfies LocalAdmission);
}

function resultAdmission(
  environment: FirewallEnvironment,
  entry: IncomingEntry,
  item: ResultItem,
) {
  return Effect.gen(function* () {
    const sources = yield* decodeRuntimeValue(
      ResultSources,
      entry.canonicalSources ?? new Uint8Array(),
    );
    const checked = yield* checkSources(
      environment,
      entry,
      selectedSources(item, sources, environment.localAgentCard.agentId),
    );
    const projected = projectResult(item, checked);
    return {
      queuedItem: entry.canonicalItem,
      item: yield* projectedEntry(entry, projected, resultGuidance(checked)),
      completions: checked.map((source) => source.checked.completion),
      visibility: sourceVisibility(item, sources, checked),
    } satisfies LocalAdmission;
  });
}

function selectedSources(
  item: ResultItem,
  sources: ResultSources,
  localAgentId: string,
): readonly SourceText[] {
  return [
    ...sources.answers.flatMap((message): SourceText[] => {
      const outcome = item.outcomes.find(
        (candidate) => candidate.member.agentId === message.author.agentId,
      );
      return message.author.agentId === localAgentId || outcome === undefined
        ? []
        : [{ kind: "answer", message, text: renderOutcome(outcome.outcome) }];
    }),
    ...(sources.request === undefined ||
    sources.request.author.agentId === localAgentId ||
    typeof item.question !== "string"
      ? []
      : [
          {
            kind: "question" as const,
            message: sources.request,
            text: JSON.stringify(item.question),
          },
        ]),
  ];
}

function checkSources(
  environment: FirewallEnvironment,
  entry: IncomingEntry,
  sources: readonly SourceText[],
) {
  return Effect.gen(function* () {
    const captured = yield* readFirewallEvidenceSlices({
      ranges: sources.map((source) => ({
        message: source.message,
        conversation: replyAddress(source.message),
      })),
      maximumRecords: environment.firewall.configuration.maximumHistoryRecords,
      registrySignerPublicKey: environment.registrySignerPublicKey,
    }).pipe(Effect.provideService(SqlClient.SqlClient, environment.sql));
    const inputs = yield* Effect.forEach(
      sources,
      (source, index) => {
        const evidence = captured.slices[index];
        return evidence === undefined
          ? Effect.fail(new EndpointStoreError({ reason: "corrupt" }))
          : prepareSource(environment, entry, source, evidence);
      },
      { concurrency: 1 },
    );
    const checks =
      inputs.length === 0 ? [] : yield* environment.firewall.check(inputs);
    if (checks.length !== sources.length) {
      return yield* new EndpointStoreError({ reason: "corrupt" });
    }
    return yield* Effect.forEach(
      sources,
      (source, index) =>
        checks[index] === undefined
          ? Effect.fail(new EndpointStoreError({ reason: "corrupt" }))
          : Effect.succeed({ ...source, checked: checks[index] }),
      { concurrency: 1 },
    );
  });
}

function prepareSource(
  environment: FirewallEnvironment,
  entry: IncomingEntry,
  source: SourceText,
  evidence: FirewallEvidence,
) {
  return Effect.gen(function* () {
    const projected = yield* projectHistoryMessage({
      message: source.message,
      visible: false,
    });
    return yield* prepareMessageCheck(environment, {
      direction: "incoming",
      canonicalCandidate: entry.canonicalItem,
      evidence,
      message: {
        ...messageFields(projected.message),
        text: source.text,
      },
      cards: messageMembers(source.message),
    });
  });
}

function projectResult(
  item: ResultItem,
  checked: readonly CheckedSource[],
): ResultItem {
  const question = checked.find((source) => source.kind === "question");
  return {
    ...item,
    question:
      question === undefined || question.checked.completion.admit
        ? item.question
        : withheldBecause(question.checked),
    outcomes: EffectArray.map(item.outcomes, (outcome) => {
      const source = checked.find(
        (candidate) =>
          candidate.kind === "answer" &&
          candidate.message.author.agentId === outcome.member.agentId,
      );
      return source === undefined || source.checked.completion.admit
        ? outcome
        : { member: outcome.member, outcome: withheldBecause(source.checked) };
    }),
  };
}

function sourceVisibility(
  item: ResultItem,
  sources: ResultSources,
  checked: readonly CheckedSource[],
): LocalAdmission["visibility"] {
  return resultVisibility(item, sources).map((visibility) => {
    const assessment = checked.find(
      (source) =>
        source.message.author.agentId === visibility.message.authorAgentId &&
        source.message.msgId === visibility.message.msgId,
    );
    return {
      ...visibility,
      admitted:
        visibility.admitted &&
        (assessment === undefined || assessment.checked.completion.admit),
    };
  });
}

function withheldBecause(checked: CheckedMessage): WithheldText {
  return {
    kind: "withheld",
    checkId: checked.completion.checkId,
    reason: refusalReason(checked.result.reason),
  };
}

function resultGuidance(
  checked: readonly CheckedSource[],
): LocalGuidance | undefined {
  const answers = checked.flatMap((source) =>
    source.kind !== "answer" || source.checked.guidance === undefined
      ? []
      : [{ member: source.message.author, ...source.checked.guidance }],
  );
  const question = checked.find((source) => source.kind === "question")?.checked
    .guidance;
  return answers.length === 0 && question === undefined
    ? undefined
    : { answers, ...(question === undefined ? {} : { question }) };
}

function projectedEntry(
  entry: IncomingEntry,
  projected: ResultItem,
  guidance?: LocalGuidance,
): Effect.Effect<LocalAdmission["item"], EndpointStoreError> {
  return Effect.gen(function* () {
    const canonicalItem = yield* encodeRuntimeValue(projected);
    const canonicalGuidance =
      guidance === undefined ? undefined : yield* encodeRuntimeValue(guidance);
    const fits =
      canonicalGuidance !== undefined &&
      canonicalItem.byteLength + canonicalGuidance.byteLength <=
        MAXIMUM_RUNTIME_ITEM_BYTES;
    return {
      deliveryToken: entry.deliveryToken,
      canonicalItem,
      ...(fits ? { canonicalGuidance } : {}),
    };
  });
}
