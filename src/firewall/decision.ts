/** @file One message's decision and the check record that saves it, and a contract's refusal, which needs no text check. */

import { Effect, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type { Message } from "../communication/index.js";
import type {
  Direction,
  firewallDecisionSummarySchema,
  FirewallRefusalReason,
} from "./rules.js";
import type { CheckRecord } from "./store.js";
import { decodeJson } from "../encoding.js";
import {
  type FirewallEvidence,
  InvalidCheckError,
  type LocalGuidance,
} from "./check.js";
import {
  type ContractCheck,
  contractReply,
  messageKey,
} from "./contract-check.js";

type DecisionSummary = typeof firewallDecisionSummarySchema.Type;

/** One message under check with its evidence. */
export interface Candidate {
  readonly message: Message;
  readonly evidence: FirewallEvidence;
}

/** A refused or withheld message's reason, the contract stage that decided, and what its sender is told. */
interface Refusal {
  readonly reason: FirewallRefusalReason;
  readonly ruleId?: string;
  readonly feedback: ReadonlyArray<{ readonly text: string }>;
  readonly detail?: unknown;
  readonly reply?: string;
}

/**
 * One candidate's decision and the check record that saves it. `refused`
 * refuses a message the agent sends and withholds one it receives. Only a
 * message of an agent without a message policy has no record.
 */
export type Decision =
  | {
      readonly kind: "allowed";
      readonly record?: CheckRecord;
      readonly guidance?: LocalGuidance;
    }
  | {
      readonly kind: "refused";
      readonly record: CheckRecord;
      readonly refusal: Refusal;
    };

/**
 * The refusal of a message that fails its contract's structure or protocol.
 * Its rule names the norm of the instance in progress and the stage that
 * failed. A withheld message is answered with the contract's `:on-refusal`
 * act when it declares one.
 */
export function contractRefusal(
  revision: string,
  candidate: Candidate,
  direction: Direction,
  check: Extract<ContractCheck, { readonly kind: "refused" }>,
): Decision {
  const { problem, selected } = check;
  const detail = {
    ...(selected === undefined
      ? {}
      : { norm: selected.name, digest: selected.digest }),
    stage: problem.stage,
    explanation: problem.detail,
    ...(problem.field === undefined ? {} : { field: problem.field }),
  };
  const ruleId = `${check.view.instance?.contract.name ?? "norm"}-${problem.stage}`;
  const reply =
    direction === "incoming" ? contractReply(check, problem.detail) : undefined;
  return {
    kind: "refused",
    record: {
      checkId: mintCheckId(),
      messageKey: messageKey(candidate.message),
      direction,
      messageJson: JSON.stringify(candidate.message),
      inputJson: JSON.stringify({
        revision,
        ...(selected === undefined
          ? {}
          : { norm: selected.name, digest: selected.digest }),
      }),
      resultJson: JSON.stringify({
        detail,
        summaries: peersOf(candidate, direction).map(
          (peer): DecisionSummary => ({
            direction,
            peer,
            judgment: problem.reason === "rule-violation" ? "reject" : "hold",
            reason: problem.reason,
            decidedBy: [{ ruleId, from: "chain" }],
            checkedRuleIds: [ruleId],
            activeRevision: revision,
          }),
        ),
      }),
      allowed: false,
    },
    refusal: {
      reason: problem.reason,
      ruleId,
      feedback: [{ text: problem.detail }],
      detail,
      ...(reply === undefined ? {} : { reply }),
    },
  };
}

/**
 * Adds to the decision of an act that passed its contract's structure and
 * protocol what its record saves: the contract's norm and digest, and for an
 * allowed act its protocol record, which later checks replay. A refusal by
 * the text rules carries the contract's diagnostics, and a withheld act is
 * answered with the `:on-refusal` act when one is declared.
 */
export const withContract = Effect.fn("withContract")(
  function* (
    decision: Decision,
    check: Extract<ContractCheck, { readonly kind: "ready" }>,
    direction: Direction,
  ) {
    if (decision.record === undefined) {
      return yield* new InvalidCheckError({ reason: "evidence" });
    }
    const json = Schema.Record({ key: Schema.String, value: Schema.Unknown });
    const input = yield* decodeJson(json, decision.record.inputJson);
    const result = yield* decodeJson(json, decision.record.resultJson);
    const detail = {
      norm: check.contract.name,
      digest: check.contract.digest,
      act: check.record.act.name,
      stage: "semantic",
    };
    const record: CheckRecord = {
      ...decision.record,
      inputJson: JSON.stringify({
        ...input,
        norm: check.contract.name,
        digest: check.contract.digest,
      }),
      resultJson: JSON.stringify({
        ...result,
        detail,
        ...(decision.kind === "allowed" ? { protocol: check.record } : {}),
      }),
    };
    if (decision.kind === "allowed") {
      return { ...decision, record } satisfies Decision;
    }
    const text =
      decision.refusal.feedback.map((entry) => entry.text).join(" ") ||
      `The semantic policy check returned ${decision.refusal.reason}.`;
    const reply =
      direction === "incoming" ? contractReply(check, text) : undefined;
    return {
      ...decision,
      record,
      refusal: {
        ...decision.refusal,
        detail,
        ...(reply === undefined ? {} : { reply }),
      },
    } satisfies Decision;
  },
  Effect.catchTag("JsonRepresentationError", () =>
    Effect.fail(new InvalidCheckError({ reason: "evidence" })),
  ),
);

/** A check id correlates a check without granting lookup authority. */
export function mintCheckId() {
  return `chk_${randomBytes(32).toString("hex")}`;
}

/** The peers a decision summary names: the recipients of a message the agent sends, the author of one it receives. */
function peersOf(candidate: Candidate, direction: Direction) {
  return direction === "outgoing"
    ? candidate.message.recipients
    : [candidate.message.author];
}
