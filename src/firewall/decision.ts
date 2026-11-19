/** @file One message's decision and the check record that saves it, and the decisions that need no text check: the chain's, a contract's refusal, and that of a message nothing judged. */

import { Effect, Schema } from "effect";
import { randomBytes } from "node:crypto";
import type { Message } from "../communication/index.js";
import type { AgentCard } from "../identity.js";
import type {
  Direction,
  firewallDecisionSummarySchema,
  FirewallRefusalReason,
} from "./rules.js";
import type { CheckRecord } from "./store.js";
import { decodeJson } from "../encoding.js";
import {
  type Chain,
  type ChainDecision,
  evaluateChain,
  type FirewallEvidence,
} from "./chain.js";
import { InvalidCheckError, type LocalGuidance } from "./check.js";
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

/** A refused or withheld message's reason, the chain rule or contract stage that decided, and what its sender is told. */
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
 * message of an agent without a message policy that no chain rule matched has
 * no record.
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
 * The decision of the chain rule that decides a candidate, with its check
 * record, or nothing when no rule of `chain` matches it.
 */
export const chainDecision = Effect.fn("chainDecision")(function* (
  chain: Chain,
  checked: {
    readonly candidate: Candidate;
    readonly direction: Direction;
    readonly self: AgentCard;
    readonly revision: string;
  },
) {
  const { candidate, direction, self, revision } = checked;
  const decided = yield* evaluateChain(chain, {
    direction,
    message: candidate.message,
    evidence: candidate.evidence,
    self,
  });
  if (decided.kind === "unmatched") {
    return undefined;
  }
  const record: CheckRecord = {
    checkId: mintCheckId(),
    messageKey: messageKey(candidate.message),
    direction,
    messageJson: JSON.stringify(candidate.message),
    inputJson: JSON.stringify({ revision, ruleId: decided.ruleId }),
    resultJson: JSON.stringify({
      check: {
        ruleId: decided.ruleId,
        ...(decided.kind === "refused" ? { reason: decided.reason } : {}),
      },
      summaries: chainSummaries(candidate, direction, decided, revision),
    }),
    allowed: decided.kind === "allowed",
  };
  if (decided.kind === "allowed") {
    return { kind: "allowed", record } satisfies Decision;
  }
  return {
    kind: "refused",
    record,
    refusal: {
      reason: decided.reason,
      ruleId: decided.ruleId,
      feedback: [],
      ...(decided.detail === undefined ? {} : { detail: decided.detail }),
      ...(decided.reply === undefined ? {} : { reply: decided.reply }),
    },
  } satisfies Decision;
});

/** The allowed decision of a message no rule judged, with the record a later contract check reads of it. */
export function uncheckedDecision(
  revision: string,
  candidate: Candidate,
  direction: Direction,
): Decision {
  return {
    kind: "allowed",
    record: {
      checkId: mintCheckId(),
      messageKey: messageKey(candidate.message),
      direction,
      messageJson: JSON.stringify(candidate.message),
      inputJson: JSON.stringify({ revision }),
      resultJson: JSON.stringify({
        summaries: peersOf(candidate, direction).map(
          (peer): DecisionSummary => ({
            direction,
            peer,
            judgment: "allow",
            reason: "eligible",
            decidedBy: [],
            checkedRuleIds: [],
            activeRevision: revision,
          }),
        ),
      }),
      allowed: true,
    },
  };
}

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
 * the chain or the text rules carries the contract's diagnostics, and a
 * withheld act is answered with the `:on-refusal` act when one is declared.
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
      stage:
        decision.kind === "refused" && decision.refusal.ruleId !== undefined
          ? "policy"
          : "semantic",
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

/** One summary row per peer, as for a check of the text rules, naming the deciding chain rule. */
function chainSummaries(
  candidate: Candidate,
  direction: Direction,
  decided: Exclude<ChainDecision, { readonly kind: "unmatched" }>,
  revision: string,
): readonly DecisionSummary[] {
  const held =
    decided.kind === "refused" && decided.reason === "owner-approval-required";
  const judged = decided.kind === "refused" ? "reject" : "allow";
  return peersOf(candidate, direction).map((peer) => ({
    direction,
    peer,
    judgment: held ? "hold" : judged,
    reason: decided.kind === "allowed" ? "eligible" : decided.reason,
    decidedBy: [{ ruleId: decided.ruleId, from: "chain" }],
    checkedRuleIds: [decided.ruleId],
    activeRevision: revision,
  }));
}

/** The peers a decision summary names: the recipients of a message the agent sends, the author of one it receives. */
function peersOf(candidate: Candidate, direction: Direction) {
  return direction === "outgoing"
    ? candidate.message.recipients
    : [candidate.message.author];
}
