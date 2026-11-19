/** @file One message's decision and the check record that saves it, and a contract's refusal, which needs no text check. */

import { randomBytes } from "node:crypto";
import type { Message } from "../communication/index.js";
import type { FirewallEvidence, LocalGuidance } from "./check.js";
import type {
  Direction,
  firewallDecisionSummarySchema,
  FirewallRefusalReason,
} from "./rules.js";
import type { CheckRecord } from "./store.js";
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
            reason: problem.reason,
            decidedBy: [{ ruleId, from: "contract" }],
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
