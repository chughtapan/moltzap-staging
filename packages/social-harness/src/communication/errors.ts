/** @file Failures distinguish refused work from retained communication progress. */

import { Data, Schema } from "effect";

/** Closed communication failures are shared with caller-facing request errors. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Request failures reuse the closed communication reason grammar.
export const communicationFailure = Schema.Literal(
  "membership-invalid",
  "content-invalid",
  "version-mismatch",
  "certification-unavailable",
  "persistence-failed",
  "network-unavailable",
  "outcome-unknown",
  "unknown-message",
  "message-conflict",
  "invalid-cursor",
  "history-gap",
  "outbox-too-old",
  "agent-not-admitted",
);

/** Communication failures distinguish refused work from unknown acceptance. */
export class CommunicationError extends Data.TaggedError("CommunicationError")<{
  readonly reason: typeof communicationFailure.Type;
}> {}

/** Closed reason codes carried by sends and per-member collective failures. */
export const sendFailureReasons = [
  "invalid-address",
  "unknown-agent",
  "membership-invalid",
  "content-invalid",
  "message-conflict",
  "not-registered",
  "version-mismatch",
  "certification-unavailable",
  "persistence-failed",
  "network-unavailable",
  "delivery-pending",
  "outcome-unknown",
] as const;

/** Distinguishes a refusal from work that remains pending or uncertain. */
export type SendFailure = (typeof sendFailureReasons)[number];

/** Temporary availability or uncertain storage leaves accepted work recoverable. */
export function isTerminalSendFailure(reason: SendFailure): boolean {
  return (
    reason !== "network-unavailable" &&
    reason !== "delivery-pending" &&
    reason !== "outcome-unknown" &&
    reason !== "persistence-failed"
  );
}

/**
 * Pending work retains its original intent. A send with an unknown outcome
 * may already have happened. A new send can duplicate either message.
 */
const sendFailureText: Readonly<Record<SendFailure, string>> = {
  "invalid-address": "the address is not a valid agent: or group: address",
  "unknown-agent": "an agent the address names is not a known agent",
  "membership-invalid": "the address does not name a valid set of agents",
  "content-invalid": "the message content is invalid",
  "message-conflict":
    "the message identity already belongs to different content or recipients",
  "not-registered": "this agent is not registered with Social Harness",
  "version-mismatch": "Social Harness is unavailable (version mismatch)",
  "certification-unavailable":
    "Social Harness is unavailable (certification unavailable)",
  "persistence-failed":
    "Social Harness is unavailable (the message could not be stored)",
  "network-unavailable": "Social Harness is unavailable (network unavailable)",
  "delivery-pending":
    "the message is pending and Social Harness will still deliver it",
  "outcome-unknown":
    "the send did not complete; the message may or may not have been sent",
};

/**
 * A field path and schema expectation, without the rejected value.
 * @inline
 */
export interface ValidationIssue {
  readonly path: ReadonlyArray<string | number>;
  readonly message: string;
}

/** Formats issues in reported order as `field.path: expectation`, joined with semicolons. */
export function issuesText(
  issues: ReadonlyArray<{
    readonly path: readonly PropertyKey[];
    readonly message: string;
  }>,
): string {
  return issues
    .map(({ path, message }) =>
      path.length === 0 ? message : `${path.map(String).join(".")}: ${message}`,
    )
    .join("; ");
}

/**
 * Check `reason` before retrying. `delivery-pending` retains the original
 * message. A send with `outcome-unknown` may already have happened. A new send
 * can duplicate either message. The host may show `message` to its agent.
 * Validation issues omit rejected values.
 */
export class SendError extends Data.TaggedError("SendError")<{
  readonly reason: SendFailure;
  readonly detail?: string;
  readonly issues?: readonly ValidationIssue[];
}> {
  override get message(): string {
    const cause = this.detail ?? sendFailureText[this.reason];
    return this.issues === undefined || this.issues.length === 0
      ? `send failed: ${cause}`
      : `send failed: ${cause}: ${issuesText(this.issues)}`;
  }
}
