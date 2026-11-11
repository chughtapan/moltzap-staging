/** @file The failures communication and sends report. */

import { Data } from "effect";

/** A refused send or read names one of these reasons. */
export class CommunicationError extends Data.TaggedError("CommunicationError")<{
  readonly reason:
    | "membership-invalid"
    | "content-invalid"
    | "persistence-failed"
    | "message-conflict"
    | "invalid-cursor"
    | "history-gap";
}> {}

/** The reasons a send fails with. */
export const sendFailureReasons = [
  "invalid-address",
  "unknown-agent",
  "membership-invalid",
  "content-invalid",
  "message-conflict",
  "persistence-failed",
  "network-unavailable",
  "outcome-unknown",
] as const;

type SendFailure = (typeof sendFailureReasons)[number];

/**
 * What the agent is told for each reason. A send with an unknown outcome may
 * already have happened, so a new send can duplicate it.
 */
const sendFailureText: Readonly<Record<SendFailure, string>> = {
  "invalid-address": "the address is not a valid agent: or group: address",
  "unknown-agent": "an agent the address names is not a known agent",
  "membership-invalid": "the address does not name a valid set of agents",
  "content-invalid": "the message content is invalid",
  "message-conflict":
    "the message identity already belongs to different content or recipients",
  "persistence-failed":
    "Social Harness is unavailable (the message could not be stored)",
  "network-unavailable": "Social Harness is unavailable (network unavailable)",
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
 * Check `reason` before retrying. A send with `outcome-unknown` may already
 * have happened, so a new send can duplicate it. The host may show `message`
 * to its agent. Validation issues omit rejected values.
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
