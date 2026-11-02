/** @file Failures of reliable message acceptance, completion and history. */

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
