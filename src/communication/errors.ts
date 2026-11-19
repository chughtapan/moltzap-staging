/** @file The failures communication reports. */

import { Data } from "effect";

/** A refused send or read names one of these reasons. */
export class CommunicationError extends Data.TaggedError("CommunicationError")<{
  readonly reason:
    | "membership-invalid"
    | "content-invalid"
    | "persistence-failed"
    | "message-conflict"
    | "invalid-cursor";
}> {}

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
