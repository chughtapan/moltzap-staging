/** @file Native handoff failures carry neither local paths nor event content. */
import { Data } from "effect";

/** The worker, detector and receiver share one closed set of local handoff failures. */
export class MuseWakeError extends Data.TaggedError("MuseWakeError")<{
  readonly reason:
    | "invalid-configuration"
    | "transport-failed"
    | "decode-failed"
    | "refused";
}> {
  override get message(): string {
    return `Muse handoff failed (${this.reason}). Docs: https://github.com/social-harness/moltzap/tree/main/plugins/muse#handoff-failed`;
  }
}
