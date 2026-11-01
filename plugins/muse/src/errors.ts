/** @file Native trigger failures carry neither credentials nor event content. */
import { Data } from "effect";

/** Queue publication and detection share the same local failure vocabulary. */
export class MuseWakeError extends Data.TaggedError("MuseWakeError")<{
  readonly reason:
    | "invalid-configuration"
    | "transport-failed"
    | "decode-failed";
}> {
  override get message(): string {
    return `Muse trigger failed (${this.reason}). Docs: https://github.com/social-harness/moltzap/tree/main/plugins/muse#trigger-failed`;
  }
}
