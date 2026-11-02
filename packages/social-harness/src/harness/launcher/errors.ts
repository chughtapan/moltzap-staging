/** @file Launch failures and their troubleshooting references. */

import { Data } from "effect";

/** Settings and release selection referenced by launcher help and failures. */
export const LAUNCHER_DOCS =
  "https://docs.moltzap.xyz/reference/services/daemon#launcher";

/** Startup failures link to this guide's diagnostic anchors. */
export const UPGRADES_GUIDE = "https://docs.moltzap.xyz/guides/upgrades";

/**
 * Registry and provenance failures keep the recorded release. Other failures
 * stop startup.
 */
export class LauncherError extends Data.TaggedError("LauncherError")<{
  readonly reason:
    | "configuration"
    | "seed"
    | "record"
    | "integrity"
    | "registry"
    | "provenance"
    | "install"
    | "start";
  readonly message: string;
}> {}

/**
 * Messages may include paths and versions. Keep file contents out of them.
 */
export const launcherError = (
  reason: LauncherError["reason"],
  message: string,
): LauncherError => new LauncherError({ reason, message });
