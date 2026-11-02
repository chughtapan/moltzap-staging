/**
 * @file Which release a daemon start runs: the recorded release, or the
 * candidate the dist-tag or an exact pin names. Pure, so every refusal rule
 * is decided without the network or the disk.
 */

/** The release the agent ran last, with the wire versions its daemon admits. */
export interface RecordedRelease {
  readonly version: string;
  readonly liveWireVersions: readonly string[];
}

/** A published release, with the wire version its daemon emits. */
export interface CandidateRelease {
  readonly version: string;
  readonly wireVersion: string;
}

/** Why a start keeps the recorded release. */
export type KeepReason =
  | "current"
  | "older"
  | "outside-wire-window"
  | "upgrade-off"
  | "unresolved"
  | "unverified"
  | "uninstalled";

/** What one start runs. */
export type Choice =
  | { readonly kind: "keep"; readonly reason: KeepReason }
  | { readonly kind: "take" };

/** The packages a launcher may follow: the daemon alone, or the OpenClaw channel and the daemon it pins. */
export type ReleasePackage =
  | "@moltzap/social-harness"
  | "@moltzap/openclaw-channel";

/** The launcher's settings. */
export interface LauncherConfiguration {
  readonly directory: string;
  readonly releasePackage: ReleasePackage;
  readonly selection: Selection;
  readonly registry: string;
  /** The prefix the image installed, when it is not the launcher's own. */
  readonly seed?: string;
}

/** How the release to run is chosen. */
export type Selection =
  | { readonly kind: "follow"; readonly distTag: string }
  | { readonly kind: "pinned"; readonly version: string }
  | { readonly kind: "off" };

const CALENDAR_VERSION = /^(\d+)\.(\d+)\.(\d+)$/u;

/**
 * Decides whether a start takes `candidate` over `recorded`. Following a
 * dist-tag takes only a newer release whose emitted wire version the
 * recorded daemon already admits, so a release outside the live window, which
 * every reset release is, never applies on its own and no start moves an
 * agent backwards. An exact pin is the operator's choice in either direction
 * and across a wire change; the endpoint store still refuses a schema newer
 * than the pinned daemon creates.
 *
 * @param recorded The release the agent ran last.
 * @param candidate The release the selection resolved to.
 * @param selection How the release is chosen.
 * @returns Whether to take the candidate, or why the recorded release runs.
 */
export function choose(
  recorded: RecordedRelease,
  candidate: CandidateRelease,
  selection: Exclude<Selection, { readonly kind: "off" }>,
): Choice {
  const order = compareVersions(candidate.version, recorded.version);
  if (order === undefined) {
    return { kind: "keep", reason: "unresolved" };
  }
  if (order === 0) {
    return { kind: "keep", reason: "current" };
  }
  if (selection.kind === "pinned") {
    return { kind: "take" };
  }
  if (order < 0) {
    return { kind: "keep", reason: "older" };
  }
  return recorded.liveWireVersions.includes(candidate.wireVersion)
    ? { kind: "take" }
    : { kind: "keep", reason: "outside-wire-window" };
}

/**
 * Orders two calendar versions `YYYY.MDD.N` by their numeric parts.
 *
 * @param left One version.
 * @param right The other version.
 * @returns A negative number, zero or a positive number as `left` is older
 *   than, equal to or newer than `right`; `undefined` when either is not a
 *   calendar version.
 */
export function compareVersions(
  left: string,
  right: string,
): number | undefined {
  const a = CALENDAR_VERSION.exec(left);
  const b = CALENDAR_VERSION.exec(right);
  if (a === null || b === null) {
    return undefined;
  }
  for (const index of [1, 2, 3]) {
    const difference = Number(a[index]) - Number(b[index]);
    if (difference !== 0) {
      return difference;
    }
  }
  return 0;
}
