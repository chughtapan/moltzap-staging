/** @file Reports the package version and defines compatible protocol versions. */

import { Context, Effect, Option, Schema } from "effect";
import packageJson from "../package.json" with { type: "json" };

/** Installed package version, independent of the versions admitted on the network. */
export const packageVersion: string = packageJson.version;

/** Calendar wire syntax shared by HTTP mismatch decoders and release admission. */
export const WIRE_VERSION_PATTERN = /^\d{4}\.\d{3,4}\.\d+$/;

/**
 * The version emitted by new artifacts. The {@link LIVE_WIRE_VERSIONS} window
 * determines which versions peers admit, independently of package publication versions.
 */
export const WIRE_VERSION = "2026.1030.1";

/**
 * The emitted version and at most one adjacent version let neighboring
 * releases interoperate on either side of a version bump.
 */
type LiveWindow =
  | readonly [emitted: typeof WIRE_VERSION]
  | readonly [emitted: typeof WIRE_VERSION, adjacent: string];

/** A window whose adjacent version repeats the emitted one is not a window. */
type DistinctWindow<Window extends LiveWindow> = Window extends readonly [
  string,
  typeof WIRE_VERSION,
]
  ? never
  : Window;

const liveWindow = [WIRE_VERSION] as const satisfies LiveWindow;

/**
 * Versions accepted at outer-envelope and authenticated HTTP admission by the
 * Registry, the Router, and the daemon.
 */
export const LIVE_WIRE_VERSIONS = liveWindow satisfies DistinctWindow<
  typeof liveWindow
>;

/** Versions that left the live window since the last protocol reset. */
const RETIRED_WIRE_VERSIONS = [] as const;

/** Retained artifacts remain readable until the next protocol reset. */
export const HISTORY_WIRE_VERSIONS = [
  ...LIVE_WIRE_VERSIONS,
  ...RETIRED_WIRE_VERSIONS,
] as const;

/** A version represented by retained protocol artifacts. */
export type WireVersion = (typeof HISTORY_WIRE_VERSIONS)[number];

/**
 * Injectable release policy lets process tests compose adjacent wire releases.
 * Production compositions use the binary's emitted version and acceptance sets.
 */
export class WireVersions extends Context.Tag(
  "@moltzap/social-harness/WireVersions",
)<
  WireVersions,
  {
    readonly emitted?: string;
    readonly live: readonly string[];
    readonly history: readonly string[];
  }
>() {
  /** Signing uses one version throughout a process composition. */
  static readonly emitted = Effect.serviceOption(WireVersions).pipe(
    Effect.map((configuration) =>
      Option.match(configuration, {
        onNone: () => WIRE_VERSION,
        onSome: (sets) => sets.emitted ?? WIRE_VERSION,
      }),
    ),
  );

  /** Retained values share a window throughout nested artifact decoding. */
  static readonly history = Schema.String.pipe(
    Schema.filterEffect((version) =>
      Effect.serviceOption(WireVersions).pipe(
        Effect.map((configuration) =>
          Option.match(configuration, {
            onNone: () =>
              HISTORY_WIRE_VERSIONS.some((item) => item === version),
            onSome: (sets) =>
              HISTORY_WIRE_VERSIONS.some((item) => item === version) ||
              sets.history.includes(version),
          }),
        ),
      ),
    ),
  );

  /** Outer admission is independent from retained readability. */
  static readonly live = (version: string) =>
    Effect.serviceOption(WireVersions).pipe(
      Effect.map((configuration) =>
        Option.match(configuration, {
          onNone: () => LIVE_WIRE_VERSIONS.some((item) => item === version),
          onSome: (sets) =>
            sets.live.includes(version) &&
            (sets.emitted !== undefined ||
              LIVE_WIRE_VERSIONS.some((item) => item === version)),
        }),
      ),
    );
}

/** Read-only compatibility operations expose no injectable context tag. */
export const wireVersions = Object.freeze({
  emitted: WireVersions.emitted,
  history: WireVersions.history,
  live: WireVersions.live,
  liveSet: Effect.serviceOption(WireVersions).pipe(
    Effect.map((configuration) =>
      Option.match(configuration, {
        onNone: () => LIVE_WIRE_VERSIONS,
        onSome: (sets) => sets.live,
      }),
    ),
  ),
});
