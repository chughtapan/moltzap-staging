/**
 * @file The manifest a package tarball carries, read from its bytes. Once
 * provenance covers those bytes, this manifest, not the registry's copy, is
 * what the release really pins and declares.
 */

import { Effect, Schema } from "effect";
import { gunzipSync } from "node:zlib";
import { launcherError, type LauncherError } from "./errors.js";

/** The manifest fields the launcher compares with the registry's metadata. */
const packedManifest = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  dependencies: Schema.optional(
    Schema.Record({ key: Schema.String, value: Schema.String }),
  ),
  sharnessHostCodeHash: Schema.optional(Schema.String),
  sharnessWireVersion: Schema.optional(Schema.String),
});

/** The fields of a packed manifest the launcher reads. */
export type PackedManifest = typeof packedManifest.Type;

const MANIFEST_PATH = "package/package.json";
const BLOCK = 512;

/**
 * The `package/package.json` an npm tarball holds.
 *
 * @param bytes A gzipped npm tarball.
 * @returns Its manifest, or a `provenance` failure for a tarball without a
 *   readable one.
 */
export const readPackedManifest = (
  bytes: Uint8Array,
): Effect.Effect<PackedManifest, LauncherError> =>
  Effect.try({
    try: () => manifestText(gunzipSync(bytes)),
    catch: () =>
      launcherError("provenance", "the tarball is not a gzipped tar"),
  }).pipe(
    Effect.flatMap((text) =>
      text === undefined
        ? Effect.fail(
            launcherError(
              "provenance",
              `the tarball holds no ${MANIFEST_PATH}`,
            ),
          )
        : Schema.decodeUnknown(Schema.parseJson(packedManifest))(text).pipe(
            Effect.mapError(() =>
              launcherError(
                "provenance",
                `the tarball's ${MANIFEST_PATH} is unreadable`,
              ),
            ),
          ),
    ),
  );

/** The text of the archive's manifest entry, walking ustar headers. */
function manifestText(archive: Uint8Array): string | undefined {
  const decoder = new TextDecoder();
  let offset = 0;
  while (offset + BLOCK <= archive.length) {
    const header = archive.subarray(offset, offset + BLOCK);
    const name = field(decoder, header, 0, 100);
    if (name === "") {
      return undefined;
    }
    const prefix = field(decoder, header, 345, 155);
    const size = Number.parseInt(field(decoder, header, 124, 12).trim(), 8);
    const start = offset + BLOCK;
    const path = prefix === "" ? name : `${prefix}/${name}`;
    if (path === MANIFEST_PATH && header[156] !== 0x78) {
      return decoder.decode(archive.subarray(start, start + size));
    }
    offset = start + Math.ceil(size / BLOCK) * BLOCK;
  }
  return undefined;
}

function field(
  decoder: TextDecoder,
  header: Uint8Array,
  start: number,
  length: number,
): string {
  const bytes = header.subarray(start, start + length);
  const end = bytes.indexOf(0);
  return decoder.decode(end === -1 ? bytes : bytes.subarray(0, end));
}
