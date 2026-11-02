/** @file The npm integrity of a tarball, the one fact both download and restart check. */

import { createHash } from "node:crypto";

/**
 * The SRI `sha512-` integrity of `bytes`, as npm records it.
 *
 * @param bytes A tarball.
 * @returns Its integrity.
 */
export function integrityOf(bytes: Uint8Array): string {
  return `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
}
