/**
 * @file Reads the Social Harness wire version literal from a workspace's
 * source, so release scripts can read any checked-out tree without running
 * its Client code.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const WIRE_VERSION_SOURCE = "packages/social-harness/src/version.ts";

const WIRE_VERSION_LITERAL =
  /export\s+const\s+WIRE_VERSION\s*=\s*["']([^"']*)["']/;

/**
 * Reads the trimmed source literal under an absolute workspace root.
 * Returns an error message for unreadable, missing or empty values.
 */
export const readCompatibilityVersion = (
  workspaceRoot: string,
): { readonly value: string } | { readonly error: string } => {
  const filePath = resolve(workspaceRoot, WIRE_VERSION_SOURCE);
  let source: string;
  try {
    source = readFileSync(filePath, "utf8");
  } catch (cause) {
    return {
      error: `could not read ${WIRE_VERSION_SOURCE}: ${cause instanceof Error ? cause.message : String(cause)}`,
    };
  }
  return parseCompatibilityVersion(source);
};

/**
 * Reads the wire version from source text so release-needed.sh can inspect a
 * committed tree without executing it.
 */
export function parseCompatibilityVersion(
  source: string,
): { readonly value: string } | { readonly error: string } {
  const match = WIRE_VERSION_LITERAL.exec(source);
  if (match === null) {
    return {
      error: `expected \`export const WIRE_VERSION = "..."\` in ${WIRE_VERSION_SOURCE}`,
    };
  }
  const value = (match[1] ?? "").trim();
  return value.length === 0
    ? { error: `expected a nonempty version in ${WIRE_VERSION_SOURCE}` }
    : { value };
}
