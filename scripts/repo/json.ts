/** @file JSON readers for repository manifests. */
import { Data } from "effect";
import { readFileSync } from "node:fs";

/** Fields remain unknown until a consumer narrows them. */
export type JsonObject = Readonly<Record<string, unknown>>;

/** Malformed manifests cannot supply dependency-version evidence. */
class MalformedJsonInput extends Data.TaggedError("MalformedJsonInput")<{
  readonly message: string;
}> {}

/**
 * Requires an absolute path. Unreadable files, invalid JSON and non-object
 * values fail the read.
 */
export function readJsonObject(file: string): JsonObject {
  const value: unknown = JSON.parse(readFileSync(file, "utf8"));
  if (!isJsonObject(value)) {
    throw new MalformedJsonInput({
      message: `Malformed ${file}: expected a JSON object`,
    });
  }
  return value;
}

/** Returns an empty object for an absent field and rejects malformed fields. */
export function objectAt(source: JsonObject, key: string): JsonObject {
  const value = source[key];
  if (value === undefined) {
    return {};
  }
  if (!isJsonObject(value)) {
    throw new MalformedJsonInput({
      message: `Malformed ${key}: expected an object`,
    });
  }
  return value;
}

/** Returns undefined for absent or non-string values. */
export function stringAt(source: JsonObject, key: string): string | undefined {
  const value = source[key];
  return typeof value === "string" ? value : undefined;
}

/** Excludes null and arrays from object values. */
export function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
