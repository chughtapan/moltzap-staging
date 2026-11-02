/** @file Validates identifier spelling and orders IDs and names. */

import { Array, Either, Encoding, Schema } from "effect";

const HASH_BYTE_LENGTH = 32;

/**
 * Returns bytes for canonical base64url, or undefined for another spelling.
 */
export const decodeCanonicalBase64Url = (
  value: string,
): Uint8Array | undefined =>
  Either.match(Encoding.decodeBase64Url(value), {
    onLeft: () => undefined,
    onRight: (bytes) =>
      Encoding.encodeBase64Url(bytes) === value ? bytes : undefined,
  });

/**
 * Requires the prefix followed by canonical base64url encoding of 32 bytes.
 */
export function isCanonicalIdentifier(prefix: string, value: string): boolean {
  if (!value.startsWith(prefix)) {
    return false;
  }
  return (
    decodeCanonicalBase64Url(value.slice(prefix.length))?.byteLength ===
    HASH_BYTE_LENGTH
  );
}

/**
 * Compares validated agent IDs by decoded bytes, as membership lists require.
 */
export const compareAgentIds = (left: string, right: string): number => {
  const leftBytes = decodeCanonicalBase64Url(left.slice(4));
  const rightBytes = decodeCanonicalBase64Url(right.slice(4));
  if (leftBytes === undefined || rightBytes === undefined) {
    return 0;
  }
  const length = Math.min(leftBytes.byteLength, rightBytes.byteLength);
  for (let index = 0; index < length; index += 1) {
    const difference = (leftBytes[index] ?? 0) - (rightBytes[index] ?? 0);
    if (difference !== 0) {
      return difference;
    }
  }
  return leftBytes.byteLength - rightBytes.byteLength;
};

/**
 * Compares ASCII names in the order used by group addresses.
 */
export const compareAscii = (left: string, right: string): number => {
  const sharedLength = Math.min(left.length, right.length);
  for (let index = 0; index < sharedLength; index += 1) {
    const difference = left.charCodeAt(index) - right.charCodeAt(index);
    if (difference !== 0) {
      return difference;
    }
  }
  return left.length - right.length;
};

/** Rejects duplicate, missing or out-of-order values. */
export function isStrictlyAscending<Value>(
  values: readonly Value[],
  compare: (left: Value, right: Value) => number,
): boolean {
  for (let index = 1; index < values.length; index += 1) {
    const previous = values[index - 1];
    const current = values[index];
    if (
      previous === undefined ||
      current === undefined ||
      compare(previous, current) >= 0
    ) {
      return false;
    }
  }
  return true;
}

/**
 * Requires canonical base64url of the specified decoded byte length.
 */
export const hasCanonicalBase64UrlLength = (
  value: string,
  byteLength: number,
): boolean => decodeCanonicalBase64Url(value)?.byteLength === byteLength;

/**
 * Keeps the prefix, decoded byte length and canonical base64url spelling identical
 * in runtime validation and generated JSON Schema.
 */
export const canonicalIdentifier = <const Name extends string>(
  name: Name,
  prefix: string,
  byteLength: number,
) => {
  const wholeCharacters = Math.floor((byteLength * 8) / 6);
  const remainingBits = (byteLength * 8) % 6;
  const finalCharacter =
    remainingBits === 0
      ? ""
      : `[${Array.makeBy(2 ** remainingBits, (value) =>
          Encoding.encodeBase64Url(
            Uint8Array.of(value << (8 - remainingBits)),
          ).charAt(0),
        ).join("")}]`;
  const pattern = new RegExp(
    `^${prefix}[A-Za-z0-9_-]{${wholeCharacters}}${finalCharacter}$`,
    "u",
  );
  const characters = Math.ceil((byteLength * 8) / 6);
  return Schema.String.pipe(
    Schema.length(prefix.length + characters),
    Schema.pattern(pattern),
    Schema.brand(name),
    Schema.annotations({
      identifier: name,
      description: `${name}: ${prefix} then ${String(characters)} base64url characters`,
    }),
  );
};
