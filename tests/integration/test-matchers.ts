/** @file Typed asymmetric matchers keep expected values outside the unsafe any type. */
import { expect } from "vitest";

/** A string-valued field whose exact value the scenario does not constrain. */
export const anyString: unknown = expect.any(String);

/** A string-valued field that matches `pattern`. */
export function stringMatching(pattern: RegExp): unknown {
  return expect.stringMatching(pattern);
}

/** A numeric field whose exact value the scenario does not constrain. */
export const anyNumber: unknown = expect.any(Number);

/** Match the observed fields while allowing additional response fields. */
export function objectContaining(value: Record<string, unknown>): unknown {
  return expect.objectContaining(value);
}

/** Match required members without constraining additional entries or order. */
export function arrayContaining(value: readonly unknown[]): unknown {
  return expect.arrayContaining([...value]);
}
