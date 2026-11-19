/** @file Typed asymmetric matchers keep expected values outside the unsafe any type. */
import { expect } from "vitest";

/** A string-valued field that matches `pattern`. */
export function stringMatching(pattern: RegExp): unknown {
  return expect.stringMatching(pattern);
}

/** Match the observed fields while allowing additional response fields. */
export function objectContaining(value: Record<string, unknown>): unknown {
  return expect.objectContaining(value);
}

/** Match required members without constraining additional entries or order. */
export function arrayContaining(value: readonly unknown[]): unknown {
  return expect.arrayContaining([...value]);
}
