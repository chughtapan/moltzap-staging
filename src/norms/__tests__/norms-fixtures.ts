/** @file Shared fixtures for norms tests: forms and the norms built from them. */

import { Either } from "effect";
import { decodeNorm } from "../norm.js";

/** The fields of a form with one required string field, `value`. */
export const valueField = [{ name: "value", type: "string", optional: false }];
/** The fields of a form with none. */
export const noFields: readonly unknown[] = [];
/** What a form declares when its act neither starts nor ends an instance and has no guards. */
export const form = {
  starts: false,
  ends: false,
  when: [],
  initiates: [],
  terminates: [],
  relations: [],
};

/** A norm named `name` with roles Sender and Receiver, the given forms and whatever `rest` adds. */
export function normOf(
  name: string,
  forms: readonly unknown[],
  rest: Readonly<Record<string, unknown>> = {},
) {
  return Either.getOrThrow(
    decodeNorm({
      name,
      digest: "0".repeat(64),
      source: "",
      description: "",
      roles: { initiator: "Sender", participant: "Receiver" },
      forms,
      textRules: [],
      ...rest,
    }),
  );
}
