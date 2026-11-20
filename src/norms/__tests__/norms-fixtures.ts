/** @file Shared fixtures for norms tests: forms and the contracts built from them. */

import { Schema } from "effect";
import { compiledContractSchema } from "../contract.js";

/** A form of one string field, `value`. */
export const valueField = {
  type: "object",
  properties: { value: { type: "string" } },
  required: ["value"],
};
/** A form with no fields. */
export const noFields = { type: "object", properties: {}, required: [] };
/** What a form declares when its act neither starts nor ends an instance and has no guards. */
export const form = {
  starts: false,
  ends: false,
  when: [],
  initiates: [],
  terminates: [],
  relations: [],
};

/** A contract named `name` with roles Sender and Receiver, the given forms and whatever `rest` adds. */
export function contractOf(
  name: string,
  forms: readonly unknown[],
  rest: Readonly<Record<string, unknown>> = {},
) {
  return Schema.decodeUnknownSync(compiledContractSchema)({
    name,
    digest: "0".repeat(64),
    source: "",
    description: "",
    roles: { initiator: "Sender", participant: "Receiver" },
    forms,
    textRules: [],
    ...rest,
  });
}
