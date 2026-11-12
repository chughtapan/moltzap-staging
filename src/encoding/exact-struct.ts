/** @file Struct schemas that refuse missing and unknown properties. */

import type { ParseOptions } from "effect/SchemaAST";
import { Schema } from "effect";

/** Rejects missing required fields and unknown properties. */
export const exactParseOptions = {
  exact: true,
  onExcessProperty: "error",
} satisfies ParseOptions;

/** Rejects properties outside the supplied fields. */
export const exactStruct = <Fields extends Schema.Struct.Fields>(
  fields: Fields,
) => Schema.Struct(fields).annotations({ parseOptions: exactParseOptions });
