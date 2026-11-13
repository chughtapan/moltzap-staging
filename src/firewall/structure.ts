/** @file Checks a whole structured message against its declared fields. */

import { Effect, Either } from "effect";
import { validateAnswer } from "../communication/index.js";
import type { Outgoing } from "../communication/service.js";
import {
  readSExpressions,
  type SExpression,
  writeSExpression,
} from "../sexpr.js";
import {
  type CompiledContract,
  ContractProblem,
  type StructuredAct,
} from "./contract.js";

/** A governed direct message contains exactly one act and no uninspected content parts. */
export const readAct = Effect.fn("readAct")(function* (message: Outgoing) {
  if (
    "kind" in message ||
    message.operation !== undefined ||
    message.recipients.length !== 1 ||
    message.content.some((part) => part.type !== "text")
  ) {
    return yield* structuralProblem(
      "This contract governs direct text messages.",
    );
  }
  const text = message.content
    .map((part) => (part.type === "text" ? part.text : ""))
    .join("\n");
  const [form, ...extra] = yield* readSExpressions(text, false).pipe(
    Either.mapLeft((error) => structuralProblem(error.detail)),
  );
  if (form?.kind !== "list" || extra.length !== 0) {
    return yield* structuralProblem(
      "Error: Reply does not contain exactly one parenthesized speech act.",
    );
  }
  const [head, ...parameters] = form.values;
  if (
    head?.kind !== "symbol" ||
    !/^[a-z][a-z0-9-]{0,63}$/u.test(head.value) ||
    parameters.length % 2 !== 0
  ) {
    return yield* structuralProblem(
      "An act names a performative followed by :field value pairs.",
    );
  }
  return yield* readParameters(head.value, parameters);
});

/** Validation returns the form for subsequent protocol checks, never a final admission. */
export const checkStructure = Effect.fn("checkStructure")(function* (
  contract: CompiledContract,
  act: StructuredAct,
) {
  const form = contract.forms.find((entry) => entry.act === act.name);
  if (contract.name !== act.norm) {
    return yield* new ContractProblem({
      stage: "protocol",
      reason: "rule-violation",
      detail: `Error: ${act.name} is not valid for the active norm ${contract.name}.`,
    });
  }
  if (form === undefined) {
    return yield* structuralProblem(
      `Error: ${act.name} is not a valid act in ${act.norm}.`,
    );
  }
  yield* validateAnswer(form.fields, act.fields).pipe(
    Effect.mapError((failures) =>
      structuralProblem(
        failures
          .map((failure) => {
            if (failure.reason === "unexpected") {
              return `Error: ${failure.field} is not a valid field of ${act.name} in ${act.norm}.`;
            }
            if (failure.reason === "missing") {
              return `Error: ${failure.field} is required for ${act.name} in ${act.norm}.`;
            }
            return `Error: ${failure.field} is not valid for ${act.name} in ${act.norm}.`;
          })
          .join(" "),
      ),
    ),
  );
  return form;
});

/** Automatic policy feedback uses the same quoted-value representation as agent messages. */
export function formatAct(act: StructuredAct): string {
  const values: SExpression[] = [
    { kind: "symbol", value: act.name },
    { kind: "symbol", value: ":ontology" },
    { kind: "symbol", value: act.norm },
  ];
  for (const [name, value] of Object.entries(act.fields)) {
    values.push({ kind: "symbol", value: `:${name}` }, valueExpression(value));
  }
  return writeSExpression({ kind: "list", values });
}

function readParameters(
  name: string,
  parameters: readonly SExpression[],
): Effect.Effect<StructuredAct, ContractProblem> {
  return Effect.gen(function* () {
    const fields = new Map<string, StructuredAct["fields"][string]>();
    for (let index = 0; index < parameters.length; index += 2) {
      const key = parameters[index];
      const value = parameters[index + 1];
      if (
        key?.kind !== "symbol" ||
        !/^:[a-z][a-z0-9-]{0,63}$/u.test(key.value) ||
        value === undefined
      ) {
        return yield* structuralProblem(
          "Parameters must have distinct :names and one value each.",
        );
      }
      const field = key.value.slice(1);
      if (fields.has(field)) {
        return yield* structuralProblem(
          `Error: ${field} appears more than once.`,
          field,
        );
      }
      fields.set(field, yield* readValue(value));
    }
    const norm = fields.get("ontology");
    if (typeof norm !== "string") {
      return yield* structuralProblem(
        ":ontology must name an installed norm.",
        "ontology",
      );
    }
    fields.delete("ontology");
    return {
      name,
      norm,
      fields: Object.fromEntries(fields),
    };
  });
}

function readValue(
  node: SExpression,
): Effect.Effect<StructuredAct["fields"][string], ContractProblem> {
  if (node.kind === "string") {
    return Effect.succeed(node.value);
  }
  if (node.kind === "list") {
    return Effect.forEach(
      node.values,
      (item) =>
        item.kind === "list"
          ? Effect.fail(
              structuralProblem("A list field contains scalar choices."),
            )
          : Effect.succeed(item.value),
      { concurrency: 1 },
    );
  }
  if (node.value === "true" || node.value === "false") {
    return Effect.succeed(node.value === "true");
  }
  if (!/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/u.test(node.value)) {
    return Effect.succeed(node.value);
  }
  return Number.isFinite(Number(node.value))
    ? Effect.succeed(Number(node.value))
    : Effect.fail(structuralProblem("Numbers must be finite."));
}

function valueExpression(value: StructuredAct["fields"][string]): SExpression {
  if (typeof value === "string") {
    return { kind: "string", value };
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return { kind: "symbol", value: String(value) };
  }
  return {
    kind: "list",
    values: value.map((item) => ({ kind: "string", value: item })),
  };
}

function structuralProblem(detail: string, field?: string) {
  return new ContractProblem({
    stage: "structural",
    reason: "rule-violation",
    detail,
    ...(field === undefined ? {} : { field }),
  });
}
