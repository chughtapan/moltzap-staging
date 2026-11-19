/** @file A contract: the forms, turn rules and text rules one norm compiles to, and the reading of a message as one act of it. */

import { Data, Effect, Either, Schema } from "effect";
import {
  type Message,
  RequestedSchema,
  validateResponseContent,
} from "../communication/index.js";
import {
  exactStruct,
  readSExpressions,
  type SExpression,
  writeSExpression,
} from "../encoding.js";
import { TextRule } from "./rules.js";

const name = Schema.String.pipe(
  Schema.pattern(/^[a-zA-Z][a-zA-Z0-9-]{0,63}$/u),
);
const names = Schema.Array(name);
const actReference = Schema.Union(
  name,
  Schema.NonEmptyArray(name),
  exactStruct({ fact: name }),
);
const fieldValue = Schema.Union(
  Schema.String,
  Schema.Number.pipe(Schema.finite()),
  Schema.Boolean,
  Schema.Array(Schema.String),
);
const actSchema = exactStruct({
  name,
  norm: name,
  fields: Schema.Record({ key: name, value: fieldValue }),
});

/**
 * One act as a message states it: its performative in `name`, the norm its
 * `:ontology` names and its fields, each with the type its text gave it.
 */
export type Act = typeof actSchema.Type;

const fieldSource = exactStruct({
  kind: Schema.Literal("field"),
  act: actReference,
  field: name,
});
const sourceSchema = Schema.Union(
  fieldSource,
  exactStruct({
    kind: Schema.Literal("present"),
    act: actReference,
    fields: names,
  }),
  exactStruct({
    kind: Schema.Literal("values"),
    act: actReference,
    keys: fieldSource,
  }),
  exactStruct({
    kind: Schema.Literal("field-values"),
    act: actReference,
    fields: Schema.NonEmptyArray(name),
  }),
);
const relationSchema = exactStruct({
  operator: Schema.Literal("equals", "member", "subset"),
  field: name,
  source: sourceSchema,
});
const formSchema = exactStruct({
  act: name,
  by: Schema.Union(
    names,
    exactStruct({ recipient: exactStruct({ fact: name }) }),
  ),
  fields: RequestedSchema,
  starts: Schema.Boolean,
  ends: Schema.Boolean,
  when: names,
  unless: names,
  initiates: names,
  terminates: names,
  maximum: Schema.optional(Schema.Positive.pipe(Schema.int())),
  delay: Schema.optional(
    exactStruct({
      fact: name,
      field: name,
      millisecondsPerUnit: Schema.Positive,
    }),
  ),
  relations: Schema.Array(relationSchema),
});

/**
 * The form of one performative: its fields, who may send it, and the facts it
 * needs, sets and clears.
 */
export type MessageForm = typeof formSchema.Type;

/**
 * One norm as the firewall applies it. `digest` is the SHA-256 of `source`,
 * the norm document it was compiled from. A text rule's `act` names the one
 * performative it governs; a rule without it governs every act of the norm.
 */
export const compiledContractSchema = exactStruct({
  name,
  digest: Schema.String.pipe(Schema.pattern(/^[a-f0-9]{64}$/u)),
  source: Schema.String,
  description: Schema.String,
  roles: exactStruct({ initiator: name, participant: name }),
  forms: Schema.Array(formSchema),
  textRules: Schema.Array(
    exactStruct({ ...TextRule.fields, act: Schema.optional(name) }),
  ),
  onRefusal: Schema.optional(name),
});

/** A compiled norm: data the firewall checks messages against, with no code of its own. */
export type CompiledContract = typeof compiledContractSchema.Type;

/**
 * What the check record of an allowed act saves: the act and its contract's
 * digest. The act that opens an instance also saves the whole contract, so
 * the instance is replayed under the document it began with.
 */
export const protocolRecordSchema = exactStruct({
  act: actSchema,
  digest: compiledContractSchema.fields.digest,
  opening: Schema.optional(compiledContractSchema),
});

/** @useDeclaredType */
export type ProtocolRecord = typeof protocolRecordSchema.Type;

/**
 * One message of a conversation's history with what this firewall saved when
 * it checked it: the time of that check and, for an act it allowed, its
 * protocol record.
 */
export interface CheckedMessage {
  readonly message: Message;
  readonly at: number;
  readonly protocol?: ProtocolRecord;
}

/** Why a message fails its contract: the stage that failed, and the text its sender reads. */
export class ContractProblem extends Data.TaggedError("ContractProblem")<{
  readonly stage: "structural" | "protocol";
  readonly reason: "rule-violation" | "unresolved-evidence";
  readonly detail: string;
  readonly field?: string;
}> {}

/** Whether a contract can govern `message`: a message from one agent to one other. */
export function isDirectText(message: Message) {
  return message.recipients.length === 1;
}

/**
 * Reads a message as one act. A contract governs direct text messages: the
 * whole text is one parenthesized act, with no other content part.
 */
export const readAct = Effect.fn("readAct")(function* (message: Message) {
  if (
    !isDirectText(message) ||
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

/** The form `contract` declares for `act`, once the act's fields fit it. */
export const checkStructure = Effect.fn("checkStructure")(function* (
  contract: CompiledContract,
  act: Act,
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
  yield* validateResponseContent(form.fields, act.fields).pipe(
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

/** An act as message text, with strings quoted as a norm document quotes them. */
export function formatAct(act: Act): string {
  const values: SExpression[] = [
    { kind: "symbol", value: act.name },
    { kind: "symbol", value: ":ontology" },
    { kind: "symbol", value: act.norm },
  ];
  for (const [field, value] of Object.entries(act.fields)) {
    values.push({ kind: "symbol", value: `:${field}` }, valueExpression(value));
  }
  return writeSExpression({ kind: "list", values });
}

function readParameters(
  performative: string,
  parameters: readonly SExpression[],
): Effect.Effect<Act, ContractProblem> {
  return Effect.gen(function* () {
    const fields = new Map<string, Act["fields"][string]>();
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
      name: performative,
      norm,
      fields: Object.fromEntries(fields),
    };
  });
}

function readValue(
  node: SExpression,
): Effect.Effect<Act["fields"][string], ContractProblem> {
  if (node.kind === "string") {
    return Effect.succeed(node.value);
  }
  if (node.kind === "list") {
    return Effect.forEach(
      node.values,
      (choice) =>
        choice.kind === "list"
          ? Effect.fail(
              structuralProblem("A list field contains scalar choices."),
            )
          : Effect.succeed(choice.value),
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

function valueExpression(value: Act["fields"][string]): SExpression {
  if (typeof value === "string") {
    return { kind: "string", value };
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return { kind: "symbol", value: String(value) };
  }
  return {
    kind: "list",
    values: value.map((choice) => ({ kind: "string", value: choice })),
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
