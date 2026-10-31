/**
 * @file Answer validation against a collective request's form-mode schema.
 *
 * Validation runs field by field so a failure names each failing field rather
 * than one combined message. The form-mode grammar is flat, so a field's
 * property schema is the whole rule for that field.
 *
 * Each keyword has its JSON Schema 2020-12 meaning, and every failing keyword
 * is reported: a keyword that constrains one JSON type applies only to a
 * value of that type, while `type`, `enum` and `oneOf` apply to any value.
 */

import { Effect } from "effect";
import type { CollectiveMemberOutcome } from "./inbound.js";
import type { FormField, FormModeSchema } from "./wire.js";
import { matchesFormat } from "./answer-formats.js";
import {
  type AnswerContent,
  type AnswerFieldFailures,
  answerFieldsText,
  type CollectiveResponse,
} from "./contracts.js";

type AnswerValue = AnswerContent[string];

type MultiSelectItems = Extract<
  FormField,
  { readonly items: unknown }
>["items"];

/** One keyword's check: each failure it finds, as `keyword: requirement`. */
type KeywordCheck = (field: FormField, value: AnswerValue) => readonly string[];

const ALLOWED_VALUES = "must be one of the allowed values";

/** Every JSON Schema check a form field's keywords make of an answer. */
const keywordChecks: readonly KeywordCheck[] = [
  typeIssues,
  limitIssues,
  formatIssues,
  enumIssues,
  oneOfIssues,
  itemIssues,
];

/**
 * Why one answer field failed its request's schema; `detail` names each
 * keyword an `invalid` field fails.
 */
type CollectiveFieldFailure = AnswerFieldFailures[number];

/**
 * Field names come from peers, so a lookup ignores the prototype chain: an
 * answer field named `constructor` must not find `Object.prototype.constructor`.
 */
const ownValue = <Value>(
  record: Readonly<Record<string, Value>>,
  key: string,
): Value | undefined => (Object.hasOwn(record, key) ? record[key] : undefined);

function fieldFailure(
  requestedSchema: FormModeSchema,
  content: AnswerContent,
  field: string,
): CollectiveFieldFailure | undefined {
  const definition = ownValue(requestedSchema.properties, field);
  if (definition === undefined) {
    return { field, reason: "unexpected" };
  }
  const value = ownValue(content, field);
  if (value === undefined) {
    return requestedSchema.required?.includes(field) === true
      ? { field, reason: "missing" }
      : undefined;
  }
  const issues = answerIssues(definition, value);
  return issues.length === 0
    ? undefined
    : { field, reason: "invalid", detail: issues.join(", ") };
}

/**
 * Check an answer against the schema its request carried.
 * @param requestedSchema The request's form-mode schema.
 * @param content The member's answer.
 * @returns The answer unchanged, or every failing field.
 */
export const validateAnswer = (
  requestedSchema: FormModeSchema,
  content: AnswerContent,
): Effect.Effect<AnswerContent, AnswerFieldFailures> => {
  const fields = new Set([
    ...Object.keys(requestedSchema.properties),
    ...Object.keys(content),
  ]);
  const [first, ...rest] = [...fields].flatMap((field) => {
    const failure = fieldFailure(requestedSchema, content, field);
    return failure === undefined ? [] : [failure];
  });
  return first === undefined
    ? Effect.succeed(content)
    : Effect.fail([first, ...rest]);
};

/**
 * Record a received response as the member's outcome. An `accept` whose content
 * fails the request's schema is recorded as invalid rather than rejected.
 * @param requestedSchema The schema the request carried.
 * @param response The member's decoded response.
 * @returns The member's outcome.
 */
export const outcomeOfResponse = (
  requestedSchema: FormModeSchema,
  response: CollectiveResponse,
): Effect.Effect<CollectiveMemberOutcome> => {
  switch (response.action) {
    case "accept":
      return validateAnswer(requestedSchema, response.content).pipe(
        Effect.match({
          onFailure: (fields): CollectiveMemberOutcome => ({
            kind: "invalid",
            reason: answerFieldsText(fields),
          }),
          onSuccess: (content): CollectiveMemberOutcome => ({
            kind: "answered",
            content,
          }),
        }),
      );
    case "decline":
      return Effect.succeed({ kind: "declined" });
    default: {
      const exhaustive: never = response;
      return exhaustive;
    }
  }
};

/**
 * Every failing keyword of one answer value. A titled single-select's value
 * must equal exactly one option, so options repeating a `const` admit
 * neither.
 * @param field The field's admitted property schema.
 * @param value The answer's value for the field.
 * @returns Each failing keyword with what it requires; empty when the value
 *   satisfies the field.
 */
function answerIssues(field: FormField, value: AnswerValue): readonly string[] {
  return keywordChecks.flatMap((check) => check(field, value));
}

function typeIssues(field: FormField, value: AnswerValue): readonly string[] {
  return hasType(field, value) ? [] : [`type: must be ${field.type}`];
}

function hasType(field: FormField, value: AnswerValue): boolean {
  switch (field.type) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number";
    case "integer":
      return typeof value === "number" && Number.isInteger(value);
    case "boolean":
      return typeof value === "boolean";
    case "array":
      return Array.isArray(value);
    default: {
      const exhaustive: never = field;
      return exhaustive;
    }
  }
}

/**
 * Report every failed bound without skipping later diagnostics. String bounds
 * count Unicode code points, so an astral character counts once.
 */
function limitIssues(field: FormField, value: AnswerValue): readonly string[] {
  switch (field.type) {
    case "string":
      return stringLimitIssues(field, value);
    case "number":
    case "integer":
      return numberLimitIssues(field, value);
    case "array":
      return arrayLimitIssues(field, value);
    case "boolean":
      return [];
    default: {
      const exhaustive: never = field;
      return exhaustive;
    }
  }
}

function stringLimitIssues(
  field: Extract<FormField, { readonly type: "string" }>,
  value: AnswerValue,
): readonly string[] {
  if (typeof value !== "string" || "enum" in field || "oneOf" in field) {
    return [];
  }
  const length = Array.from(value).length;
  return [
    ...(field.minLength !== undefined && length < field.minLength
      ? [`minLength: must have at least ${field.minLength} characters`]
      : []),
    ...(field.maxLength !== undefined && length > field.maxLength
      ? [`maxLength: must have at most ${field.maxLength} characters`]
      : []),
  ];
}

function numberLimitIssues(
  field: Extract<FormField, { readonly type: "number" | "integer" }>,
  value: AnswerValue,
): readonly string[] {
  if (typeof value !== "number") {
    return [];
  }
  return [
    ...(field.minimum !== undefined && value < field.minimum
      ? [`minimum: must be >= ${field.minimum}`]
      : []),
    ...(field.maximum !== undefined && value > field.maximum
      ? [`maximum: must be <= ${field.maximum}`]
      : []),
  ];
}

function arrayLimitIssues(
  field: Extract<FormField, { readonly type: "array" }>,
  value: AnswerValue,
): readonly string[] {
  if (typeof value !== "object") {
    return [];
  }
  return [
    ...(field.minItems !== undefined && value.length < field.minItems
      ? [`minItems: must have at least ${field.minItems} items`]
      : []),
    ...(field.maxItems !== undefined && value.length > field.maxItems
      ? [`maxItems: must have at most ${field.maxItems} items`]
      : []),
  ];
}

function formatIssues(field: FormField, value: AnswerValue): readonly string[] {
  if (
    typeof value !== "string" ||
    !("format" in field) ||
    field.format === undefined
  ) {
    return [];
  }
  return matchesFormat(field.format, value)
    ? []
    : [`format: must match format "${field.format}"`];
}

function enumIssues(field: FormField, value: AnswerValue): readonly string[] {
  return "enum" in field && !field.enum.some((allowed) => allowed === value)
    ? [`enum: ${ALLOWED_VALUES}`]
    : [];
}

function oneOfIssues(field: FormField, value: AnswerValue): readonly string[] {
  return "oneOf" in field &&
    field.oneOf.filter((option) => option.const === value).length !== 1
    ? ["oneOf: must equal exactly one option's const"]
    : [];
}

function itemIssues(field: FormField, value: AnswerValue): readonly string[] {
  if (typeof value !== "object" || !("items" in field)) {
    return [];
  }
  const { items } = field;
  return value.flatMap((item, index) => selectedItemIssues(items, item, index));
}

function selectedItemIssues(
  items: MultiSelectItems,
  item: string,
  index: number,
): readonly string[] {
  if ("enum" in items) {
    return items.enum.includes(item)
      ? []
      : [`items[${index}].enum: ${ALLOWED_VALUES}`];
  }
  return items.anyOf.some((option) => option.const === item)
    ? []
    : [`items[${index}].anyOf: must equal some option's const`];
}
