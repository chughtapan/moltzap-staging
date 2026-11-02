/** @file Pins which message texts state an operation and how each is read. */

import { Either, FastCheck as fc, Schema } from "effect";
import { describe, expect, it } from "vitest";
import { requestId } from "../__tests__/collective-operation-fixtures.js";
import { MessageAddressInput } from "../communication/records/values.js";
import { MAXIMUM_DEADLINE_SECONDS } from "../communication/requests/contracts.js";
import { type MessageTextError, parseMessageText } from "./message-text.js";

const group = Schema.decodeUnknownSync(MessageAddressInput)(
  "group:alice,bob,carol",
);
const slotSchema = {
  type: "object",
  properties: { slot: { type: "string", enum: ["mon", "tue"] } },
  required: ["slot"],
};
const OPERATION_KEYS = ["gather", "all_gather", "action"];

/** Texts that read as plain text, each for a different reason. */
const PLAIN_TEXTS = [
  "Monday works for me.",
  '{"note": "a JSON object with no operation key"}',
  '["gather", 60]',
  '"gather"',
  '{"gather": "Which day?"',
  'Answer with {"action":"accept","content":{"slot":"mon"}}',
  "",
];

/** Malformed operation texts, the operation each states, and the field named. */
const REFUSALS: ReadonlyArray<
  readonly [object, MessageTextError["operation"], string]
> = [
  [{ gather: "Q", requestedSchema: slotSchema }, "gather", "deadline"],
  [{ gather: "Q", deadline: 60 }, "gather", "requestedSchema"],
  [
    { gather: "Q", deadline: 0, requestedSchema: slotSchema },
    "gather",
    "deadline",
  ],
  [
    { gather: "Q", deadline: 1.5, requestedSchema: slotSchema },
    "gather",
    "deadline",
  ],
  [
    {
      gather: "Q",
      deadline: MAXIMUM_DEADLINE_SECONDS + 1,
      requestedSchema: slotSchema,
    },
    "gather",
    "deadline",
  ],
  [
    {
      gather: "Q",
      deadline: 60,
      requestedSchema: { ...slotSchema, type: "array" },
    },
    "gather",
    "requestedSchema.type",
  ],
  [
    { gather: 7, deadline: 60, requestedSchema: slotSchema },
    "gather",
    "gather",
  ],
  [
    { gather: "Q", all_gather: "Q", deadline: 60, requestedSchema: slotSchema },
    "gather",
    "all_gather",
  ],
  [
    { gather: "Q", deadline: 60, requestedSchema: slotSchema, to: "agent:bob" },
    "gather",
    "to",
  ],
  [{ all_gather: "Q", deadline: 60 }, "all_gather", "requestedSchema"],
  [{ action: "maybe" }, "answer", "action"],
  [{ action: "cancel" }, "answer", "action"],
  [{ action: "accept" }, "answer", "content"],
  [
    { action: "accept", content: { slot: { day: "mon" } } },
    "answer",
    "content.slot",
  ],
];

describe("message text", () => {
  it("reads any text without an operation key as a multicast", readsPlainText);
  it.each(PLAIN_TEXTS)("reads %j as a multicast of the whole text", (text) => {
    expect(parsed(text)).toEqual({ to: group, text });
  });
  it(
    "reads a gather or all_gather as its question, deadline and form",
    readsQuestions,
  );
  it("reads every answer action", readsAnswers);
  it.each(REFUSALS)(
    "refuses %j as a %s naming %s",
    (value, operation, field) => {
      const error = refusal(JSON.stringify(value));

      expect(error.operation).toBe(operation);
      expect(error.message).toContain(`invalid ${operation}: ${field}: `);
    },
  );
  it(
    "refuses plain text that is not well-formed Unicode",
    refusesIllFormedText,
  );
});

function readsPlainText() {
  const withoutOperationKey = fc
    .dictionary(fc.string(), fc.jsonValue())
    .filter((value) => !OPERATION_KEYS.some((key) => Object.hasOwn(value, key)))
    .map((value) => JSON.stringify(value));
  fc.assert(
    fc.property(fc.oneof(fc.string(), withoutOperationKey), (text) => {
      fc.pre(!startsAnOperation(text));
      expect(parsed(text)).toEqual({ to: group, text });
    }),
  );
}

function readsQuestions() {
  fc.assert(
    fc.property(
      fc.constantFrom("gather" as const, "all_gather" as const),
      fc.string(),
      fc.integer({ min: 1, max: MAXIMUM_DEADLINE_SECONDS }),
      fc.boolean(),
      (op, question, deadline, padded) => {
        const json = JSON.stringify({
          [op]: question,
          deadline,
          requestedSchema: slotSchema,
        });

        expect(parsed(padded ? `\n  ${json}\n` : json)).toEqual({
          to: group,
          text: question,
          collective: { op, deadline, requestedSchema: slotSchema },
        });
      },
    ),
  );
}

function readsAnswers() {
  expect(
    parsed(
      JSON.stringify({
        operationId: requestId,
        action: "accept",
        content: { slot: "mon" },
      }),
    ),
  ).toEqual({
    to: group,
    collectiveResponse: {
      operationId: requestId,
      action: "accept",
      content: { slot: "mon" },
    },
  });
  expect(
    parsed(JSON.stringify({ operationId: requestId, action: "decline" })),
  ).toEqual({
    to: group,
    collectiveResponse: { operationId: requestId, action: "decline" },
  });
}

function refusesIllFormedText() {
  const error = refusal("broken \ud800 text");

  expect(error.message).toMatch(/^send failed: invalid message: /u);
}

function parsed(text: string) {
  return Either.getOrThrow(parseMessageText(group, text));
}

function refusal(text: string): MessageTextError {
  return Either.match(parseMessageText(group, text), {
    onLeft: (error) => error,
    onRight: (input) => {
      throw new Error(`expected a refusal, read ${JSON.stringify(input)}`);
    },
  });
}

/** Whether a generated string happens to be a JSON object with an operation key. */
function startsAnOperation(text: string): boolean {
  return Either.match(
    Schema.decodeUnknownEither(
      Schema.parseJson(
        Schema.Record({ key: Schema.String, value: Schema.Unknown }),
      ),
    )(text),
    {
      onLeft: () => false,
      onRight: (value) =>
        OPERATION_KEYS.some((key) => Object.hasOwn(value, key)),
    },
  );
}
