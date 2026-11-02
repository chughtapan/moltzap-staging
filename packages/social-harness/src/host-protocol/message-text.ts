/**
 * @file Reads the operation a message's text states.
 *
 * In native hosts, a model reaches every operation through the ordinary
 * message tool, as message text. The OpenClaw and NanoClaw adapters read it
 * with {@link parseMessageText}, so both hosts accept the same text:
 *
 * ```
 * plain text                                                   multicast
 * {"gather": "Which day?", "deadline": 600, "requestedSchema": {...}}
 * {"all_gather": "Which day?", "deadline": 600, "requestedSchema": {...}}
 * {"action": "accept", "operationId": "col_...", "content": {...}}       answer
 * {"action": "decline", "operationId": "col_..."}                        answer
 * ```
 *
 * Only a whole text that is a JSON object with a `gather`, `all_gather` or
 * `action` key states an operation; any other text, JSON-looking prose
 * included, is a multicast of that text. A text that states an operation but
 * fails validation is refused, never sent as text, because the model meant
 * the operation and the error names the field to fix.
 */

import { Data, Either, Option, Predicate, Schema } from "effect";
import type { MessageAddressInput } from "../communication/records/values.js";
import {
  AnswerSendRequest,
  MessageSendRequest,
  QuestionSendRequest,
  type SendRequest,
} from "./send-request.js";
import { validationIssues } from "./validation-errors.js";

/** The operations that ask a question, in the order their keys are read. */
const COLLECTING_OPERATIONS = ["gather", "all_gather"] as const;

type CollectingOperation = (typeof COLLECTING_OPERATIONS)[number];

/**
 * Which operation a refused text stated.
 * @inline
 */
type StatedOperation = "message" | CollectingOperation | "answer";

/**
 * A message text states an operation it does not validly carry, or is plain
 * text that is not well-formed Unicode. The message names each failing field
 * so a host can hand it to its model as the tool error.
 */
export class MessageTextError extends Data.TaggedError("MessageTextError")<{
  readonly operation: StatedOperation;
  readonly detail: string;
}> {
  override get message(): string {
    const action = this.operation === "answer" ? "reply" : "send";
    return `${action} failed: invalid ${this.operation}: ${this.detail}`;
  }
}

const decodeJsonText = Schema.decodeUnknownOption(
  Schema.parseJson(Schema.Unknown),
);

/** The JSON object a whole text is, if it is one. */
const jsonObject = (text: string) =>
  decodeJsonText(text).pipe(Option.filter(Predicate.isRecord));

/**
 * Read one message text as the send it states.
 * @param to The address the host's tool call names: the recipients of a
 *   multicast, gather or all_gather, or for an answer the conversation whose
 *   open request it answers.
 * @param text The whole text of the message.
 * @returns The send input, or an error naming each field a stated operation
 *   gets wrong.
 */
export const parseMessageText = (
  to: MessageAddressInput,
  text: string,
): Either.Either<SendRequest, MessageTextError> =>
  Option.match(jsonObject(text), {
    onNone: () => sendInput("message", { to, text }),
    onSome: (value) =>
      statedOperation(to, value) ?? sendInput("message", { to, text }),
  });

/**
 * The operation a JSON object states by its key, or undefined when it has
 * none of `gather`, `all_gather` and `action` and so is plain text.
 */
function statedOperation(
  to: MessageAddressInput,
  value: Readonly<Record<string, unknown>>,
): Either.Either<SendRequest, MessageTextError> | undefined {
  const op = COLLECTING_OPERATIONS.find((key) => Object.hasOwn(value, key));
  if (op !== undefined) {
    const { [op]: question, ...details } = value;
    return sendInput(op, {
      to,
      text: question,
      collective: { ...details, op },
    });
  }
  return Object.hasOwn(value, "action")
    ? sendInput("answer", { to, collectiveResponse: value })
    : undefined;
}

function sendInput(
  operation: StatedOperation,
  value: unknown,
): Either.Either<SendRequest, MessageTextError> {
  switch (operation) {
    case "message":
      return decodeText(operation, MessageSendRequest, value);
    case "answer":
      return decodeText(operation, AnswerSendRequest, value);
    case "gather":
    case "all_gather":
      return decodeText(operation, QuestionSendRequest, value);
    default:
      return operation satisfies never;
  }
}

function decodeText<A, I>(
  operation: StatedOperation,
  schema: Schema.Schema<A, I>,
  value: unknown,
): Either.Either<A, MessageTextError> {
  return Schema.decodeUnknownEither(schema, { errors: "all" })(value).pipe(
    Either.mapLeft(
      (error) =>
        new MessageTextError({
          operation,
          detail: validationIssues(error)
            .map(({ path, message }) => {
              const nativePath = fieldPath(operation, path);
              return `${nativePath.join(".")}: ${message}`;
            })
            .join("; "),
        }),
    ),
  );
}

function fieldPath(
  operation: StatedOperation,
  path: ReadonlyArray<string | number>,
) {
  const [first, ...rest] = path;
  if (first === "collective" || first === "collectiveResponse") {
    return rest;
  }
  if (first === "text" && operation !== "message") {
    return [operation];
  }
  return path;
}
