/** @file Questions and result sources retain the messages selected by one collective operation. */

import { Clock, Effect, Option, Schema } from "effect";
import { exactStruct } from "../../encoding/exact.js";
import { compareAscii } from "../../encoding/identifier.js";
import { AgentCard } from "../../identity/agent.js";
import { CommunicationError } from "../errors.js";
import { Message, MessageParticipants } from "../message.js";
import { maximumMembers, wellFormedString } from "../records/values.js";
import { DeliveryToken } from "../store/types.js";
import {
  encodeCollectiveContent,
  mintCollectiveId,
  RequestedSchema,
  validateAnswer,
} from "./answers.js";
import {
  AnswerContent,
  CollectiveId,
  DeadlineSeconds,
  EpochMillis,
  RequestError,
  RequestOperation,
} from "./contracts.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their names with their decoded types. */

/** The operation is captured before assessment, including its absolute deadline and participants. */
const requestSchema = MessageParticipants.pipe(
  Schema.extend(
    exactStruct({
      kind: Schema.Literal("request"),
      operationId: CollectiveId,
      op: RequestOperation,
      question: wellFormedString,
      requestedSchema: RequestedSchema,
      deadlineAt: EpochMillis,
    }),
  ),
);
/** A gather sends a separate request to each recipient; an all_gather shares one group request. */
export type Request = typeof requestSchema.Type;

/** Captures a complete question and its deadline before policy assessment. */
export const Request = Object.assign(requestSchema, {
  create(
    input: Pick<Request, "author" | "recipients" | "question" | "op"> & {
      readonly requestedSchema: RequestedSchema;
      readonly deadline: number;
    },
  ) {
    return Effect.gen(function* () {
      const requestedSchema = input.requestedSchema;
      const seconds = yield* Schema.decodeUnknown(DeadlineSeconds)(
        input.deadline,
      );
      const request = yield* Schema.validate(requestSchema)({
        author: input.author,
        question: input.question,
        op: input.op,
        recipients: [...input.recipients].sort((left, right) =>
          compareAscii(left.agentName, right.agentName),
        ),
        kind: "request",
        operationId: yield* mintCollectiveId(input.author.agentId),
        requestedSchema,
        deadlineAt: (yield* Clock.currentTimeMillis) + seconds * 1000,
      });
      yield* encodeCollectiveContent(
        {
          kind: "operation",
          op: request.op,
          operationId: request.operationId,
          deadlineAt: request.deadlineAt,
          requestedSchema: request.requestedSchema,
        },
        request.question,
      );
      if (request.op === "all_gather" && request.recipients.length < 2) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
      return request;
    }).pipe(
      Effect.withSpan("Request.create"),
      Effect.catchTags({
        ParseError: () => new CommunicationError({ reason: "content-invalid" }),
        CollectiveContentError: () =>
          new CommunicationError({ reason: "content-invalid" }),
      }),
    );
  },
});

/** A received request retains the ordinary message and its decoded meaning. */
export const ReceivedRequest = Message.pipe(
  Schema.filter(
    (
      message,
    ): message is Message & {
      readonly operation: Extract<
        NonNullable<Message["operation"]>,
        { readonly kind: "request" }
      >;
    } => message.operation?.kind === "request",
  ),
);
/** A request Message exposes a complete decoded response form. */
export type ReceivedRequest = typeof ReceivedRequest.Type;

/** Request narrowing preserves the Message and its owning decoder's private classification. */
export function isReceivedRequest(
  message: Message,
): message is ReceivedRequest {
  return message.operation?.kind === "request";
}

/** A member contributes one selected reply or a final absence. */
// eslint-disable-next-line agent-code-guard/no-exported-brand-constructor -- Firewall extends complete raw outcomes with its local disclosure decision.
export const AnswerOutcome = Schema.Union(
  exactStruct({ kind: Schema.Literal("answered"), content: AnswerContent }),
  exactStruct({ kind: Schema.Literal("declined") }),
  exactStruct({ kind: Schema.Literal("invalid"), reason: Schema.String }),
  exactStruct({ kind: Schema.Literal("no-answer") }),
);
/** A complete outcome precedes local disclosure policy. */
export type AnswerOutcome = typeof AnswerOutcome.Type;

/** Complete request outcomes remain independent of policy or host addressing. */
export const RequestResult = exactStruct({
  request: requestSchema,
  closedAt: Schema.Int.pipe(Schema.nonNegative()),
  closedBy: Schema.Literal("settled", "deadline"),
  outcomes: Schema.NonEmptyArray(
    exactStruct({ member: AgentCard, outcome: AnswerOutcome }),
  ),
});
/** Every original respondent contributes one selected outcome. */
export type RequestResult = typeof RequestResult.Type;

/** A decline contains no form data. */
export type AnswerInput =
  | {
      readonly action: "accept";
      readonly content: AnswerContent;
    }
  | { readonly action: "decline" };

/** Validates a reply and fixes its recipients and message identity before assessment. */
export const Answer = {
  create(input: {
    readonly author: Message["author"];
    readonly request: ReceivedRequest;
    readonly response: AnswerInput;
  }) {
    return Effect.gen(function* () {
      if (
        !input.request.recipients.some(
          (member) => member.agentId === input.author.agentId,
        )
      ) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
      if (input.response.action === "accept") {
        const fields = yield* validateAnswer(
          input.request.operation.requestedSchema,
          input.response.content,
        ).pipe(Effect.flip, Effect.option);
        if (Option.isSome(fields)) {
          return yield* new RequestError({
            operationId: input.request.operation.operationId,
            failure: { kind: "answer-invalid", fields: fields.value },
          });
        }
      }
      const members = [input.request.author, ...input.request.recipients];
      const [first, ...rest] = members.filter(
        (member) => member.agentId !== input.author.agentId,
      );
      if (first === undefined) {
        return yield* new CommunicationError({ reason: "membership-invalid" });
      }
      return yield* Message.create({
        author: input.author,
        recipients: [first, ...rest],
        content: yield* encodeCollectiveContent({
          kind: "response",
          operationId: input.request.operation.operationId,
          ...input.response,
        }),
      });
    }).pipe(
      Effect.withSpan("Answer.create"),
      Effect.catchTag(
        "CollectiveContentError",
        () => new CommunicationError({ reason: "content-invalid" }),
      ),
    );
  },
};

/** Original request and selected answer messages supply the peer-controlled result text. */
export const ResultSources = exactStruct({
  answers: Schema.Array(Message).pipe(Schema.maxItems(maximumMembers - 1)),
  request: Schema.optionalWith(ReceivedRequest, { exact: true }),
});
/** Own answers remain sources but do not need an inbound policy check. */
export type ResultSources = typeof ResultSources.Type;

/** The item and its sources use one durable result identity through retries and restart. */
export const ResultDelivery = exactStruct({
  operationId: CollectiveId,
  deliveryToken: DeliveryToken,
  item: Schema.Union(
    exactStruct({ kind: Schema.Literal("result"), result: RequestResult }),
    exactStruct({
      kind: Schema.Literal("request-failed"),
      request: requestSchema,
      error: Schema.String,
    }),
  ),
  sources: ResultSources,
});
/** Public item bytes and private source bytes are bounded separately. */
export type ResultDelivery = typeof ResultDelivery.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after schema/type pairs. */
