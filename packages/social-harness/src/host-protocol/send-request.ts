/** @file MCP's message, question and explicit-answer argument grammar. */
import { Schema } from "effect";
import { RequestedSchema } from "../communication/index.js";
import {
  MessageAddressInput,
  wellFormedString,
} from "../communication/records/values.js";
import {
  AcceptResponse,
  DeadlineSeconds,
  DeclineResponse,
  describeIssues,
} from "../communication/requests/contracts.js";
import { exactStruct } from "../encoding/exact.js";

const multicast = exactStruct({
  op: Schema.optionalWith(Schema.Literal("multicast"), { exact: true }),
});
const question = exactStruct({
  op: Schema.Literal("gather", "all_gather"),
  deadline: DeadlineSeconds,
  requestedSchema: RequestedSchema,
});
const response = Schema.Union(AcceptResponse, DeclineResponse);

/** An explicit answer always names the request it answers. */
export type CollectiveResponse = typeof response.Type;
/** An ordinary message may explicitly select multicast. */
export const MessageSendRequest = exactStruct({
  to: MessageAddressInput,
  text: wellFormedString,
  collective: Schema.optionalWith(multicast, { exact: true }),
});
/** A question supplies its response form and relative deadline together. */
export const QuestionSendRequest = exactStruct({
  to: MessageAddressInput,
  text: wellFormedString,
  collective: question,
});
/** An answer names the request it addresses. */
export const AnswerSendRequest = exactStruct({
  to: MessageAddressInput,
  collectiveResponse: response,
});
/** Host addresses are resolved by SocialHarness before communication preparation. */
export const SendRequest = Schema.Union(
  MessageSendRequest,
  QuestionSendRequest,
  AnswerSendRequest,
).annotations({ identifier: "SendRequest" });
/** Question deadlines include policy assessment time. */
export type SendRequest = typeof SendRequest.Type;
/** Form grammar and bounded field diagnostics have one owning implementation. */
export {
  AcceptResponse,
  DeadlineSeconds,
  DeclineResponse,
  describeIssues,
  RequestedSchema,
};
