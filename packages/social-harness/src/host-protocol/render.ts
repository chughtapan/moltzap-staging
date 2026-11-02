/** @file The text a host's model reads for a delivered message, question or result, the same on every host. */

import type { Message, Request } from "../communication/index.js";
import type { PermittedItem } from "../firewall/service.js";
import {
  agentAddressOf,
  type Content,
  GroupAddress,
  type MessageAddressInput,
} from "../communication/records/values.js";
import { compareAscii } from "../encoding/identifier.js";
import { type LocalGuidance, renderLocalGuidance } from "./advice.js";

type CollectiveRequestItem = Extract<
  PermittedItem,
  { readonly kind: "collectiveRequest" }
>;
type CollectiveResultItem = Extract<
  PermittedItem,
  { readonly kind: "collectiveResult" }
>;
type MemberOutcome = CollectiveResultItem["outcomes"][number]["outcome"];

/**
 * A message's content as the text a model reads: each text part as written,
 * each data part as JSON, one part per line.
 * @param content The delivered message's content.
 * @returns The text a host puts in the model's turn.
 */
export function renderContent(content: Content): string {
  return content
    .map((part) =>
      part.type === "text" ? part.text : (JSON.stringify(part.value) ?? "null"),
    )
    .join("\n");
}

/**
 * A request message's question as every member's model reads it: its text
 * parts, one per line. The collective part is a data part, so it is never
 * among them.
 * @param content The request message's content.
 * @returns The text parts, joined by newlines.
 */
export function questionText(content: Content): string {
  return content
    .flatMap((part) => (part.type === "text" ? [part.text] : []))
    .join("\n");
}

/**
 * The agents a group address names, as `agent:` addresses, in its order; a
 * direct address names none. Hosts list them on a group turn.
 * @param address A canonical address an item carries.
 * @returns The group's member addresses, or an empty list.
 */
export function groupMembers(address: MessageAddressInput): readonly string[] {
  return address.startsWith("group:")
    ? address
        .slice("group:".length)
        .split(",")
        .map((name) => `agent:${name}`)
    : [];
}

/**
 * A question another agent asked: who asked and until when, the question,
 * its form, and the exact text that answers it, naming the question's operationId so
 * the answer reaches it when several questions are open in the conversation.
 * Only the last line differs by host, since each host's model sends the
 * answer through its own tool.
 * @param item The delivered question.
 * @param howToAnswer The host's sentence naming the tool that sends the answer to its conversation.
 * @param guidance Optional local instructions accompanying the peer question.
 * @returns The text of the model's turn.
 */
export function renderCollectiveRequest(
  item: CollectiveRequestItem,
  howToAnswer: string,
  guidance?: LocalGuidance,
): string {
  const body = renderCollectiveRequestBody(item);
  const operationId = JSON.stringify(item.message.operation.operationId);
  const answerInstructions = [
    `Answer with: {"action":"accept","operationId":${operationId},"content":{...}} where content matches the form, or {"action":"decline","operationId":${operationId}}`,
    howToAnswer,
  ];
  if (guidance === undefined) {
    return [body, ...answerInstructions].join("\n");
  }
  return [
    renderLocalGuidance(guidance, { text: body, origin: "peer" }),
    ...answerInstructions,
  ].join("\n");
}

/**
 * A question another agent asked: who asked and until when, the question and
 * its form. The firewall checks exactly this text, which the model reads
 * before the answer instructions `renderCollectiveRequest` adds.
 * @param item The delivered question.
 * @returns The question's lines of the model's turn.
 */
export function renderCollectiveRequestBody(
  item: CollectiveRequestItem,
): string {
  return renderCollectiveQuestion({
    author: item.message.author,
    recipients: item.message.recipients,
    ...item.message.operation,
  });
}

/** Incoming and outgoing assessment use the same question text and form. */
export function renderCollectiveQuestion(
  question: Pick<
    Request,
    | "author"
    | "recipients"
    | "question"
    | "op"
    | "deadlineAt"
    | "requestedSchema"
  >,
): string {
  const asked =
    question.op === "all_gather"
      ? `all_gather from agent:${question.author.agentName} to ${replyAddress(question)}`
      : `gather from agent:${question.author.agentName}`;
  return [
    `${asked}, open until ${new Date(question.deadlineAt).toISOString()}.`,
    `Question: ${question.question}`,
    `Form: ${JSON.stringify(question.requestedSchema)}`,
  ].join("\n");
}

/**
 * A gather's or all_gather's result: the question and one line per member.
 * The question is quoted as JSON, so a question a peer asked cannot add
 * lines that read as other members' outcomes.
 * @param item The delivered result.
 * @returns The text of the model's turn.
 */
export function renderCollectiveResult(item: CollectiveResultItem): string {
  const question =
    typeof item.question === "string"
      ? JSON.stringify(item.question)
      : `(withheld: ${renderWithheld(item.question)})`;
  return [
    `${item.op} result: ${question}`,
    ...item.outcomes.map(
      ({ member, outcome }) =>
        `- agent:${member.agentName}: ${renderOutcome(outcome)}`,
    ),
  ].join("\n");
}

/** Direct replies reach the author; group replies keep the complete participant set. */
export function replyAddress(message: Pick<Message, "author" | "recipients">) {
  return message.recipients.length === 1
    ? agentAddressOf(message.author)
    : GroupAddress.make(
        `group:${[message.author, ...message.recipients]
          .map((member) => member.agentName)
          .sort(compareAscii)
          .join(",")}`,
      );
}

/**
 * One member's outcome as its line of a result reads it. The firewall checks
 * an answer as exactly this text.
 * @param outcome The member's outcome.
 * @returns The outcome's text.
 */
export function renderOutcome(outcome: MemberOutcome): string {
  switch (outcome.kind) {
    case "answered":
      return `answered ${JSON.stringify(outcome.content)}`;
    case "declined":
      return "declined";
    case "invalid":
      return `answered outside the form (${outcome.reason})`;
    case "no-answer":
      return "no answer";
    case "withheld":
      return `answer withheld: ${renderWithheld(outcome)}`;
    default: {
      const exhaustive: never = outcome;
      return exhaustive;
    }
  }
}

function renderWithheld(
  withheld: Extract<MemberOutcome, { readonly kind: "withheld" }>,
): string {
  return `local policy held it (${withheld.reason}, check ${withheld.checkId})`;
}
