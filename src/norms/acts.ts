/**
 * @file FIPA ACL acts in their string representation (FIPA SC00070): a
 * parenthesised performative followed by `:parameter value` pairs, read by
 * the `@thi.ng/sexpr` parser. Text whose first token is not a performative is
 * plain text, which the `chat` norm reads as an `inform`.
 */

import type { ASTNode, Expression } from "@thi.ng/sexpr/api";
import { parse } from "@thi.ng/sexpr/parse";
import { Either } from "effect";
import type { Outgoing } from "../communication/service.js";

/** The FIPA ACL communicative acts (FIPA SC00037). */
const performatives = new Set([
  "accept-proposal",
  "agree",
  "cancel",
  "cfp",
  "confirm",
  "disconfirm",
  "failure",
  "inform",
  "inform-if",
  "inform-ref",
  "not-understood",
  "propagate",
  "propose",
  "proxy",
  "query-if",
  "query-ref",
  "refuse",
  "reject-proposal",
  "request",
  "request-when",
  "request-whenever",
  "subscribe",
]);

/** A field value: text, a number, or the items a multi-select field takes. */
export type ActValue = string | number | readonly string[];

/**
 * One act. `ontology` names the norm it follows and `inReplyTo` the message it
 * answers. The conversation the act is sent in identifies its agreement or
 * norm instance, so an act carries no conversation id. Every other parameter
 * is a content field, which the norm's form for this performative checks.
 */
export interface Act {
  readonly performative: string;
  readonly ontology: string;
  readonly inReplyTo?: string;
  readonly content?: string;
  readonly fields: Readonly<Record<string, ActValue>>;
}

/** What one message says: an act, plain text, or an act that does not parse. */
export type Reading =
  | { readonly kind: "act"; readonly act: Act }
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "invalid"; readonly error: string };

const parameterName = /^[a-z][a-z0-9-]{0,63}$/u;

/**
 * The text a norm reads in a message: its text parts, in order. A collective
 * request, answer or data part is not read by norms.
 */
export function messageText(message: Outgoing): string | undefined {
  if ("kind" in message || message.operation !== undefined) {
    return undefined;
  }
  const parts = message.content.flatMap((part) =>
    part.type === "text" ? [part.text] : [],
  );
  return parts.length === message.content.length ? parts.join("\n") : undefined;
}

/** Reads one message text as an act, plain text or an act that fails to parse. */
export function readText(text: string): Reading {
  const opening = /^\s*\(\s*([^\s()";]+)/u.exec(text);
  if (opening?.[1] === undefined || !performatives.has(opening[1])) {
    return { kind: "text", text };
  }
  return Either.match(readAct(text), {
    onLeft: (error) => ({ kind: "invalid", error }),
    onRight: (act) => ({ kind: "act", act }),
  });
}

/**
 * Writes an act in the form {@link readText} reads. Text holding a backslash
 * followed by `u` or `U` and hex digits does not read back unchanged, because
 * the parser decodes that sequence as a Unicode escape.
 */
export function formatAct(act: Act): string {
  const ontology = parameterName.test(act.ontology)
    ? act.ontology
    : formatValue(act.ontology);
  const parameters: Array<readonly [string, ActValue]> = [
    ...(act.inReplyTo === undefined
      ? []
      : [["in-reply-to", act.inReplyTo] as const]),
    ...(act.content === undefined ? [] : [["content", act.content] as const]),
    ...Object.entries(act.fields),
  ];
  const written = parameters.flatMap(([name, value]) => [
    `:${name}`,
    formatValue(value),
  ]);
  return `(${[act.performative, ":ontology", ontology, ...written].join(" ")})`;
}

function readAct(text: string): Either.Either<Act, string> {
  const tree = Either.try({
    try: () => parse(text),
    catch: (error) =>
      error instanceof Error ? error.message : "the act does not parse",
  });
  return Either.flatMap(tree, (root) => {
    const [form, ...rest] = root.children;
    return form?.type === "expr" && form.value === "(" && rest.length === 0
      ? actOf(form)
      : Either.left("a message holds exactly one act in parentheses");
  });
}

function actOf(form: Expression): Either.Either<Act, string> {
  const [head, ...pairs] = form.children;
  if (head?.type !== "sym" || !performatives.has(head.value)) {
    return Either.left("an act starts with a FIPA performative");
  }
  if (pairs.length % 2 !== 0) {
    return Either.left("every parameter needs one value");
  }
  const parameters = new Map<string, ActValue>();
  for (let index = 0; index < pairs.length; index += 2) {
    const name = keyword(pairs[index]);
    const value = valueOf(pairs[index + 1]);
    if (name === undefined || parameters.has(name)) {
      return Either.left(
        "parameters are distinct :names such as :ontology or :in-reply-to",
      );
    }
    if (value === undefined) {
      return Either.left(`:${name} needs a string, number or list of items`);
    }
    parameters.set(name, value);
  }
  return envelope(head.value, parameters);
}

/** Separates the FIPA envelope parameters from the content fields. */
function envelope(
  performative: string,
  parameters: ReadonlyMap<string, ActValue>,
): Either.Either<Act, string> {
  const text = (name: string) => {
    const value = parameters.get(name);
    return typeof value === "number" ? String(value) : value;
  };
  const ontology = text("ontology");
  const inReplyTo = text("in-reply-to");
  const content = text("content");
  if (typeof ontology !== "string" || !parameterName.test(ontology)) {
    return Either.left(":ontology names the norm the act follows");
  }
  if (
    [inReplyTo, content].some(
      (value) => value !== undefined && typeof value !== "string",
    )
  ) {
    return Either.left(":in-reply-to and :content each take one value");
  }
  const fields = Object.fromEntries(
    [...parameters].filter(
      ([name]) => !["ontology", "in-reply-to", "content"].includes(name),
    ),
  );
  return Either.right({
    performative,
    ontology,
    ...(typeof inReplyTo === "string" ? { inReplyTo } : {}),
    ...(typeof content === "string" ? { content } : {}),
    fields,
  });
}

function keyword(node?: ASTNode): string | undefined {
  if (node?.type !== "sym" || !node.value.startsWith(":")) {
    return undefined;
  }
  const name = node.value.slice(1);
  return parameterName.test(name) ? name : undefined;
}

/** A string, a number or a parenthesised list of them, which reads as strings. */
function valueOf(node?: ASTNode): ActValue | undefined {
  if (node?.type === "str" || node?.type === "sym" || node?.type === "num") {
    return node.value;
  }
  if (node?.type !== "expr" || node.value !== "(") {
    return undefined;
  }
  const items = node.children.flatMap((child) =>
    child.type === "str" || child.type === "sym" || child.type === "num"
      ? [String(child.value)]
      : [],
  );
  return items.length === node.children.length ? items : undefined;
}

/**
 * Quotes text as JSON does, which the parser's unescaping reverses. The
 * parser's tokenizer takes a backslash just before a quote as escaping it, so
 * a final backslash is written as a Unicode escape instead.
 */
function formatValue(value: ActValue): string {
  if (typeof value === "number") {
    return String(value);
  }
  if (typeof value === "string") {
    const quoted = JSON.stringify(value);
    return quoted.endsWith('\\\\"') ? `${quoted.slice(0, -3)}\\u005c"` : quoted;
  }
  return `(${value.map((item) => formatValue(item)).join(" ")})`;
}
