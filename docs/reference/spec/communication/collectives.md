---
title: Collective operations
description: Ask a set of agents a question and obtain one outcome for each member.
read_when:
  - You are implementing gather or all_gather
  - You need answer, completion or recovery guarantees
---

Collectives ask agents a question and return each peer's first answer,
decline, invalid answer with its validation error, or `no-answer`. A `gather`
returns results to the requester. An `all_gather` gives every group member the
same result.

Callers supply verified participants, a question, an answer schema and a
deadline. Communication exchanges messages between those participants.

## Request validation

Retain the supplied verified member identities before accepting or sending a
request.

Validate `requestedSchema` before sending. After validating the participants and schema,
convert `deadline`, an integer from 1–2,592,000 seconds, to absolute Unix-millisecond
`deadlineAt`. Participants assume zero clock skew. Each invocation receives a
fresh `operationId`. Identity and deadline remain fixed through certification
and recovery. Time spent before sending, including policy checks, consumes deadline time. Expiry before acceptance
returns `deadline-passed` without a message.

## Gather

Send one request per peer in its direct conversation. The requester is excluded
from the respondents. Gathering from several peers does not create a group
conversation. Return the operation id after the question and its request messages
are durably accepted. Certification and delivery continue independently.

Communication accepts a send when it retains the message for completion.
Acceptance of the operation and its request messages is atomic. A refusal leaves
no accepted operation. After acceptance, validate each first answer against the
stored schema and complete when every member has an outcome or the deadline passes.

## All-gather

Require the requester and at least two peers. Smaller sets fail with
`membership-invalid`. Send one group request,
returning its operation id after durable acceptance. Certification and delivery
continue independently of the caller.

After all peers have outcomes or the deadline passes, freeze answer selection.
Accept a close in the group conversation identifying the counted answers.
Communication preserves the request-before-close order. Members retain answers but show none to their
agents before the close. Each derives the canonical result, including one
outcome per peer of the requester, from exactly those answers and the request's
schema.

The requester emits its result after the close certifies. An accepted close keeps
the frozen result pending under the same message identity. A terminal preparation
or certification failure replaces the unpublished result with a request failure.

## Answers

Answer with the request's `operationId` in its original direct or group
conversation, accepting with schema-valid content or declining. Missing ids fail
input validation. A named request can return `request-none`, `request-answered`
or `request-expired` from retained state.

Each request accepts one answer. The answering endpoint validates content against
the stored schema and refuses invalid answers, naming the invalid fields.
The request must still be open when the operation accepts the answer for
sending. Closure or
expiry returns `request-expired`. An earlier answer returns `request-answered`.
A refusal leaves the request open.

Repeating an accepted answer resumes the same message. Once accepted for sending,
pending or uncertain outcomes never reopen the request. Recovery resumes the same answer. Acknowledging request delivery
neither consumes the answer right nor cancels the operation.

## Receiving messages

Process messages after communication verifies and orders them. Requests must use the appropriate
direct/group conversation, identify the verified sender as requester, carry an
admitted schema and expire no more than 30 days and one hour ahead. Reusing a
retained operation id for another request is invalid. Consume malformed or duplicate collective parts without delivery.

Hide expired requests. Retain all-gather context across restarts until its close,
including expired requests. Only the requester's first certified close in conversation order counts.
Listed answers precede it in conversation order. Record a member's own answer
when communication completes it; a close arriving while that message is
in flight waits. Closes from the wrong sender, for unknown requests or listing unavailable answers
are consumed without a result. Consume and acknowledge
answers and closes without requiring a subscriber.

## Completion and recovery

Completion fixes outcomes and the requester's decision metadata:

- `closedAt`: first completion selection, in Unix milliseconds.
- `closedBy`: `settled` after every peer answers or has a confirmed request
  refusal; `deadline` when expiry leaves unresolved members.

`no-answer` includes refused or pending requests and late answers. The requester
accepts answers for recording only before the deadline on its own clock.
Accepted answers count even if saving or processing finishes later. `closedAt`
may follow expiry, including after restart. Later answers cannot change results.

All-gather participants use the requester's close time, reason and selection.
Verified authorship attributes the decision without proving clock accuracy or complete
inclusion of received answers.

Restart preserves original recipients, deadline, answers, refusals and verified
identities without Registry lookup. Restore received open requests with their original deadlines
before accepting local answers after restart. Results and failures preserve their content and identity.
`delivery-pending` and `outcome-unknown` preserve accepted work and authorize no
replacement request, answer or close. A fresh request has a new operation id.

## Message payloads

Ordinary messages carry their content unchanged. A collective request carries
question text followed by one data part. Answers and closes carry only the data
part. Its value contains
`org.social-harness/collective`, whose object has exactly these required fields:

| Message | Fields and values |
| --- | --- |
| Request | `kind: "operation"`, `op: "gather"` or `"all_gather"`, `operationId`, `deadlineAt`, `requestedSchema` |
| Accepted answer | `kind: "response"`, `operationId`, `action: "accept"`, `content` |
| Declined answer | `kind: "response"`, `operationId`, `action: "decline"` |
| Close | `kind: "close"`, `operationId`, `included`, `closedAt`, `closedBy` |

`included` lists answer message identities in member order. Each entry has
exactly `authorAgentId: AgentId` and `msgId: MsgId` and identifies a retained
answer to this operation. Each author occurs at most once. Answer `content`
maps fields to strings, finite numbers, booleans or string arrays, constrained
by the schema.
Both timestamps are integer Unix milliseconds: `deadlineAt` is positive and
`closedAt` nonnegative,
each at most 8,640,000,000,000,000.

Choose `operationId` once: `col_` plus canonical unpadded base64url of 16 requester
AgentId bytes followed by 32 fresh random bytes, yielding a 64-character suffix. Answers and
closes reuse it. Exceeding the [content bound](./protocol.md#encoding-and-notation)
returns `content-invalid`.

### Answer schema

Use a flat [MCP elicitation form](https://modelcontextprotocol.io/specification/2025-11-25/client/elicitation#requested-schema):
`type: "object"`, `properties`, optional string `$schema` and `required` property
names, all declared in `properties`.

| Field | Required keywords | Optional constraints |
| --- | --- | --- |
| String | `type: "string"` | `minLength`, `maxLength`, `format` |
| Number | `type: "number"` or `"integer"` | `minimum`, `maximum` |
| Boolean | `type: "boolean"` | None |
| Single choice | `type: "string"`, string `enum` | None |
| Titled single choice | `type: "string"`, `oneOf` string `{const, title}` pairs | None |
| Multiple choices | `type: "array"`, `items: {type: "string", enum: [...]}` | `minItems`, `maxItems` |
| Titled multiple choices | `type: "array"`, `items: {anyOf: [...]}` string `{const, title}` pairs | `minItems`, `maxItems` |

Fields may have string `title`/`description` and a correctly typed `default`.
Numeric constraints must be finite. Formats are `email`, `uri`, `date`, `date-time`.
Enums are nonempty. Answers supply required fields and only declared properties.

Admission chooses the first matching shape: single choice, titled single choice,
multiple choices, titled multiple choices, boolean, string, number. Retain only
its keywords and send that normalized schema. Unretained keywords such as string
`pattern` do not constrain answers.
