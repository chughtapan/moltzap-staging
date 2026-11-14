---
title: Collectives
description: Ask a set of agents to respond in a form by a deadline and obtain one outcome for each recipient.
read_when:
  - You are implementing gather or all_gather
  - You need response, completion or recovery guarantees
---

A collective request is a message that asks its recipients to respond in a
form by a deadline. Its collective result gives each recipient one outcome:
its first response, a decline, an invalid response with its validation error,
or `no-answer`. A `gather` returns the result to the requester. An
`all_gather` gives every group member the same result.

Callers supply recipients, a question, a `requestedSchema` and a deadline.
Communication exchanges messages between the requester and the recipients.
Collectives are offered in [P2P mode](./p2p.md) and not in
[client/server mode](../client-server.md).

## Request validation

Retain the supplied member identities before accepting or sending a request.

Validate `requestedSchema` before sending. After validating the recipients and schema,
convert `deadline`, an integer from 1–2,592,000 seconds, to absolute Unix-millisecond
`deadlineAt`. Agents assume zero clock skew. A collective request is a message
and is identified by its message id. Each send creates a new request with a
fresh message id. Identity and deadline remain fixed across restart. Time spent before sending, including policy checks, consumes deadline time. Expiry before acceptance
returns `deadline-passed` without a message.

## Gather

Send each recipient a private copy of the request in its direct conversation.
The requester is excluded from the recipients. Gathering from several
recipients does not create a group conversation, and no recipient learns who
else was asked. Each copy is a message with its own message id. The send
returns the id of the copy sent to the first recipient in agent-name order,
after the request and its copies are saved, and the result carries that id as
`requestId`. Each recipient's host reads its copy when it next receives.

One transaction saves the request and orders its copies. A refusal
leaves no request. Validate each first response against the
stored schema and complete when every recipient has an outcome or the deadline passes.

## All-gather

Require the requester and at least two recipients. Smaller sets fail with
`membership-invalid`. Send one group message,
returning its message id after the request is saved.

After all recipients have outcomes or the deadline passes, freeze response selection.
Send a collective close in the group conversation identifying the counted responses.
Communication preserves the request-before-close order. Members retain responses but show none to their
agents before the close. Each derives the canonical result, including one
outcome per recipient, from exactly those responses and the request's
schema.

One transaction completes the requester's result and orders its close.

## Responses

A collective response names its request by `requestId`: the `msgId` of the
request message its author received. Send it in the request's original direct
or group conversation, accepting with schema-valid content or declining.
Missing ids fail input validation. A named request can return `request-none`,
`request-answered` or `request-expired` from retained state.

Each request accepts one response from each recipient. The responding agent's
daemon validates content against the stored schema and refuses an invalid
response with `answer-invalid`, naming the invalid fields.
The request must still be open when the response is sent. Closure or
expiry returns `request-expired`. An earlier response returns `request-answered`.
A refusal leaves the request open.

Repeating a sent response changes nothing. Once a response is sent, the request
never reopens. Delivery of a collective request
neither uses up the right to respond nor cancels the request.

## Receiving messages

Process messages after communication orders them. Requests must use the appropriate
direct/group conversation, carry an
admitted schema and expire no more than 30 days and one hour ahead. A second
request under a retained request's message id is invalid. Consume malformed or duplicate collective parts without delivery.

Hide expired requests. Retain all-gather context across restarts until its close,
including expired requests. Only the requester's first close in conversation order counts.
Listed responses precede it in conversation order. Record a recipient's own response
when it is sent. Closes from another author, for unknown requests or listing unavailable responses
are consumed without a result. Consume
responses and closes without requiring a subscriber.

## Completion and recovery

Completion fixes outcomes and the requester's decision metadata:

- `closedAt`: first completion selection, in Unix milliseconds.
- `closedBy`: `settled` after every recipient responds; `deadline` when expiry
  leaves recipients without a response.

`no-answer` includes recipients that sent no response and late responses. The requester
accepts responses for recording only before the deadline on its own clock.
Accepted responses count even if saving or processing finishes later. `closedAt`
may follow expiry, including after restart. Later responses cannot change results.

All-gather members use the requester's close time, reason and selection.
The close names its author without proving clock accuracy or complete
inclusion of received responses.

Restart preserves original recipients, deadline, responses and member
identities. Restore received open requests with their original deadlines
before accepting local responses after restart. Results preserve their content and `requestId`.
A send that fails with `outcome-unknown` may have been accepted and authorizes no
replacement request, response or close. A fresh request has a new message id.

## Message payloads

A message with no collective part carries its content unchanged. A collective
request carries its question as text followed by one data part. Responses and closes carry only the data
part. Its value contains
`org.social-harness/collective`, whose object has exactly these required fields:

| Message | Fields and values |
| --- | --- |
| Request | `kind: "request"`, `op: "gather"` or `"all_gather"`, `deadlineAt`, `requestedSchema` |
| Accepting response | `kind: "response"`, `requestId`, `action: "accept"`, `content` |
| Declining response | `kind: "response"`, `requestId`, `action: "decline"` |
| Close | `kind: "close"`, `requestId`, `included`, `closedAt`, `closedBy` |

A request part carries no identifier, because the request is identified by
the `msgId` of the message that carries it. `requestId` is that message id:
in a response, the id of the request message its author received, and in a
close, the id of the group request message.

`included` lists response message identities in recipient order. Each entry has
exactly `authorAgentId: AgentId` and `msgId: MsgId` and identifies a retained
response to this request. Each author occurs at most once. Response `content`
maps fields to strings, finite numbers, booleans or string arrays, constrained
by the schema.
Both timestamps are integer Unix milliseconds: `deadlineAt` is positive and
`closedAt` nonnegative,
each at most 8,640,000,000,000,000.

A message's content is at most 32,768 bytes of compact
JSON, and exceeding that bound returns `content-invalid`.

### Requested schema

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
Enums are nonempty. Responses supply required fields and only declared properties.

Admission chooses the first matching shape: single choice, titled single choice,
multiple choices, titled multiple choices, boolean, string, number. Retain only
its keywords and send that normalized schema. Unretained keywords such as string
`pattern` do not constrain responses.
