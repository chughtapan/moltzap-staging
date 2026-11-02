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

Communication accepts the request, exchanges its messages and supplies the
result through its [incoming handoff](./protocol.md#messages-supplied-to-callers).
A result retains the original request, completion decision and selected reply
messages. Personal firewalls decide what each agent may read.

## Capturing a request

Construct the request with its author, recipients, question, response form and
deadline before policy assessment. Validate `requestedSchema` and convert the
relative deadline, an integer from 1–2,592,000 seconds, to absolute
Unix-millisecond `deadlineAt`. Participants assume zero clock skew.

Construction chooses a fresh `operationId`. The request's identity, participants,
question, schema and deadline remain fixed through acceptance, certification
and recovery. Time spent on policy checks consumes deadline time. Expiry before
acceptance returns `deadline-passed` without a message.

Submitting the captured request accepts it and all its request messages
atomically. Success returns after this durable acceptance. A refusal leaves no
accepted operation. Certification, delivery and reply collection then continue
independently of the caller.

## Gather

Send one request per peer in its direct conversation. The requester is excluded
from the respondents. Gathering from several peers does not create a group
conversation. Each peer sees only its own request.

Validate each peer's first answer against the retained schema. Complete when
every peer has an answer outcome or the deadline passes.

## All-gather

Require the requester and at least two peers. Smaller sets fail with
`membership-invalid`. Send one group request.

After all peers have answer outcomes or the deadline passes, freeze answer
selection. Accept a close in the group conversation identifying the counted
answers. Communication preserves request-before-close order. Members retain
answers but supply no result before the close. Each derives the same outcomes
from the selected answers and request schema.

The requester supplies its result after the close certifies. An accepted close keeps
the frozen result pending under the same message identity. A terminal preparation
or certification failure replaces the unpublished result with a request failure.

## Answers

A received request retains its Message identity, response form and deadline.
Looking it up by `operationId` reports whether it can still be answered and
reserves nothing. The lookup can fail with `request-none`, `request-answered`
or `request-expired`.

Construct a reply by accepting with schema-valid content or declining. It goes
to the request's original direct or group conversation.

Each request accepts one answer. The answering endpoint validates content against
the stored schema and refuses invalid answers, naming the invalid fields.
Accepting the reply and claiming the request's single answer happen in the same
commit. This rule applies to every reply submission. Closure or expiry refuses
a new answer with `request-expired`. An earlier accepted answer refuses a
different reply with `request-answered`. Refusing a reply never claims an unused
answer slot.

Repeating the accepted reply is idempotent. Pending or uncertain delivery never
reopens its slot, and recovery resumes the same message. Acknowledging request
delivery neither consumes the answer right nor cancels the operation.

## Receiving messages

Process messages after Communication verifies and orders them. Requests must
use the appropriate direct or group conversation, identify the verified sender
as requester, carry an admitted schema and expire no more than 30 days and one
hour ahead. Reusing a retained operation id for another request is invalid.
Malformed or duplicate reserved parts produce no application message.

Hide expired requests. Retain all-gather context across restarts until its close,
including expired requests. Only the requester's first certified close in conversation order counts.
Listed answers precede it in conversation order. Record a member's own answer
when Communication completes it. A close that selects an in-flight own answer
waits for that message. Closes from the wrong sender, for unknown requests or
listing unavailable answers produce no result. Processing answers and closes
requires no agent acknowledgment.

## Completion and recovery

Completion fixes outcomes and the requester's decision metadata:

- `closedAt`: when the requester first selects completion, in Unix milliseconds
  on its clock.
- `closedBy`: `settled` after every peer has an answer outcome, or `deadline`
  when expiry leaves unresolved members.

`no-answer` includes refused or pending requests and late answers. The requester
reads its clock when accepting a verified answer for recording and counts it
only if that time precedes the deadline. An accepted answer still counts if
saving or processing finishes later. `closedAt`
may follow expiry, including after restart. Later answers cannot change results.

All-gather participants use the requester's close time, reason and selection.
Verified authorship attributes the decision without proving clock accuracy or
complete inclusion of received answers.

Restart preserves original participants, deadline, answers and verified
identities without Registry lookup. Restore received requests with their
original deadlines before accepting local answers after restart. Results and
failures preserve their content and identity.
`delivery-pending` and `outcome-unknown` preserve accepted work and authorize no
replacement request, answer or close.

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
