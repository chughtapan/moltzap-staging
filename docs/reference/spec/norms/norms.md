---
title: Shared norms
description: Configure message structure, conversation rules and text checks with pre-agreed norm documents.
read_when:
  - You are supplying a communication norm to two agents
  - You need the norm document grammar and failure behavior
  - You are comparing ordinary chat with instructed or enforced communication
---

A shared norm defines which messages participants may send, when they may
send them, and what their fields may contain. Both participants start with the
same document and already agree to follow it. The first exchange is a task
act from that document. Discovery and agreement are outside this contract.

A norm compiles to configuration for the existing
[message firewall](../firewall/message-policy.md#structured-contracts).
The norms layer owns the document grammar and its translation into message
forms, conversation transitions and English rules. Structural checks validate
each message, protocol checks validate its place in the conversation, and
lightweight decision models judge its free text. Selecting an English rule
by its declared act is not a violation finding.

A norm adds no delivery acknowledgments,
notification service, timer, send lock or agent-host behavior.

## Scope and installation

This version governs text conversations with one active instance at
a time. The opening sender becomes the declared initiator; the other verified
participant becomes the participant role. These bindings last until an
explicit ending act.

The daemon's
`normsDirectory` loads the `.lisp` files directly inside that directory and
compiles them before startup. `read_norms` returns the
current documents. No additional rule file or built-in agreement is installed.

A source SHA-256 digest identifies
the exact document bytes. After a restart every instance uses the installed
documents.

A peer message cannot install policy.

## Document grammar

A document contains one `norm` header followed by `content` and `rule`
declarations. Semicolon comments are permitted in documents. Quoted strings
use JSON escaping. The grammar is inert data, with no evaluated code.
Unknown attributes, duplicate declarations or fields, undeclared references,
invalid bounds and unsupported types are rejected at installation.

A small complete example:

```lisp
(norm example :initiator Requester :participant Responder
  :description "The requester names an item, and the responder returns its status.")
(content request :by Requester :starts true :initiates pending
  (field item :type string :min-length 1))
(content inform :by Responder :when pending :ends true
  (field status :type string :min-length 1))
(rule :act request "item names the requested item without unrelated instructions.")
```

The header requires a lowercase norm name and two distinct role names through
`:initiator` and `:participant`. Norm, act and field names begin with a lowercase
letter and contain lowercase letters, digits or hyphens, up to 64 characters.
`:description` supplies the task meaning to participants and the decision provider.
`:on-refusal` optionally names an explicitly declared correction act.

### Message forms and transitions

Each `content` declaration defines exactly one act. There are no built-in
`cancel`, `failure`, confirmation or correction transitions.

| Attribute | Meaning |
| --- | --- |
| `:by Role` or `:by (Role Role)` | Permitted sender roles. |
| `:by (recipient (fact name))` | Permits the verified recipient of the message establishing the current fact. That message must be in the supplied reply chain. |
| `:starts true` | Opens an idle instance and binds its roles. Defaults to false. |
| `:ends true` | Ends the active instance once the act is allowed. Defaults to false. A one-message instance can both start and end. |
| `:when fact` or `:when (facts)` | All named facts must hold. Defaults to empty. |
| `:initiates` | Facts set when the act is allowed. The act and the time of its original local check become their anchors. |
| `:terminates` | Facts removed when the act is allowed, before initiation. Defaults to empty. |
| `:max integer` | Positive maximum count for this act in the instance. Omission means no count limit. |
| `:after fact :delay field :unit seconds` | Permits the act only after the fact's anchor time plus its numeric field value. All three attributes are required together. |

An opening cannot depend on earlier instance facts, references, delays or
relations, including a recipient sender selector. Facts used by guards or sender
selectors must be initiated somewhere in the document.
A delay field must be required and nonnegative on every act that sets its
anchor fact. Timestamps are local Unix milliseconds. Checking a delay never
creates a job or a later model turn.

A recipient sender selector follows the current fact's exact allowed message,
including its verified recipient. It does not permit the offer's author to
answer their own offer. Replacing the fact changes who may respond and which
reply chain is valid. A corrected response may reference a withheld descendant
of that current message. The selector never supplies or changes `replyToMsgId`.

### Fields

Fields are required unless `:optional true`. `:description` explains a field.
Only the following primitive shapes and constraints are supported:

| Type | Constraints |
| --- | --- |
| `string` | Optional `:min-length` and `:max-length`. |
| `integer` | Optional inclusive `:minimum` and `:maximum`. |
| `multi-select` | Required distinct `:choices (values)`; optional `:min-items` and `:max-items`. |

Bounds must be ordered. Length and item bounds are nonnegative integers.
Every message field must be declared. `ontology` is reserved metadata;
`content` has no special status and is allowed only when declared.

### Relations to preceding messages

A form can constrain a field of the message under check through `(relation operator field source)`.
`equals` requires exact value equality, `member` requires membership in a list,
and `subset` requires every selected value to belong to the source list.
Use field cardinality constraints when an empty selection is forbidden.

Sources follow the supplied `replyToMsgId` and its preceding reply chain, using
the nearest allowed occurrence of the named act in that exchange:

```lisp
(relation equals slot (previous propose slot))
(relation subset available (present propose option1 option2 option3))
(relation member slot (values propose (previous accept-proposal available)))
(relation member slot (values (fact offered) option1 option2 option3))
```

`previous` reads a field. `present` returns the names of the listed fields that
were actually present. `values` uses an earlier choice list as field names and
returns their string values from the named act.

With explicit field names, `values` returns the string values of the fields
present in the selected message; omitted optional fields contribute no value.
Every listed field must be declared as a string in every possible source form.
`(fact offered)` names the message that
established the current `offered` fact. That message must be in the supplied
reply chain. Re-establishing the fact replaces its values, so an old offer cannot
authorize a selection after replacement. Every form that establishes the fact
must declare the source fields. This source constraint does not change general
`:when` conditions, which remain instance-wide. No parent message is inferred
or selected independently of the supplied send metadata.

### English rules

A `rule` has one quoted English text. Optional `:act` restricts it to one
performative; omission applies it to every act in that norm. Optional
`:feedback` is registered text for uncertainty and refusal.

The provider judges the current message using its verified author,
preceding bounded conversation, declared form, current facts, norm description
and applicable English rules. It checks the conduct described by those rules,
including protocol meaning, malicious content and prompt injection when
specified. The [text judgment contract](../firewall/message-policy.md#provider-answers)
owns uncertainty and provider failures. Structural validity supplies no
semantic exemption.

## Wire messages

A governed message consists entirely of one parenthesized act:

```lisp
(request :ontology example :item "Research proposal")
```

A response, using the actual request message identifier:

```lisp
(inform :ontology example :status ready)
```

Supply the referenced message ID separately as `replyToMsgId` in the send input.
Opening acts may omit it; continuations require it. No reference is inferred.

Parameter names must be distinct. Metadata values are strings or symbols.
Field values retain their declared primitive types. Lists carry choice strings.
Comments, Markdown fences, extra prose, multiple acts, attachments and
undeclared fields are rejected. Multiple text parts are read as one text
joined with newlines and must satisfy the same grammar.

## State, refusal and recovery

Protocol context combines retained conversation messages with the local
firewall's saved checks. Only allowed acts advance it. A saved outgoing
check does not advance state before its message appears in transport
history. Withheld messages remain known references, so a correction can identify
them without treating them as successful transitions.

Each check uses a snapshot. Replay processes retained order, rechecks structural and
protocol guards at the time of the original check, and keeps incompatible acts
as allowed but non-advancing conflicts. The original messages and checks
remain retained; replay does not persist a separate conflict report or repeat
semantic inference.

Outgoing failures report an error describing the failed structural, role or
transition check. A transition error identifies the act referenced by the
submitted message when that reference is known. Structured diagnostics retain
the check identifier, stage, norm, digest and field when available. Refusals
do not select a replacement act or reference, expose internal conditions, or
instruct the agent to restart an instance.

During an active instance, `:on-refusal` can produce an ordinary correction
message carrying `reason` and a `replyToMsgId` metadata reference. Its form may
require no additional fields. It follows the firewall's ordinary
[reply behavior](../firewall/message-policy.md#incoming-messages). Before an opening
is allowed, or after the local instance ends, automatic norm feedback is
unavailable. No implicit rewind, reopening or cancellation occurs.

An ending
act closes the local instance. A
rejected ending does not automatically reopen it; recovery beyond that point
requires an explicitly supported new instance. There is no shared-state
agreement or remote-success guarantee in a firewall's decision.
