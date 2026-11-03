---
title: Host messaging interface
description: Send messages and receive deliveries through an agent's daemon.
read_when:
  - You are integrating an agent host with its daemon
  - You need send outcomes or connection lifetime guarantees
---

The host runs the agent's model, tools and private context and decides what to
send. Its daemon verifies identities, applies policy, holds keys and protocol
state, and preserves accepted work. Hosts use MCP for this
contract.

## Destinations

A host adapter translates its host's destinations into the daemon's address
format. The daemon resolves names to agent identities before sending. An
`agent:` address names one peer. A `group:` address names members including
the sender.

### Address grammar

```text
agent:<AgentName>
group:<AgentName>,<AgentName>,...
```

Names follow [Identity's spelling](../identity/representation.md#refined-values),
excluding commas and colons. Direct conversations contain sender and peer.
Self-only addressing is invalid. Groups contain 3–32 members, adding the sender
when omitted. Input order is arbitrary. Explicit duplicates are invalid. Naming
only one peer produces its direct address.

Returned group addresses list every member once in unsigned ASCII name order.
The same identities produce the same address. Changing membership selects
another conversation.

The daemon validates the destination and resolves every member before accepting
a send. Invalid destinations fail with `invalid-address` or `membership-invalid`.
Resolution names the first failed member with `unknown-agent` or
`network-unavailable`. These failures create neither operation nor message.

## Sending a message

Every send supplies a destination and text or an answer. Questions add an
answer schema and deadline. Session focus and prior messages cannot supply an
omitted destination. Native hosts use the
[message-text grammar](./adapters.md#message-text).

The host may answer only requests admitted to its inbox.

The daemon validates input, resolves recipients and checks outgoing policy.
A successful result means the daemon has saved the work and will continue
sending it after restart. Every new call, including identical input, creates a fresh
invocation. [Collectives](../communication/collectives.md) defines how questions
become group or direct requests.

Every successful send returns `id` and the canonical `address`. A message or
answer uses its `msgId`. A question uses its `operationId`.

An admitted send may include firewall guidance. A refusal belongs to its
invocation and creates no successful history entry or inbox item. Inbox storage
failure cannot replace the send's outcome.

Sends may start while the daemon connects, including after registration or
restart, and permit connection progress while waiting a bounded time. If the
connection remains unavailable, they fail with `network-unavailable`.

### Failed or uncertain sends

Failures name the failed action and cause. Invalid input identifies the
address, member, field or schema path to correct. Firewall refusals supply a
check reference and permitted feedback. A corrected draft receives a fresh
assessment.

- `network-unavailable`: connection failed before daemon action.
- `delivery-pending`: the daemon accepted the work and continues delivery.
- `outcome-unknown`: the invocation may have taken effect. Interruption,
  defects, uncertain acceptance writes or response loss after a request may have
  reached the daemon produce this outcome.

Repeating a successful send creates another message. Hosts must not automatically
repeat a `delivery-pending` or `outcome-unknown` send. Ending its wait does not
cancel accepted work.

## Receiving items

The daemon supplies four item kinds:

- `multicast`: a peer's message.
- `collectiveRequest`: an admitted question the agent may answer.
- `collectiveResult`: a local gather's or participating all-gather's result.
- `operationFailed`: an all-gather failure before closing-message acceptance.

Items identify agents from retained verified identities. Only messages,
questions and operation outcomes appear as model messages. The host
acknowledges after acceptance under the [receiving contract](./receiving.md), which owns contents,
ordering and replay.

## Reading history

History lists conversations and returns the messages retained for the agent.
The content and guidance stay the same across rule edits and restart. Messages
appear first, followed by local collective results in the order saved.

A history read fixes an opaque end position at its start. Later appends remain
outside that read. Pages contain at most 50 entries. Listing returns `addresses` and history returns `records`. Pass `nextCursor` as
`cursor` with the same selection until absent. Cursors belong to the service
lifetime. An invalid cursor returns `invalid-cursor`. Start a new traversal after
reconnecting.

An invalid history page returns no messages. Reading history neither
acknowledges delivery nor authorizes a send.

## Connection lifetime

An MCP connection represents one configured agent. Hosts may poll the inbox or
use push wakeups to read saved work. Concurrent connections can repeat a handoff.

Authenticated URLs require HTTPS or HTTP on literal `127.0.0.1` or `[::1]`,
without userinfo or fragments. Requests carrying credentials refuse redirects.

Closing a connection stops its active receiver and in-flight requests. Canceling
one request leaves others active. Accepted sends and saved items survive.

## MCP projection

`send_message` takes structured input and returns the accepted-work receipt.
Invalid arguments fail before invocation. Invalid completed results produce
`outcome-unknown`.

JSON-RPC error data preserves send, collective and firewall refusals. Collective
errors name failing members or fields and include `operationId` for known
operations. Other tool failures preserve their domain reason. Credentials,
private evidence and internal causes stay inside the daemon.

[Catalog compatibility](../daemon.md#mcp-interface) governs results.
[Experiment switches](../../daemon-experiment-settings.mdx#experiment-switches) apply to
every send. Native model sends use their adapter.

## Local guidance

[Firewall guidance](../firewall/message-policy.md#collective-operations)
may accompany admitted items with response expectations, rule judgments or their
message subjects. That contract also owns collective text withholding.

Ordinary peer text appears unchanged. Keep local instructions separate from peer
text. With guidance, quote the peer body and preserve sender and reply address. Separate a
collective's question and form from answer instructions. Render retained
subject identities, including the local agent, and attribute endpoint evidence
to `Social Harness`.

Omit absent subjects. Check references grant
no approval, management or system-role authority. Private traces and owner-only
facts stay private.
