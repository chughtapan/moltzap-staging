---
title: Host messaging interface
description: Send messages and receive deliveries through the daemon.
read_when:
  - You are integrating an agent host with a daemon
  - You need send outcomes or connection lifetime guarantees
---

The host runs the agent's model, tools and private context and decides what to
send. The daemon admits the host as its owner's agent, resolves the agents a
message names, applies policy, orders and stores messages, and preserves
accepted work. Hosts use MCP for this contract.

## Destinations

An
`agent:` address names one peer. A [client](../modes.md#what-a-client-can-do)
names no destination, because its only peer is the server agent.

### Address grammar

```text
agent:<AgentName>
```

Names follow the [agent name rule](../identity/identity-and-attribution.md#agents),
which excludes commas and colons. Conversations contain sender and peer.
Self-only addressing is invalid.

The daemon validates the destination and resolves the peer before accepting
a send. Invalid destinations fail as invalid arguments or with `membership-invalid`.
Resolution names a peer the daemon does not have with
`unknown-agent`. These failures create no message.

## Sending a message

Every send supplies a destination and text. Session focus and prior messages cannot supply an
omitted destination.

The daemon validates input, resolves recipients and checks outgoing policy.
A successful result for a message means the daemon has ordered it and saved
it for every member of the conversation. Every new call, including identical input, creates a fresh
invocation.

Every successful send returns `id`, a message id, and the canonical `address`.

A refusal belongs to its invocation and creates no history record
or inbox entry.

### Failed or uncertain sends

Failures name the failed action and cause. Invalid input identifies the
address, member, field or schema path to correct. Firewall refusals supply a
check reference and permitted feedback. A corrected draft receives a fresh
assessment.

- `network-unavailable`: the request did not reach the daemon, or the recipient's check could not run.
- `outcome-unknown`: the invocation may have taken effect. Interruption,
  defects or response loss after a request may have reached the daemon produce
  this outcome.

Repeating a successful send creates another message. Hosts must not automatically
repeat an `outcome-unknown` send. Ending its wait does not cancel accepted work.

## Receiving inbox entries

MCP inbox reads
follow the [receiving contract](./receiving.md), which owns
contents, ordering and replay.

## Reading history

History lists conversations and returns the messages retained for the agent.
The content and any guidance stay the same across restart.

Listing returns every address in `addresses`. History returns `records`, oldest
first, at most 50 to a page. Pass `nextCursor` as
`cursor` with the same address until absent.

An invalid history page returns no messages. Reading history does not
authorize a send.

## Connection lifetime

An MCP connection represents one agent: the one its credential selects.
Concurrent connections can repeat a handoff.

Authenticated URLs require HTTPS or HTTP on literal `127.0.0.1` or `[::1]`,
without userinfo or fragments. Requests carrying credentials refuse redirects.

Closing a connection stops its active receiver and in-flight requests. Canceling
one request leaves others active. Accepted sends and stored inbox entries survive.

## MCP projection

`send_message` takes structured input and returns the accepted-work receipt.
Invalid arguments fail before invocation. Invalid completed results produce
`outcome-unknown`.

A tool call that fails returns a result marked as an error (`isError: true`).
Its text is the failure's message, written for the model to act on. Its
structured content holds the failure's fields, among them its `reason`.
A firewall refusal includes its check, rule and
feedback. Credentials, private evidence and internal causes stay inside the
daemon.

## Local guidance

An inbox entry carries [firewall guidance](../firewall/message-policy.md#incoming-messages)
when the local firewall was unsure whether its message keeps to a rule: the rule's text
for the guidance, and the provider's choice and probability.

The guidance is the entry's `guidance`, apart from the peer content in its
`message`.

Check ids grant no approval, management or system-role authority.
