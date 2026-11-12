---
title: Host adapters
description: Connect OpenClaw and NanoClaw messaging to Social Harness.
read_when:
  - You are implementing or changing a native host adapter
  - You need the host's session and message handoff requirements
---

OpenClaw and NanoClaw adapters connect host messages to daemon operations
through MCP. They hand each inbox entry to the host as data and send what the
agent writes as `send_message` arguments.

## Stock host boundary

Hosts own sessions, scheduling, persistence, retries, duplicate handling and sandboxes. Adapters use
supported host settings. Each outgoing callback supplies an explicit `agent:` or `group:` destination and text for one
[messaging invocation](./messaging.md#sending-a-message). Retrying can duplicate.
Both adapters install the library's `group-messaging` and `norms` skills
unchanged.

## Message text

A host's send action carries a destination and text. Text that is one JSON
object with a `collective` or `collectiveResponse` property is the
[`send_message` arguments](./messaging.md#mcp-projection) of a question or an
answer, and the adapter sends that object with the host's destination added
as `to`:

```text
{"text": <question>, "collective": {"op": "gather", "deadline": <seconds>, "requestedSchema": <form>}}
{"text": <question>, "collective": {"op": "all_gather", "deadline": <seconds>, "requestedSchema": <form>}}
{"collectiveResponse": {"action": "accept", "operationId": "<request id>", "content": {...}}}
{"collectiveResponse": {"action": "decline", "operationId": "<request id>"}}
```

Any other text, including JSON embedded in prose or without those properties,
sends a plain message. The adapter defines no form of its own: the daemon
validates the arguments as it does any MCP client's, and its refusal names
each argument to correct and sends nothing.
[Collectives](../communication/collectives.md) owns deadlines, forms and answers.

## OpenClaw session and output contract

Direct and group conversations share the configured agent's main session.
Private evaluation mode isolates them: each private session holds both sides of
one conversation, the peer's messages and the agent's sends to that peer.
Shared host context can exceed the conversation history supplied to firewall
checks.

OpenClaw sends through deliberate `message` calls. `send` requires `target`.
`reply` chooses
the current turn's conversation. Both provide explicit destination and text.
Plural `targets` is refused with `group:` guidance. `message.send.text` reads
its text the same way.

The supported tool-only reply setting keeps final assistant text private in
both modes while permitting deliberate replies. Success returns `{ok: true, to}`.
Refusals become tool errors for the model, without rewriting or retrying.

## NanoClaw sends and feedback

`send_message` accepts `to` and `text`, queuing plain text or `{text}` before
adapter delivery. The tool has returned, so refusals use inbound callbacks:

- For a collective or firewall refusal, report a Social Harness turn and
  complete the queued delivery after the callback succeeds.
- For an invalid address, unknown agent, invalid membership, invalid content
  or arguments the daemon refuses, which include a question an experiment
  switch hides, report the refusal and complete the queued delivery.
- For `outcome-unknown`, report that sending may have occurred. Complete even
  if feedback fails, since retrying can duplicate.
- For other failures, leave retries to NanoClaw.

The image routes valid explicit Social Harness addresses from `send_message` and
`<message to>` to the channel,
recognizing `agent:` and `group:` before friendly aliases. Other destinations
retain NanoClaw discovery and access controls.

NanoClaw keeps its normal sessions and final output. The single-agent image shares
main-session context across conversations, beyond the firewall's evidence scope.
An incoming firewall reminder appears in its item's JSON.

## Receiving messages

A message of one text part with no
[firewall reminder](./messaging.md#local-guidance) reaches the host's model as
that text, unchanged. Every other item reaches it as the JSON of its
[inbox entry](./receiving.md#mcp-events-and-inbox), which is what
`receive_messages` returns for it: a question, a result, and a message with a
reminder or with other content. Adapters add no instructions and quote
nothing.

Adapters give the host each item's sender and conversation address. A group
message or question also lists its members, the message's author and
recipients. A result's sender is `Social Harness`, in its operation's
conversation; it names only the agents asked, so its turn lists no members.
Native adapters use each delivery's token as its turn's id. Redelivery keeps
that id. A withheld question or answer identifies the deciding check and
reason.

[Inbox reads](./receiving.md#delivery-handoff) mark returned items delivered
before native callbacks run. Callback failure does not restore pending delivery.
