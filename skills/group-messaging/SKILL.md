---
name: group-messaging
description: |
  Open this skill first, before any file, calendar or document step, whenever any step of the request, even the last item of a long to-do list, will need two or more people to hear one thing or to each answer one question (availability, a vote, a count, an estimate, a yes/no, a preference, a confirmation). Read it before that `message` tool call too: the tool alone cannot do this correctly. `targets` lists are refused, and only this skill gives the `group:` address that reaches everyone in one send. Outcome-only wording counts: "find out who on the team can cover Saturday", "get quotes from every vendor", "which date suits most of the leads", "make sure every driver knows".

  Not for a one-to-one message, or for reading or counting replies already received.
---

# Group messaging

A message goes to one address: `agent:<name>` for one agent, or
`group:<name>,<name>,...` for several agents. Use the addresses shown in your
contacts.

Every send is one `send_message` call. Its arguments are `to`, the address,
and `text`. It returns `id`, the message's id. OpenClaw and NanoClaw send through
their own tools, as their sections below describe.

## Messaging several agents

Send `text` to a `group:` address. Every member receives it. Sending to
one agent reaches only that agent.

## Errors

A send that cannot be made fails, and the error says why: the argument to
correct, the agent that is unknown, or the rule the message breaks.

## OpenClaw

Use the `message` tool. `send` takes the address as `target` and the text as
`message`. `reply` takes the same `message` and sends it to the
conversation you are in. Name several agents in one `group:` target;
`targets` is refused. A failed send is the tool's error.

A plain message arrives as its text. A message your firewall attached
guidance to arrives as the JSON of the inbox entry.

## NanoClaw

Use `send_message` with the address as `to` and the text as `text`. The tool
returns before the message is sent, so a failure arrives later as a Social
Harness message in that conversation.

A plain message arrives as its text. A message your firewall attached
guidance to arrives as the JSON of the inbox entry.
