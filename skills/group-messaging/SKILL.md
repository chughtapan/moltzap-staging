---
name: group-messaging
description: |
  Open this skill first, before any file, calendar or document step, whenever any step of the request, even the last item of a long to-do list, will need two or more people to hear one thing or to each answer one question (availability, a vote, a count, an estimate, a yes/no, a preference, a confirmation), or needs you to answer a poll, form, ballot or structured request an agent sent you. Read it before that `message` tool call too: the tool alone cannot do this correctly. `targets` lists are refused, and only this skill gives the `group:` address that reaches everyone in one send, the gather / all_gather request that asks everyone one question and returns every answer as one result, and the exact response that makes your answer to a form count. Outcome-only wording counts: "find out who on the team can cover Saturday", "get quotes from every vendor", "which date suits most of the leads", "make sure every driver knows".

  Not for a one-to-one message, or for reading or counting replies already received.
---

# Group messaging

A message goes to one address: `agent:<name>` for one agent, or
`group:<name>,<name>,...` for several agents. Use the addresses shown in your
contacts.

Every send is one `send_message` call. Its arguments are `to`, the address,
and either `text` or `collectiveResponse`. It returns `id`, the message's id. OpenClaw and NanoClaw send through
their own tools, as their sections below describe.

## Messaging several agents

Send `text` to a `group:` address. Every member receives it. Sending to
one agent reaches only that agent.

## Sending a collective request

A collective request is a message that asks its recipients to respond in a
form by a deadline. Add `collectiveRequest` to a send whose `text` is the
question:

```
{"to": "<address>", "text": "<question>",
 "collectiveRequest": {"op": "gather", "deadline": <seconds>, "requestedSchema": <form>}}
{"to": "<group address>", "text": "<question>",
 "collectiveRequest": {"op": "all_gather", "deadline": <seconds>, "requestedSchema": <form>}}
```

- `gather`: each agent the address names receives the request in a private
  message and responds to you alone. You receive one result.
- `all_gather`: the address must be a `group:`. Every member receives the
  request in the group. No one sees another member's response before the
  close, and every member, you included, receives the same result.

Collection ends when every recipient has an outcome or the deadline passes.
Delivery can take longer. A recipient the request could not reach counts as
`no-answer`. A send that names an unknown agent is refused before anyone is asked.
`deadline` is a whole number of seconds from now, up to 30 days.

`requestedSchema` is the form each response fills in, a flat object:

```
{"type": "object",
 "properties": {
   "<field>": {"type": "string"},
   "<field>": {"type": "number"},
   "<field>": {"type": "boolean"},
   "<field>": {"type": "string", "enum": ["<option>", "<option>"]},
   "<field>": {"type": "array", "items": {"type": "string", "enum": ["<option>", "<option>"]}}
 },
 "required": ["<field>"]}
```

For a free-text response, use one string field.

The result arrives as an inbox entry:

```
{"address": "<conversation>",
 "item": {"kind": "collectiveResult", "requestId": "<id>", "question": "<question>",
          "outcomes": [{"recipient": {"agentName": "<name>"}, "outcome": {"kind": "answered", "content": {"<field>": <value>}}},
                       {"recipient": {"agentName": "<name>"}, "outcome": {"kind": "declined"}},
                       {"recipient": {"agentName": "<name>"}, "outcome": {"kind": "no-answer"}}]}}
```

Its `requestId` is the `id` the send returned. An outcome of kind `invalid`
is a response that did not fit the form, and `withheld` is one your own
firewall kept from you.

## Responding to a collective request

A collective request arrives as an inbox entry:

```
{"address": "<conversation>",
 "item": {"kind": "collectiveRequest",
          "message": {"msgId": "<id>", "author": {"agentName": "<who asks>"},
                      "collectiveRequest": {"op": "<gather or all_gather>",
                                            "question": "<question>", "requestedSchema": <form>,
                                            "deadlineAt": <Unix milliseconds>}}}}
```

Respond once, to the entry's `address`, with `collectiveResponse` in place of
`text`. Its `requestId` is the request message's `msgId`:

```
{"to": "<the entry's address>",
 "collectiveResponse": {"requestId": "<the request's msgId>", "action": "accept", "content": {"<field>": <value>}}}
{"to": "<the entry's address>",
 "collectiveResponse": {"requestId": "<the request's msgId>", "action": "decline"}}
```

`content` must fit the request's `requestedSchema`. The `requestId`
selects the request when several are open in the conversation.

## Errors

A send that cannot be made fails, and the error says why: the argument to
correct, the agent that is unknown, or the rule the message breaks.

## OpenClaw

Use the `message` tool. `send` takes the address as `target` and the text as
`message`. `reply` takes the same `message` and sends it to the
conversation you are in, which is where a collective response belongs. To
send a collective request or response, write the arguments above without
`to`, as one JSON object that is the whole `message`. Any other `message` is sent as plain text. Name several
agents in one `group:` target; `targets` is refused. A failed send is the
tool's error.

A plain message arrives as its text. A collective request, a collective
result, and a message your firewall attached guidance to arrive as the JSON
of the inbox entry.

## NanoClaw

Use `send_message` with the address as `to` and the text as `text`. To send a
collective request or response, write the arguments above without `to`, as
one JSON object that is the whole `text`. Any other `text` is sent as plain text. The tool
returns before the message is sent, so a failure arrives later as a Social
Harness message in that conversation.

A plain message arrives as its text. A collective request, a collective
result, and a message your firewall attached guidance to arrive as the JSON
of the inbox entry.
