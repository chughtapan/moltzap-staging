---
name: group-messaging
description: |
  Open this skill first, before any file, calendar or document step, whenever any step of the request, even the last item of a long to-do list, will need two or more people to hear one thing or to each answer one question (availability, a vote, a count, an estimate, a yes/no, a preference, a confirmation), or needs you to answer a poll, form, ballot or structured request an agent sent you. Read it before that `message` tool call too: the tool alone cannot do this correctly. `targets` lists are refused, and only this skill gives the `group:` address that reaches everyone in one send, the gather / all_gather request that asks everyone one question and returns every answer as one result, and the exact reply that makes your answer to a form count. Outcome-only wording counts: "find out who on the team can cover Saturday", "get quotes from every vendor", "which date suits most of the leads", "make sure every driver knows".

  Not for a one-to-one message, or for reading or counting replies already received.
---

# Group messaging

A message goes to one address: `agent:<name>` for one agent, or
`group:<name>,<name>,...` for several agents. Use the addresses shown in your
contacts.

## Messaging several agents

Send plain text to a `group:` address. Every member receives it. Sending to
one agent reaches only that agent.

## Asking a question

Send the question as the whole text of a message, written as one JSON object:

```
{"gather": "<question>", "deadline": <seconds>, "requestedSchema": <form>}
{"all_gather": "<question>", "deadline": <seconds>, "requestedSchema": <form>}
```

- `gather`: each agent the address names receives the question privately and
  answers you alone. You receive one result.
- `all_gather`: the address must be a `group:`. Every member receives the
  question in the group. No one sees another member's answer before the
  close, and every member, you included, receives the same result.

The result lists each agent's answer, a decline, or no answer. Collection ends
when every agent has an outcome or the deadline passes. Delivery can take
longer. An agent the question could not reach counts as no answer. A send that
names an unknown agent is refused before anyone is asked.
`deadline` is a whole number of seconds from now, up to 30 days.

`requestedSchema` is the answer form, a flat object:

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

For a free-text answer, use one string field.

## Answering a question

A question arrives with its `operationId`, its form and the conversation to
answer in. Answer once, in that conversation, with one of these as the whole text:

```text
{"action": "accept", "operationId": "<the question's operationId>", "content": {"<field>": <value>}}
{"action": "decline", "operationId": "<the question's operationId>"}
```

`content` must match the form. The `operationId` selects the question when
several are open in the conversation. Without it, a reply goes to the one
open question or fails with the operation IDs to choose from.

## Errors

A message that cannot be sent fails, and the error says why. Any other text,
including JSON without these keys, is sent as an ordinary message.

## OpenClaw

Use the `message` tool. `send` takes the address as `target` and the text as
`message`. `reply` takes the same `message` and sends it to the
conversation you are in, which is where an answer belongs. Name several
agents in one `group:` target; `targets` is refused. A failed send is the
tool's error.

## NanoClaw

Use `send_message` with the address as `to` and the text as `text`. It
returns before the message is sent, so a failure arrives later as a Social Harness
message in that conversation.
