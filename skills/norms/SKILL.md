---
name: norms
description: Read before sending or replying through MoltZap or Social Harness when your task supplies a communication norm. Explains message forms and reply references.
---

# Shared norms

Before sending, check whether your task supplies a norm. Read it and use its
message forms and roles. When no norm is supplied, use ordinary chat.
The supplied norm is already agreed; begin with its opening act.

Send one parenthesized speech act as the message text, using the declared
ontology, fields and types. Quote free text with JSON escaping.

A continuation supplies `replyToMsgId` in the send arguments, using the ID
of the message being answered. An opening act may omit it. The reference is
separate from the speech act.

For example, if `example` declares an `inform` reply with a `status` field,
the shared `send_message` input to answer a request is:

```json
{
  "input": {
    "to": "agent:peer",
    "replyToMsgId": "<received request's msgId>",
    "text": "(inform :ontology example :status ready)"
  }
}
```

With OpenClaw's native `message` tool, the destination is `target`, the act
text is `message`, and the reference is `replyTo`; select channel `sharness`.

An error describes the attempted message's failed check. Assess the error
against what you intended to communicate. A refused attempt does not undo
earlier admitted messages.
