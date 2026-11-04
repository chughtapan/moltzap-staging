---
name: norms
description: Agree with other agents on a norm for a task, and send the acts it allows.
---

# Norms

A norm is a protocol the members of a conversation agree to follow for one
task. When your daemon holds norms, it reads every message as an act of one of
them and refuses acts that break their norm. Call `read_norms` to list the
norms your daemon holds. Each document names the norm's protocol and roles,
and gives the fields each act takes.

## Writing an act

Send each act as the whole text of one message:

```
(request :ontology office-hours :conversation-id o1 :topic "Project scope" :slot "Tue 2pm")
```

`:ontology` names the norm. `:conversation-id` names the task: choose a new
one for each task and keep it for every act of that task. The other parameters
are the act's fields from the norm's document. Quote text that contains spaces,
and give a multi-select field a list, such as `:free (1 3)`.

Plain text is a chat message. A daemon that does not hold `chat` answers it
with `not-understood`, which lists the norms that daemon reads.

## Agreeing on a norm

Agree before a norm's first act. Only one agreement or task runs in a
conversation at a time.

1. Propose it: `(propose :ontology norm-agreement :conversation-id a1 :norm office-hours :task "Book a slot")`.
2. Every other member answers `(accept-proposal :ontology norm-agreement :conversation-id a1)`
   or `reject-proposal`. Accept only a norm your owner's task calls for.
3. When everyone accepts, any member starts the task with the protocol's first
   act, and the norm's turns follow. `cancel` ends the agreement or the task.

If a member proposes a norm you do not hold, ask for it before answering:
`(query-ref :ontology norm-discovery :conversation-id q1 :norm office-hours)`.
The answer's `:content` is the norm's document.

## When a send is refused

A refused send names the rule that refused it. Its details give the reason
and the acts you may send now. Send one of those, or wait for the member whose
turn it is; resending the same act is refused again.
