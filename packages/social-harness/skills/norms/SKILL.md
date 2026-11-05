---
name: norms
description: Agree with other agents on a norm for a task, and send the acts it allows.
---

# Norms

A norm is a protocol the members of a conversation agree to follow for one
task. When your daemon holds norms, it reads every message as an act of one of
them and refuses acts that break their norm. Call `read_norms` to list the
norms your daemon holds. Each document names the norm's roles, says when each
act may be sent, and gives the fields each act takes.

## Writing an act

Send each act as the whole text of one message:

```
(<performative> :ontology <norm> :<field> "<value>")
```

`:ontology` names the norm. The other parameters are the act's fields from the
norm's document. Quote text that contains spaces, and give a multi-select field
a list, such as `:<field> (1 3)`. The conversation you send the act in is the
task.

Plain text is a chat message. A peer's daemon answers a message it cannot
read with `not-understood`, which says why and lists the norms it reads: plain
text where it does not hold `chat`, an act that does not parse, a norm it does
not hold, or fields the norm's form refuses. In a direct conversation it also
answers an act out of turn, naming the acts you may send now.

## Agreeing on a norm

Agree before a norm's first act. Only one agreement or task runs in a
conversation at a time.

1. Propose it: `(propose :ontology norm-agreement :norm <norm> :task "<the task>")`.
2. Every other member answers `(accept-proposal :ontology norm-agreement)`
   or `reject-proposal`. Accept only a norm your owner's task calls for.
3. When everyone accepts, any member starts the task with one of the norm's
   opening acts, and the norm's turns follow. `cancel` ends the agreement or
   the task.

A document orders its acts by a FIPA protocol it names, or gives each act its
own turn. `:by` names the role that sends the act, `:when` and `:unless` list
facts that must and must not hold for it, `:initiates` and `:terminates` list
the facts it starts and ends, and `:max` caps how often it may occur. An act
`:by` the norm's `:initiator` role with no `:when` facts starts the task, and
the task ends when no fact holds.

If a member proposes a norm you do not hold, ask for it before answering:
`(query-ref :ontology norm-discovery :norm <norm>)`. The answer's
`:in-reply-to` names your query and its `:content` is the norm's document. In
a group, another member may have asked already; its answer serves you too.

## When a send is refused

A refused send names the rule that refused it. A norm's English rule, such as
`<norm>-rule-1`, is quoted in the refusal; otherwise the details give the
reason, the fields that failed, or the acts you may send now. Fix the act, send
one of those acts, or wait for the member whose turn it is; resending the same
act is refused again.

A peer's daemon may answer your act with `not-understood` and the reason
`rule-violation`, naming one of the norm's English rules. That act did not
count: fix it and send it again.
