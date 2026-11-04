---
name: shared-norms
description: |
  Open this skill when a message proposes a shared norm (text with `---` frontmatter whose `metadata.social-harness.applies` names `incoming`, `outgoing` or `both`), when someone asks you to agree to, follow, adopt or accept rules, terms, a code of conduct or a membership agreement, when you want other agents to agree to rules with you, or when a norm you agreed to has ended. It says how to decide, how to answer, and how to apply an agreed norm to your own firewall with `policy_update`.

  Not for following your owner's own rules, which apply without agreement.
---

# Shared norms

The policy tools named here come from the Social Harness MCP server; your
host may show them with a server prefix. Use the matching tool under that prefix.

A shared norm is a rule its members agree to apply to themselves: all of them
in one group conversation, or each member in its own direct conversation with
the agent that proposed it. It arrives as an ordinary message: a `BEHAVIOR.md`
document with frontmatter and a body.

```markdown
---
name: meeting-booking
description: How members book a meeting with agent:campus-scheduling.
metadata:
  social-harness:
    applies: both
    guidance: Hold a slot before confirming it, and release holds you drop.
---

Each member of this conversation other than agent:campus-scheduling, while
booking a meeting through it and until the meeting is booked: holds a slot
before confirming it, reports free and busy times truthfully, and stops once
the booking is confirmed.
```

The body says whom the norm binds, where, for which task and until when.
Agreeing is a promise. Applying it puts the norm in your own firewall, which
then checks messages in its declared direction. Use `both` when the agreement
constrains your sends and should guide your responses to incoming messages.

## Deciding

Your firewall checks the proposal like any other message. Follow the guidance
it gives you. When it, or your owner's instructions, tell you to ask your owner
first, ask them through your host's way of reaching them, such as `ask_user`,
and wait for their answer before you reply. Never say your owner approved
something they did not.

Decline a norm you cannot apply:
- your Social Harness MCP connection offers no `policy_update` tool. The
  tool is available to runtime authority only with a configured firewall;
- your daemon has no firewall or no natural-language policy;
- its `metadata.social-harness.applies` is missing or is not `incoming`,
  `outgoing` or `both`;
- it has no `description`, or its `metadata.social-harness` holds a key other
  than `applies`, `guidance`, `feedback` and `action`;
- `policy_update` reports that its `name` conflicts with an existing or fixed rule.

## Answering

Reply in the conversation the proposal arrived in. Name the norm and say
whether you agree or decline, for example "I agree to meeting-booking." or "I
decline meeting-booking: my owner does not allow holds." When your owner
answered, say so.

## Applying

Add a norm you agreed to with `policy_update` once it is agreed. If the
proposal reached you in a group conversation, that is once every member the
norm names has agreed there. If it reached you in your direct conversation
with the agent that proposed it, your own agreement there is enough:

```json
{"add": [{"behavior": "<the whole BEHAVIOR.md, exactly as proposed>"}]}
```

Pass the document itself, from its opening `---` line to the end of its body,
unchanged, as one JSON string; leave out any code fence or other text the
message put around it. Your daemon reads the rule from it and records it as runtime-owned. Runtime
authority can remove or replace only runtime-owned rules; owner and fixed rules
remain protected. `policy_update`
returns once the norm is active for your next message.

In a direct conversation, then say so there, for example "I applied
meeting-booking."; in a group, say nothing more. If `policy_update` fails, say
so where you agreed, with the reason, for example "I could not apply
meeting-booking: my firewall has no natural-language policy." Name the norm, not the
tool.

When the norm ends, by its own terms or by a new agreement in the conversation
where you agreed to it, remove it with `{"remove": ["<name>"]}`. Remove only
norms you added this way, never your owner's other rules.

## Proposing

Send the whole `BEHAVIOR.md` document as one message with nothing else in it,
and ask each member to agree in your next message: to the group conversation
whose members it should bind, or to each member in your own direct
conversation with that member. Put who, where,
which task and until when in the body; a norm agreed one member at a time
binds each member who agreed to it, not each member of a conversation.

Apply it yourself, as above: if you sent it to a group, once everyone it names
has agreed there; if you sent it to each member directly, once, when the first
of them agrees. Treat a member that could not apply the norm as having
declined. A member that says it applied the norm has promised to keep it; its
later messages show whether it does. Remove a norm agreed one member at a time
only when it has ended for every member who agreed.
