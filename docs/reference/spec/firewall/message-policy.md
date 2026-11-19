---
title: Message firewall
description: How an agent's message policy governs what it sends and receives.
read_when:
  - You need to know what a firewall decision permits or refuses
  - You are configuring rules
---

The message firewall checks what an agent sends and receives against its
owner's rules and agreed norms. It receives a message, its participants
and conversation evidence. It returns a judgment, the rules that
determined it, and the feedback or guidance the agent may receive.

The firewall owns the message policy in force and its checks. The daemon supplies the message and evidence, applies the decision to
sending or delivery, and saves the check. Storage preserves those records across restart.

The firewall is optional and experimental. Without a message-policy file,
`sharnessd` installs none. It checks messages between agents.
[Only the server agent](../modes.md#firewall-and-norms) has one. The host owns models, tools and the agent's private context.

## Structured contracts

A configured contract specifies message forms, permitted conversation
transitions and scoped English rules. For direct text messages, structural
and protocol checks run before the text rules. Successful checks continue to the
applicable English rules.

A continuation's reply reference must belong to the active instance. A
reference from an ended instance cannot change or end a later instance
in the same conversation. Withheld messages retain their reference ancestry
for correction replies without advancing protocol state. An unreferenced
withheld message does not establish membership in the active instance.

## Decisions

Applicable text rules judge messages that remain eligible after structural,
and protocol checks. Checks capture the active
rules. A reject or hold judgment refuses an outgoing send or withholds an
incoming message. Findings combine in priority order:

1. A settled rule violation rejects.
2. Required missing evidence, unavailable checks or an unsure answer about an
   outgoing draft hold.
3. Otherwise, allow.

Every active rule needs a finding. A pass cannot erase another rule's violation.
Checks use the captured message and evidence and cannot alter them. An empty
policy still validates the message and required evidence.

Rules without valid answers or sufficient budget are unavailable.
Cancellation produces no decision.

### Provider answers

Text rules describe when they apply and the conduct they require. A [norm rule](../norms/norms.md#english-rules)'s
explicit act and direction metadata only route which rules to judge.

For each rule, the provider answers one question about the message: does it
break the rule? The answer is `compliant` or `violation`, with a probability for each.
An outgoing draft is judged as the local agent's; an incoming message is judged
as its peer sender's.

An answer is unsure if its probability is below `minimumProbability` or lower
than the other answer's.

| Answer | Outgoing draft | Incoming message |
| --- | --- | --- |
| Settled violation | Refused, with the rule's feedback | Withheld; a `reply` rule answers the sender |
| Settled compliant | Sent | Allowed |
| Unsure | Held as unresolved evidence and refused with the rule's feedback, so the agent can revise it | Allowed with guidance |

Findings retain the provider's choice and probability. Settled violations retain
their effect. Invalid output or provider failure is unavailable; required
missing evidence still holds. Accepting nonempty rule text promises no
accuracy of its checks in any language.

## Incoming messages

Allowed peer content reaches the host unchanged. When the provider was unsure
about a rule, the inbox entry carries guidance: an instruction that gives the
rule's registered guidance, or its feedback when it has none, with the
provider's choice and probability. Guidance never promotes peer text to an
instruction or exposes the check's private evidence. An inbox
entry the provider settled carries no guidance.

A text rule whose `action` is `reply` answers each message it withholds for a
rule violation: the sender receives the rule's registered feedback text.
A reply is an ordinary message from this agent to the conversation of the
withheld message, sent once for that message. Every generated reply must pass the outgoing checks. If refused, the daemon
saves the check and sends no reply. A message withheld for unresolved evidence or an
unavailable check gets no reply, and `deny`, the default, withholds silently.

## Outgoing messages

Refusal returns a rule-violation, unresolved-evidence or check-unavailable
reason, the check id and
the registered feedback of the deciding text rules. Its error message carries the same fields, without private evidence,
credentials or raw provider output.

A refused draft creates no send. A revised invocation gets a new
check. The daemon neither changes the draft nor sends substitutes or
later approval. Accepted sends follow the
[send outcome contract](../host/messaging.md#failed-or-uncertain-sends).

An allowed send carries no
guidance.

## Evidence

A check receives the message, its participants, rules, owner facts and
bounded conversation evidence once. The check applies to that captured
evidence and exact message, without a freshness comparison or an
automatic second check when later evidence arrives. Acceptance preserves the
checked recipients, content and message identity.

### Available history

A check captures preceding messages in conversation order, excluding the
message under check. The limit defaults to 256 entries, configurable from 1–1024.
Outgoing checks use the destination conversation; other conversations and host
private context are excluded. `earlierHistoryOmitted` alone causes no hold.
Exceeding the
history window's separate 2 MiB budget creates an evidence gap. Required
missing evidence holds. Installed structured
contracts separately receive complete preceding protocol history and exact
local check records, outside the bounded history window.

The firewall may use earlier messages it withheld or blocked when checking a
new message. Their content remains private, as though the agent had not seen
it. Authored content without proof that it came from an agent caller remains
private evidence.

Guidance exceeding the 2 MiB inbox-entry budget makes the check unavailable,
which holds the message. Rules needing other conversations, older messages or owner instructions
elsewhere require separately supplied evidence.

### Retained decisions

Each completed check identifies the message, evidence and policy it checked.
Only completed checks enter decision history. Each completed record saves its decision summary with the revision of
the policy it applied, checked and deciding rule ids, origins and registered advice.
Reads return that saved summary without message content or private evidence.

## Message-policy file

The settings file's `messagePolicyFile` names a readable regular JSON
file, at most 1 MiB.

| Property | Requirement/default |
| --- | --- |
| `format` | Required: `org.social-harness/message-policy/v2`. |
| `text` | Optional: `fixedRules` (a `TextRule` array); `fixedBehaviors` (directory paths). Arrays default `[]`. |
| `decisionProvider` | Required with `text` and for held norms with English rules: `{model}`. |
| `textSettings` | Defaults: `minimumProbability` 0.8, below which an answer is unsure; 384,000 request bytes; 15 s per request. |
| `facts` | Owner facts for every check; default `[]`. |
| `maximumConcurrentChecks` | Optional shared inference concurrency across revisions. |

Unknown properties and unsupported values fail. Trusted process composition may
supply its own provider and fixed text rules. The generated API reference owns
those callable types. The norms an agent holds add their
[English rules](../norms/norms.md#english-rules) after the file's fixed rules,
so a daemon whose norms have rules needs `decisionProvider` even when the file
has no `text`.

Without `decisionProvider`, no
provider or key is used. Oversized catalog/history requests hold without dropping required
evidence. Bytes neither measure tokens nor reserve capacity; provider limits
remain unavailable checks.

## Behaviors

A directory's `BEHAVIOR.md` supplies one rule. Paths are policy-file-relative;
absolute paths, `..` and escaping symlinks fail. Startup loads inline rules,
then fixed behaviors.

| Rule field | Document source |
| --- | --- |
| `id` | Frontmatter `name`. |
| `text` | Trimmed body. |
| `applies` | Required `metadata.social-harness.applies`: `incoming`, `outgoing` or `both`. |
| `guidance` | Optional `metadata.social-harness.guidance`: the guidance for an allowed incoming message the provider was unsure about. |
| `feedback` | Optional `metadata.social-harness.feedback`. |
| `action` | Optional `metadata.social-harness.action`: `deny` or `reply`. |

Other metadata, description/license and neighboring files supply no policy.
The limit is 64 KiB. Unknown Social Harness metadata, invalid/oversized behavior
or duplicate ids prevent startup.

## Decision reasons

The decision feed gives
the `decision`: `allowed` or `refused` for a message the agent sent, `allowed`
or `withheld` for one it received. `checkedRuleIds` lists the rules the check
judged; a rule that does not apply to the message, by its direction, is not listed.
Outgoing checks produce one row per recipient captured before
the check, including refused first messages without a conversation. Incoming
rows identify authors. Daemon-generated failures have no row. Retained identities survive restart without lookup.

`decidedBy` lists deciding rules in policy order:

| Reason | Rules |
| --- | --- |
| Rule violation | Violated rules. |
| Unavailable | Rules in checks that could not run, plus unavailable answers. |
| Unresolved evidence | Rules reporting unresolved evidence, including an unsure answer about an outgoing draft. |
| Allowed | Rules the provider was unsure about for an incoming message, otherwise none. |

Advice is a rule's registered feedback for a refusal, and its guidance, or feedback when it has none, for an allowed message.
Sources are `contract` or `fixed`. A rule's source grants no caller
privileges.

