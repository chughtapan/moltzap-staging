---
title: Message firewall
description: How an agent's message policy governs what it sends and receives.
read_when:
  - You need to know what a firewall decision permits or refuses
  - You are configuring rules or changing who may edit them
---

The message firewall checks what an agent sends and receives against its
owner's rules and agreed norms. It receives a message, its participants
and conversation evidence. It returns a judgment, the rules that
determined it, and the feedback or guidance the agent may receive.

The firewall owns the message policy in force, its checks and authorized
edits. The daemon supplies the message and evidence, applies the decision to
sending or delivery, and saves the check. Storage preserves those records and
rule changes across restart.

The firewall is optional and experimental. Without a message-policy file,
`sharnessd` installs none. It checks messages between agents. In
[P2P mode](../communication/p2p.md) each agent has its own firewall, and in
[client/server mode](../client-server.md#firewall-and-norms) only the server
agent has one. The host owns models, tools and the agent's private context.

## Chain

The chain applies before the text rules. Like a Google Cloud firewall
policy, it is an ordered list of chain rules. Each chain rule has an `id`, the direction it
`applies` to, the name of a matcher and an action. The first rule whose matcher
matches a message decides it, and a message no rule matches goes on to the
text rules. Trusted process composition supplies the rules and registers their
matchers by name; a rule naming an unregistered matcher prevents startup.

| Action | Outgoing draft | Incoming message |
| --- | --- | --- |
| `allow` | Sent without the text rules. | Delivered without the text rules. |
| `deny` | Refused. | Withheld. |
| `reply` | Refused. | Withheld, and the matcher's reply goes to the sender. |
| `hold` | Goes on to the next rule once the owner approves it; otherwise refused with `owner-approval-required`. | Not allowed. |

A reply is an ordinary message from this agent to the conversation of the
withheld message, sent once for that message: its message id is derived from
the withheld message, so checking that message again sends no second reply. A collective result's withheld
parts get no reply. The daemon does not yet ask the owner about a held draft,
so every held draft is approved.

A match refuses for a rule violation unless the matcher reports missing
history (`unresolved-evidence`) or cannot decide (`check-unavailable`). A
matcher that fails counts as `check-unavailable`. The refusal names the
deciding rule and carries the matcher's detail. A chain decision is saved
like a check of the text rules, and its `decidedBy` names the rule with
`from: "chain"`.

## Decisions

Text rules decide a message no chain rule matched. Checks capture the active
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

Text rules describe when they apply and the conduct they require. A fixed
rule may also name a registered matcher in `match`: the provider is asked
about the rule only for messages that matcher matches, and the rule does not
apply to the rest. A rule naming an unregistered matcher prevents startup,
and editable rules name none. The daemon scopes each
[norm rule](../norms/norms.md#english-rules) this way to the acts it governs.

For each rule, the provider answers one question about the message: does it
break the rule? The answer is `compliant` or `violation`, with a probability for each.
An outgoing draft is judged as the local agent's; an incoming message is judged
as its peer sender's.

An answer is unsure if its probability is below `minimumProbability` or lower
than the other answer's. Probability sums and the selected answer's
distance from the maximum tolerate 0.02; this does not settle a less probable
answer.

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
instruction or exposes the check's private evidence. In a collective result's
guidance, each response's entry names its `recipient` by `AgentCard`. An inbox
entry the provider settled carries no guidance.

A text rule whose `action` is `reply` answers each message it withholds for a
rule violation: the sender receives the rule's registered feedback text the way
a chain rule's reply arrives. When the rule's `match` matcher gives a reply for
the message, such as a norm's `not-understood`, the sender receives that reply
instead. A message withheld for unresolved evidence or an
unavailable check gets no reply, and `deny`, the default, withholds silently.

## Outgoing messages

Refusal returns a rule-violation, unresolved-evidence, check-unavailable or
owner-approval-required reason, the check id, the deciding chain rule,
the registered feedback of the deciding text rules and the chain matcher's
detail. Its error message carries the same fields, without private evidence,
credentials or raw provider output.

A refused draft creates no send. A revised invocation gets a new
check. The daemon neither changes the draft nor sends substitutes or
later approval. Accepted sends follow the
[send outcome contract](../host/messaging.md#failed-or-uncertain-sends).

An unsure answer refuses the draft as unresolved evidence with the rule's
feedback, so the agent revises it before sending. An allowed send carries no
guidance.

## Evidence

A check receives the message, its participants, rules, owner facts and
bounded conversation evidence once. The check applies to that captured
evidence and exact message, without a freshness comparison or an
automatic second check when later evidence arrives. Acceptance preserves the
checked recipients, content and message identity. Collectives checks that a
request remains open before accepting a response.

In-flight checks keep their captured rules. Later checks use successful edits.

### Available history

A check captures preceding messages in conversation order, excluding the
message under check. The limit defaults to 256 entries, configurable from 1–1024.
Outgoing checks use the destination conversation; other conversations and host
private context are excluded. `earlierHistoryOmitted` alone causes no hold.
An incoming check sees only the ordered history before its message. Exceeding the
history window's separate 2 MiB budget creates an evidence gap. Required
missing evidence holds.

The firewall may use earlier messages it withheld or blocked when checking a
new message. Their content remains private, as though the agent had not seen
it. Authored content without proof that it came from an agent caller remains
private evidence.

Guidance exceeding the 2 MiB inbox-entry budget makes the check unavailable,
which holds the message. Oversized combined guidance of a collective result is
omitted because its responses were checked one by one. Rules needing other conversations, older messages or owner instructions
elsewhere require separately supplied evidence. Messages alone cannot establish
calendar changes, retract tool actions or reveal undisclosed collusion.

### Retained decisions

Each completed check identifies the message, evidence and policy it checked.
Earlier decisions retain the rules and registered advice that applied at that
time. Only completed checks enter decision history. Completed decisions and
guidance survive restart and delivery. Daemon-generated results
are stored without synthetic firewall checks. Private provider traces stay in
memory. Each completed record saves its decision summary with the revision of
the policy it applied, checked and deciding rule ids, origins and registered advice.
Reads return that saved summary without message content or private evidence.
An edit changes future checks.

## Collective operations

Each participant checks its own sends and views. Operation processing continues
independently of those judgments. Sharing a norm does not make complete
policies identical.

### Outgoing checks

| Action | What is checked |
| --- | --- |
| Multicast | Message and recipients. |
| Gather | Whole question and members, once before sending any request. |
| All-gather | Group question. |
| Collective response | Response to the previously selected open request. |

Refused gather/all-gather creates no request or completion work. Allowed
gather requests carry exactly the checked content. The check uses destination
history although requests travel through separate direct conversations.

The request a response names stays fixed; the request must remain open at
acceptance. Expiry, close or a prior response can refuse an otherwise allowed
response. The saved firewall judgment remains unchanged. The check consumes
the original deadline and request wait. Passing the deadline returns
`deadline-passed` without sending. The check's separate timeout can exceed the
request wait.

### Incoming checks

A collective request's check reads the question and form the agent would see.
A collective result's check reads each peer response, including peer text in
the reason of an invalid outcome, using one evidence capture and each
response's preceding history. The agent's own response and a missing one need
no check; the checks take effect together. Peer field names in their text are
quoted as JSON.

Only allowed requests can be responded to. A withheld all-gather request still
receives its result. Requests expiring before their check are withheld without a check;
those expiring during it are withheld. Neither reaches the host.

### Results shown to the agent

Membership and outcomes are allowed; the message policy selects visible text
without changing the collective result. Withheld
question or response text becomes `{kind: "withheld", checkId, reason}`. Guidance
names each recipient, check and question the provider was unsure about.

A collective result's check reads remote question and response text under one
current policy. Earlier inbox entries and history records keep their content
and guidance. Every delivery path serves the stored inbox entry. Later checks
distinguish shown request and response text from withheld responses. History
identifies allowed results and failures as local Social Harness outcomes,
ordered with the surrounding messages.

## Message-policy file

The settings file's `messagePolicyFile` names a readable regular JSON
file, at most 1 MiB. Startup reads fixed rules, owner facts, settings and provider
credentials before serving.
Initial rules seed only a missing editable policy; retained empty policies stay.

| Property | Requirement/default |
| --- | --- |
| `format` | Required: `org.social-harness/message-policy/v2`. |
| `text` | Optional: `fixedRules`, `initialRules` (`TextRule` arrays); `fixedBehaviors`, `initialBehaviors` (directory paths). Arrays default `[]`. |
| `decisionProvider` | Required with `text` and for held norms with English rules: `{kind: "jev", model}`. |
| `textSettings` | Defaults: `minimumProbability` 0.8, below which an answer is unsure; 384,000 request bytes; 15 s per request. |
| `settings` | Defaults: 150 s per check; 512,000 input bytes; 2,000,000 result bytes. |
| `facts` | Owner facts for every check; default `[]`. |
| `maximumHistoryRecords` | Default 256; [window bounds](#available-history). |
| `maximumConcurrentChecks` | Optional shared inference concurrency across revisions. |

Unknown properties and unsupported values fail. Trusted process composition may
supply its own provider and fixed text rules. The generated API reference owns
those callable types. The norms an agent holds add their
[English rules](../norms/norms.md#english-rules) after the file's fixed rules,
so a daemon whose norms have rules needs `decisionProvider` even when the file
has no `text`.

Jev is an optional TypeSafe service. It trims the key from
the settings file's `decisionProviderKeyFile` and retries connectivity, rate-limit and
server errors twice within each request timeout. Without `decisionProvider`, no
provider or key is used. Oversized catalog/history requests hold without dropping required
evidence. Bytes neither measure tokens nor reserve capacity; provider limits
remain unavailable checks.

### Configured policy assets

The npm package includes ordinary rule documents and an example policy under
`config/firewall/`. Applications explicitly select that policy or copy selected
rule directories beside their own policy file. Every selected document uses the
same loader. An empty configuration adds no rules.

The example selects credential protection, useful acknowledgements,
authority-impersonation protection and policy-change rules. These are
text rules judged by the configured provider. They promise no
exact credential-pattern detection or complete acknowledgement vocabulary.
A sender-participation policy must name the intended agents by identifier in
application configuration. The [daemon](../identity/identity-and-attribution.md#attribution),
not the firewall, establishes who sent a message.

The example selects Jev's `jev-1.13.0` model, so it needs
`decisionProviderKeyFile`. Rule content, including the impersonation
policy, is active only when explicitly configured. Fixed rules remain protected
by edit authorization regardless of their content.

## Behaviors

A directory's `BEHAVIOR.md` supplies one rule. Paths are policy-file-relative;
absolute paths, `..` and escaping symlinks fail. Startup loads inline rules,
then fixed and initial behaviors.

| Rule field | Document source |
| --- | --- |
| `id` | Frontmatter `name`. |
| `text` | Trimmed body. |
| `applies` | Required `metadata.social-harness.applies`: `incoming`, `outgoing` or `both`. |
| `guidance` | Optional `metadata.social-harness.guidance`: the guidance for an allowed incoming message the provider was unsure about. |
| `feedback` | Optional `metadata.social-harness.feedback`: `text`. |
| `action` | Optional `metadata.social-harness.action`: `deny` or `reply`. |

Other metadata, description/license and neighboring files supply no policy.
The limit is 64 KiB. Unknown Social Harness metadata, invalid/oversized behavior,
duplicate fixed ids or invalid editable rules prevent startup. Failures expose
path and `missing`, `too-large`, `not-a-file`, `frontmatter`, `schema`,
`duplicate-id` or `editable-limit`, never content.

Fixed behaviors cannot be edited or collide with retained editable ids. Initial
behaviors are validated every start but seed only missing policy.
`update_policy`'s `{behavior: "<document>"}` additions use this mapping; malformed
or oversized documents fail with `invalid-rule`.

### Policy-change rule asset

`config/firewall/rules/policy/no-policy-changes/BEHAVIOR.md` permits ordinary
shared-norm proposals while checking peer attempts to bypass adopted rules.
Select its directory through `text.fixedBehaviors` to install it. The active policy governs rule changes. Fixed rules remain protected by
configuration.

### Shipped collective behaviors

| Directory under `config/firewall/rules/collectives/` | Rule |
| --- | --- |
| `one-gather` | Ask several members one question with gather/all-gather. |
| `announce-once` | Announce the outcome once to the group. |
| `result-settles` | Act on results instead of repeating the question. |
| `facilitator` | Only the facilitator in owner facts starts questions for two or more members. |

Each participant supplies the facilitator's AgentId; names alone are
insufficient. `update_policy` changes no owner facts. Ordinary text budgets and
decisions apply.

<a id="editable-text-policy" />

## Editing rules

At most 256 editable rules have unique ids, text, an explicit `applies` direction,
and optional guidance, feedback and action. Editable ids
cannot reuse fixed rule ids. Initial rules seed a missing saved policy.
Every edit passes the configured approval step before changing rules.
The default approves automatically. A host can replace it with a confirmation
interaction. A proposed replacement cannot authorize itself. Fixed rules are supplied through configuration and
cannot be changed through `update_policy`.

An agent's editable policy also holds its
[norms](../norms/norms.md#holding-norms). `update_policy` adds a norm with
`{norm: "<document>"}` in `add` and stops holding one with `{norm: "<name>"}`
in `remove`; removing and adding one name replaces it. Each activation adds
the held norms' English rules to the fixed rules, so they refuse edits like
other fixed rules.

Rule replacement and its change record save together. An edit either applies
in full or leaves the previous rules and norms active.
`read_policy` reports editable and fixed rules and the held norms' names with
their current revision.

The complete replacement activates before return. Remove/add of one id is
atomic replacement; adding existing ids without removal, unknown removals or
duplicates fail. No-ops may retain the revision. Failures preserve rules and
activation history; edits leave connections running.

Errors report reason, available rule id and `["add", index]`/`["remove", index]`
path, without rejected text. Reasons: `invalid-rule`, `fixed-rule`,
`not-configured`, `approval-refused`, `build-failed`, `persistence-failed`. Invalid-rule problems are:

| Problem | Meaning |
| --- | --- |
| `duplicate-id` | Repeated or retained-and-added rule id or norm name. |
| `unknown-id` | Removal names no editable rule or held norm. |
| `editable-limit` | Result exceeds 256 rules or 256 norms. |
| `frontmatter` | Missing/invalid opening YAML frontmatter. |
| `schema` | Invalid rule/behavior format. |
| `too-large` | Behavior or norm document exceeds 64 KiB. |
| `invalid-norm` | The daemon has no norms directory, or the document does not parse or names a built-in norm. |

Minimum rules, instructions, mandatory questions and structure must fit the
request budget. Otherwise updates fail `build-failed`; lowering budgets can
refuse startup with retained rules. Longer messages/evidence can still overflow.

`activeRevision` hashes the complete message policy in force with SHA-256, including
rules, settings and provider configuration, and is stable across identical restarts. Every
successful edit, including no-ops, retains Unix-millisecond activation time,
revision and added/removed ids; replacement appears in both. These records
contain no rule text or provider traces and survive restart even if no message
used that revision.
In-flight checks keep their revision; later checks use the replacement. Saved decisions identify the revision,
provider, model and rule source after edits.

## Decision reasons

The decision feed separates the `judgment` (`allow`, `reject`, `hold`) from
the `decision`: `allowed` or `refused` for a message the agent sent, `allowed`
or `withheld` for one it received. `checkedRuleIds` lists the rules the check
judged; a rule that does not apply to the message, by its direction or its
`match`, is not listed.
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
| Chain decision | The chain rule that decided, including a held draft the owner has not approved. |

Advice is a rule's registered feedback for a refusal, and its guidance, or feedback when it has none, for an allowed message.
Sources are `chain`, `fixed` or `editable`. A rule's source grants no caller privileges. Agents agree on a norm in the
conversation itself; see [norms](../norms/norms.md#agreement).

