---
title: Message firewall
description: How local rules govern what an agent sends and receives.
read_when:
  - You need to know what a firewall decision permits or refuses
  - You are configuring rules or changing who may edit them
---

The message firewall checks what an agent sends and receives against its
owner's rules and agreed norms. It receives a message, verified participants
and conversation evidence. It returns a judgment, the rules that
determined it, and registered advice the agent may receive.

The firewall owns the active rules, their assessment and authorized edits.
The daemon supplies the message and evidence, applies the decision to sending
or delivery, and retains the result. Storage preserves those records and rule
changes across restart.

The firewall is optional and experimental. Without a message-policy file,
`sharnessd` installs none. It checks messages between agents. The host owns
models, tools and the agent's private context.

## Decisions

Checks capture the active rules and mode. Enforcement refuses outgoing sends
or withholds incoming messages for reject or hold judgments. Guidance
mode records judgments and admits with permitted advice. Findings combine in
priority order:

1. A settled rule violation rejects.
2. Required missing evidence, unavailable checks or conflicting expectations hold.
3. Otherwise, admit.

Every active rule needs a finding. A pass cannot erase another rule's violation.
Checks use the captured candidate and context and cannot alter them. An empty
policy still validates the input and required evidence.

Rules without valid judgments or sufficient budget are unavailable.
Cancellation produces no decision.

### Natural-language judgments

Text rules describe when they apply and the conduct they require. Providers
assess local response expectations, then candidate compliance. Each judgment
accounts for the rule's scope.
Outgoing expectations use preceding evidence without the draft's text or identity.
Outgoing compliance consumes settled local expectations. Incoming compliance
assesses the peer sender and candidate against the rule without imposing the
local recipient's response expectations on that peer message. Incoming response
expectations are conditional on admission.

A judgment is uncertain if unresolved, below `minimumProbability`, or less
probable than another option. Probability sums and the selected option's
distance from the maximum tolerate 0.02; this does not settle a less probable
choice.

Valid judgment uncertainty admits with registered advice, including in enforcement.
Uncertain expectations or subjects create no obligation or reference. Findings retain
the provider's choice and probability, with registered guidance when present.
Settled violations retain their effect. Invalid output or
provider failure is unavailable; required missing evidence still holds.
Accepting nonempty rule text promises no assessment accuracy in any language.

## Incoming messages

Admitted peer content reaches the host unchanged, alongside guidance with
required, permitted, wait or none response expectations, registered instructions
and references to messages the host may see. Admission can therefore advise silence.
Guidance uses authorized rules and evidence, never private assessor evidence
or peer text promoted to instructions. Identities come from verified context:
recipients and answer members use `AgentCard`.

Guidance neither forces replies nor establishes speaking turns. Rule findings
on withheld items create no obligation for the agent.

## Outgoing messages

Refusal returns a rule-violation, unresolved-evidence or check-unavailable reason, check reference and permitted feedback. Its next action
is revise, wait, stop or seek approval; text errors also carry that action and
reference, without private evidence, credentials or raw provider output.

Refusal before admission creates no send intent. A revised invocation gets a new
assessment. The daemon neither changes the draft nor sends substitutes or
later approval. Accepted sends follow the
[send outcome contract](../host/messaging.md#failed-or-uncertain-sends).

An admitted uncertain judgment carries registered guidance without
subjects. Guidance mode also retains registered advice for settled violations. MCP results preserve that guidance.

## Evidence

A check receives the candidate, verified participants, rules, owner facts and
bounded conversation evidence once. The assessment applies to that captured
context and exact candidate, without a freshness comparison or
automatic reassessment when later context arrives. Acceptance preserves the
assessed recipients, content and message identity. Collectives checks that a
request remains open before accepting an answer.

In-flight checks keep their captured rules. Later checks use successful edits.

### Available history

Assessment captures preceding messages in conversation order, excluding the
candidate. The limit defaults to 256 entries, configurable from 1–1024.
Outgoing checks use the destination conversation; other conversations and host
private context are excluded. `earlierHistoryOmitted` alone causes no hold.
Exceeding the history window's separate 2 MiB budget, or unresolved outgoing
sends that may already have taken effect, creates an evidence gap. Required
missing evidence holds.

The firewall may use earlier messages it withheld or blocked when checking a
new message. Their content remains private and cannot impose an obligation as
though the agent had seen it. Guidance may quote the incoming message and
content shown to the agent, including answers and local results. Authored
content without proof that it came from an agent caller remains private evidence.

A missing subject or guidance exceeding the 2 MiB inbox-item budget makes
projection unavailable; guidance mode admits without advice. Oversized combined
collective advice is omitted in either mode because answers were individually
assessed. Rules needing other conversations, older messages or owner instructions
elsewhere require separately supplied evidence. Messages alone cannot establish
calendar changes, retract tool actions or reveal undisclosed collusion.

### Retained decisions

Each completed check identifies the message, evidence and policy it assessed.
Earlier decisions retain the rules and registered advice that applied at that
time. Only completed checks enter decision history. Completed decisions and guidance
survive restart and acknowledgment. Daemon-generated results
are stored without synthetic firewall checks. Private provider traces stay in
memory. Each completed record saves its decision summary with the assessed
policy revision, checked and deciding rule ids, origins and registered advice.
Reads return that saved summary without message content or private evidence.
An edit changes future checks.

## Collective operations

Each participant checks its own sends and views. Operation processing continues
independently of those judgments. Sharing a norm does not make complete
policies identical.

### Outgoing checks

| Action | Checked candidate |
| --- | --- |
| Multicast | Message and recipients. |
| Gather | Whole question and members, once before sending any request. |
| All-gather | Group question. |
| Answer | Response to the previously selected open request. |

Refused gather/all-gather creates no request or completion work. Admitted
gather requests carry exactly the checked content. Assessment uses destination
history although requests travel through separate direct conversations.

Answer selection stays fixed; the request must remain open at acceptance.
Expiry, close or a prior answer can refuse an otherwise approved answer.
The saved firewall judgment remains unchanged. Assessment consumes
the original deadline and request wait. Passing the deadline returns
`deadline-passed` without sending. The check's separate timeout can exceed the
request wait.

### Incoming checks

Requests assess the question and form the agent would see. Results assess each
peer answer, including peer text in invalid-answer reasons, using one evidence
capture and each answer's preceding history. Own/missing answers need no check;
the assessments take effect together. Endpoint-authored operation failures
bypass policy assessment and remain ordinary saved results. Peer field names
in their text are quoted as JSON.

Only admitted requests become answerable. A withheld all-gather request still
receives its result. Requests expiring before assessment retire unchecked;
those expiring during assessment are withheld. Neither reaches the host.

### Results shown to the agent

Membership and outcomes are admitted; local policy selects visible text without
changing the operation's result. Withheld
question/answer text becomes `{kind: "withheld", checkId, reason}`. Advice names
each affected member/check and any question checked with the result. Guidance
mode withholds no text.

A result checks remote question and answer text under one current policy.
Earlier inbox and history items keep their saved content and advice. Every
delivery path serves the saved result view. Later checks distinguish shown
request and answer text from withheld answers. History identifies admitted
results and failures as local Social Harness outcomes, ordered with the
surrounding messages.

## Message-policy file

`SHARNESS_MESSAGE_POLICY_FILE` names a readable regular JSON file, at most 1 MiB.
Startup loads fixed rules, owner facts, settings and provider before serving.
Initial rules seed only a missing editable policy; retained empty policies stay.

| Property | Requirement/default |
| --- | --- |
| `format` | Required: `org.social-harness/message-policy/v2`. |
| `mode` | Required: `guidance` or `enforcement`. |
| `text` | Optional: `fixedRules`, `initialRules` (`TextRule` arrays); `fixedBehaviors`, `initialBehaviors` (directory paths). Arrays default `[]`. |
| `decisionProvider` | Required with `text`: `{kind: "jev", model}` or `{kind: "unavailable"}`. |
| `textSettings` | Defaults: probability 0.5; 384,000 request bytes; 15 s per stage. |
| `settings` | Defaults: 150 s per assessment; 512,000 input bytes; 2,000,000 result bytes. |
| `facts` | Owner facts for every check; default `[]`. |
| `maximumHistoryRecords` | Default 256; [window bounds](#available-history). |
| `maximumConcurrentChecks` | Optional shared inference concurrency across revisions. |

Unknown properties and unsupported values fail. Trusted process composition may
supply its own provider and fixed text rules. The generated API reference owns
those callable types.

Jev is an optional TypeSafe service. It trims the key from
`SHARNESS_DECISION_PROVIDER_KEY_FILE` and retries connectivity, rate-limit and
server errors twice within each stage timeout. `unavailable` makes text checks
unavailable: enforcement holds, guidance admits. Without `text`, no provider/key
is used. Oversized catalog/history requests hold without dropping required
context. Bytes neither measure tokens nor reserve capacity; provider limits
remain unavailable assessments.

### Configured policy assets

The npm package includes ordinary rule documents and an example policy under
`config/firewall/`. Applications explicitly select that policy or copy selected
rule directories beside their own policy file. Every selected document uses the
same loader. An empty configuration adds no rules.

The example selects credential protection, useful acknowledgements, plain
language, authority-impersonation protection and policy-change rules. These are
natural-language policies assessed by the configured provider. They promise no
exact credential-pattern detection or complete acknowledgement vocabulary.
A sender-participation policy must name the intended verified identities in
application configuration. Identity verification itself belongs to the protocol.

The example uses guidance mode and an unavailable provider. Select a provider
and model to obtain assessments. Rule content, including the impersonation
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
| `guidance` | Optional `metadata.social-harness.guidance`. |
| `feedback` | Optional `metadata.social-harness.feedback`: `text`, `nextAction`. |

Other metadata, description/license and neighboring files supply no policy.
The limit is 64 KiB. Unknown Social Harness metadata, invalid/oversized behavior,
duplicate fixed ids or invalid editable rules prevent startup. Failures expose
path and `missing`, `too-large`, `not-a-file`, `frontmatter`, `schema`,
`duplicate-id` or `editable-limit`, never content.

Fixed behaviors cannot be edited or collide with retained editable ids. Initial
behaviors are validated every start but seed only missing policy.
Shared-norm `{behavior: "<document>"}` additions use this mapping; malformed
or oversized documents fail with `invalid-rule`.

### Policy-change rule asset

`config/firewall/rules/policy/no-policy-changes/BEHAVIOR.md` permits ordinary
shared-norm proposals while assessing peer attempts to bypass adopted rules.
Select its directory through `text.fixedBehaviors` to install it. The active policy governs rule changes. Fixed rules remain protected by
configuration.

### Shipped collective behaviors

| Directory under `config/firewall/rules/collectives/` | Norm |
| --- | --- |
| `one-gather` | Ask several members one question with gather/all-gather. |
| `announce-once` | Announce the outcome once to the group. |
| `result-settles` | Act on results instead of repeating the question. |
| `empty-acknowledgement` | An acknowledgment answering/asking nothing needs no response. |
| `facilitator` | Only the facilitator in owner facts starts questions for two or more members. |

Each participant supplies the facilitator's verified AgentId; names alone are
insufficient. `policy_update` changes no owner facts. Ordinary text budgets and
decisions apply.

<a id="editable-text-policy" />

## Editing rules

At most 256 editable rules have unique ids, text, an explicit `applies` direction,
and optional guidance and feedback. Editable ids
cannot reuse fixed rule ids. Initial rules seed a missing saved policy.
Every edit passes the configured approval step before changing rules.
The default approves automatically. A host can replace it with a confirmation
interaction. A proposed replacement cannot authorize itself. Fixed rules are supplied through configuration and
cannot be changed through `policy_update`.

Rule replacement and its change record save together. An edit either applies
in full or leaves the previous rules active.
`policy_read` reports editable and fixed rules with their current revision.

The complete replacement activates before return. Remove/add of one id is
atomic replacement; adding existing ids without removal, unknown removals or
duplicates fail. No-ops may retain the revision. Failures preserve rules and
activation history; edits leave connections running.

Errors report reason, available rule id and `["add", index]`/`["remove", index]`
path, without rejected text. Reasons: `invalid-rule`, `fixed-rule`,
`not-configured`, `approval-refused`, `build-failed`, `persistence-failed`. Invalid-rule problems are:

| Problem | Meaning |
| --- | --- |
| `duplicate-id` | Repeated or retained-and-added id. |
| `unknown-id` | Removal names no rule. |
| `editable-limit` | Result exceeds 256 rules. |
| `frontmatter` | Missing/invalid opening YAML frontmatter. |
| `schema` | Invalid rule/behavior format. |
| `too-large` | Behavior exceeds 64 KiB. |

Minimum rules, instructions, mandatory questions and structure must fit the
request budget. Otherwise updates fail `build-failed`; lowering budgets can
refuse startup with retained rules. Longer messages/evidence can still overflow.

`activeRevision` hashes the complete installation with SHA-256, including mode,
rules, settings and provider configuration, and is stable across identical restarts. Every
successful edit, including no-ops, retains Unix-millisecond activation time,
revision and added/removed ids; replacement appears in both. These records
contain no rule text or provider traces and survive restart even if no message
used the installation.
In-flight checks keep their installation; later checks use the replacement. Retained decisions identify installation,
provider, model and rule source after edits.

## Decision reasons

The decision feed separates `outcome` (`allow`, `reject`, `hold`) from
admitted/withheld `disposition`. `checkedRuleIds` lists every rule with a
finding.
Outgoing checks produce one row per verified recipient captured before
assessment, including refused first messages without a conversation. Incoming
rows identify authors. Daemon-generated failures have no assessment row. Retained identities survive restart without lookup.

`decidedBy` lists deciding rules in policy order:

| Reason | Rules |
| --- | --- |
| Rule violation | Violated rules. |
| Unavailable | Rules in checks that could not run, plus unavailable judgments. |
| Unresolved evidence | Rules reporting unresolved evidence or conflicting response expectations. |
| Admission | Uncertain rules, otherwise none. |

Advice comes from registered guidance or feedback; the registered next action is independent of rule representation.
Sources are `fixed` or `editable`. A rule's source grants no caller privileges. A norm proposed by a peer becomes
active only through an authorized policy change.

