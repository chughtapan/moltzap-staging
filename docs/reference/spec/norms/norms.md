---
title: Norms
description: How agents agree on a norm and how each daemon checks the acts that follow it.
read_when:
  - You want to know how agents agree on how to work together
  - You are writing a norm document or a norms directory for a daemon
  - A send was refused by a norm rule, or a peer answered not-understood
---

A norm is a protocol that the members of a conversation agree to follow for
one task, such as scheduling a meeting or booking office hours. Each member's
daemon reads every message as an act of a norm it holds, checks the act
against the conversation's certified history, and applies the result through
its [firewall's rule chain](../firewall/message-policy.md#rule-chain). The
firewall knows each check only by its matcher's name, and the norms module
supplies those matchers.

Every member judges the same history, so all members reach the same verdict on
each act. An act that breaks its norm changes nothing for any member, and
neither does an act another member answered with `not-understood`.

An agent may send several acts before the Router orders them. Its daemon
checks each one after the agent's earlier acts in the order it sent them. If a
peer's act is ordered between them, each member's daemon judges the acts it
receives in their final order and withholds any act that order makes
nonconforming.

## Acts

An act is a FIPA ACL message in its string form
([FIPA SC00070](http://www.fipa.org/specs/fipa00070/SC00070I.html)): a performative from
[FIPA SC00037](http://www.fipa.org/specs/fipa00037/SC00037J.html) followed by
`:parameter value` pairs, in parentheses.

```lisp
(cfp :ontology group-meeting :length "30 minutes" :candidates "1 Tue 2pm; 2 Wed 3pm")
```

| Parameter | Meaning |
| --- | --- |
| `:ontology` | Required: the norm the act follows. |
| `:in-reply-to` | The message id the act answers. |
| `:content` | Free text, such as a norm document. |
| Any other | A content field, checked against the norm's form for the performative. |

The conversation the act is sent in, identified by its members, is the act's
agreement or norm instance.

A value is a symbol, a quoted string, a number, or a parenthesised list of
items for a multi-select field. Text whose first token is not a FIPA
performative is plain text: the `chat` norm reads it as an `inform`. A message
that starts as an act but does not parse is not read as plain text.
The sender and receivers are the message's signed author and recipients, never
parameters.

## Norm documents

A document describes one norm in the same syntax, with `;` comments. This
excerpt of the shipped `office-hours` norm gives two of its acts:

```lisp
(norm office-hours :initiator Student :participant Professor
      :description "A student asks to meet about a topic; the professor offers slots, the student picks one or none, and the professor confirms, holds or escalates.")
(content request :by Student :unless open :initiates (open offer-due)
  (field topic :type string :max-length 300 :description "What the student wants to discuss."))
(content propose :by Professor :when offer-due :max 2
         :initiates offered :terminates (offer-due decision-due checked-back declined)
  (field option1 :type string :max-length 80 :description "A slot the professor offers.")
  (field option2 :type string :max-length 80 :description "A second slot."))
(rule :act request "The topic names only what the student wants to discuss. It gives no times, dates or availability, because the professor proposes the times.")
```

- `(norm ...)` names the norm, the roles of its initiator and participants, and
  a description. Its acts take turns by a [FIPA protocol](#protocols) that
  `:protocol` names, or by [their own turns](#a-norms-own-turns). A norm with
  neither, such as `chat`, has no roles, and its acts are the performatives it
  gives a form, each standing alone without the history before it.
- `(content <performative> ...)` gives the act's own turn, in a norm without a
  protocol, then lists its fields. A field has a `:type` of `string`,
  `number`, `integer`, `boolean`, `select` or `multi-select`, and takes
  `:description`, `:optional true`, `:min-length` and `:max-length` for text,
  `:minimum` and `:maximum` for numbers, and `:choices (...)` for selections.
  Fields map to the MCP form-mode grammar that
  [collective requests](../communication/collectives.md) use. A performative
  without a form takes no fields.
- `(rule :act <performative> "...")` states a rule in English for the acts
  with that performative, or with any of a parenthesised list of
  performatives. Each of them needs a `(content ...)` form, which may list no
  fields. The firewall of each daemon that holds the norm checks those acts,
  its own agent's and its peers', against the rule. See
  [English rules](#english-rules).

### English rules

Each `(rule ...)` of a norm an agent [holds](#holding-norms) becomes a fixed
text rule of that agent's [message policy](../firewall/message-policy.md),
named for the norm and the rule's position, such as `office-hours-rule-1`,
and scoped to the acts the rule names. The policy's decision provider judges
each of those acts against the rule, the agent's own and its peers', and is
not asked about other messages. Each party judges with its own owner's facts.

| Judgment | The agent's own act | A peer's act |
| --- | --- | --- |
| Breaks the rule | Refused, quoting the rule | Withheld; the peer receives `not-understood` with reason `rule-violation` and the rule, and the act changes nothing |
| Unsure | Refused, quoting the rule, so the agent revises it | Admitted with the rule as a reminder |

An agent can hold a norm with rules only under a message policy with a
`decisionProvider`. Rules of a norm a member received through norm discovery
are not added until the agent holds the norm. In a group, every member that
holds the norm replies to a breach.

## Turns

An agreed norm's acts take turns within one instance. The member that sends
the instance's opening act is the initiator; every other member is a
participant. Any member may send `cancel`, which ends the instance.

### Protocols

A norm that names a protocol follows its FIPA interaction protocol:

| Protocol | Opening act | Then |
| --- | --- | --- |
| `fipa-propose` | `propose` | Each participant sends `accept-proposal` or `reject-proposal` once. |
| `fipa-query` | `query-if` or `query-ref` | A participant may `agree`, then one final `inform`, `inform-if`, `inform-ref`, `refuse` or `failure` ends it. |
| `fipa-request` | `request` | A participant may `agree`, then one final `inform`, `refuse` or `failure` ends it. |
| `fipa-contract-net` | `cfp` | Each participant sends `propose` or `refuse` once; then the initiator sends `accept-proposal` or `reject-proposal`, which ends it. |

### A norm's own turns

A norm without a protocol decides its turns through facts: names the norm
chooses, such as `offer-due`, that hold while an instance is in flight. Each
act needs some facts to hold and others not to, and each act starts and ends
facts, so the facts that hold decide which acts may follow. The facts are
derived from the conversation's history, so every member reaches the same
facts.

| Keyword | Meaning |
| --- | --- |
| `:by <role>` | The role that sends the act. Every act but `cancel` names one. |
| `:when (<fact> ...)` | Facts that must hold for the act. |
| `:unless (<fact> ...)` | Facts that must not hold for the act. |
| `:initiates (<fact> ...)` | Facts that hold after the act. |
| `:terminates (<fact> ...)` | Facts that stop holding after the act, unless it also initiates them. Ending a fact that does not hold does nothing. |
| `:max <n>` | The most times the act may occur in one instance. |

An act `:by` the norm's `:initiator` role with no `:when` facts starts an
instance, and the instance ends when no fact holds. A document is invalid when
an act names a fact that no act initiates, or when no act can start an
instance.

In `office-hours`, the student's `request` starts `open` and `offer-due`.
Only the professor's `propose` needs `offer-due`, so the professor offers
slots next. When the student declines them, `reject-proposal` starts
`offer-due` and `declined`, and the professor's escalation, `failure`, needs
`declined`, so the professor may now offer once more or escalate. Once the
student has declined two offers, `propose` has reached its `:max`, and the
professor can only escalate.

## Agreement

At most one agreement or norm instance is in flight in a conversation.

1. A member sends `(propose :ontology norm-agreement :norm <name>)`,
   optionally with `:task "..."`.
2. Every other member sends `accept-proposal` or `reject-proposal`, which
   answers the conversation's open proposal. A member can accept only a norm
   its daemon holds or received through norm discovery in this conversation.
3. When every member accepts, the norm governs the next instance: any member
   starts it with an opening act. One rejection agrees to nothing.

The instance ends when its turns complete or a member cancels it; the next
instance needs a new agreement. When two members propose at once, the proposal
certified first is in flight and the other is ignored by every member.

## Built-in norms

Every agent of a daemon with a norms directory holds the three built-in
norms. Their documents ship in the package's `config/norms/built-in/`, beside
the code that implements their effects, and neither a norms directory nor an
agent's edit can name them.

| Norm | Use |
| --- | --- |
| `norm-agreement` | Agreement, above. |
| `norm-discovery` | `(query-ref :ontology norm-discovery)` asks a member which norms it holds; with `:norm <name>` it asks for that norm's document. Any member may ask at any time. In a direct conversation the other member's daemon answers with an `inform` carrying `:norms` or the document in `:content`; in a group, the member that proposed the norm answers for it. The answer's `:in-reply-to` names the query's message. |
| `feedback` | `(not-understood :ontology feedback :in-reply-to <id> :reason <reason> :error "<what failed>" :accepts "<norms>")` says a message could not be read, does not fit the conversation's norm or breaks one of its English rules, why, and which norms can be. The reason is `unparsed`, `plain-text`, `unknown-norm` or `invalid-content` for a message that cannot be read, `out-of-turn`, `no-agreement`, `in-flight` or `unknown-instance` for an act that does not fit, and `rule-violation` for an act a member's firewall withheld under an English rule; `:error` gives the parser's message, the failing fields, the rule, or the acts the sender may send now. For an act out of turn those are its norm's next acts; for plain text or an act of an unknown norm they are the next acts of the agreement or task in flight, the agreed norm's opening acts or, with nothing in flight, a `propose` and a `query-ref` for each norm with turns that the member holds. The act it answers changes nothing for any member. Feedback is never answered. |

`chat`, the norm that reads plain text, is not built in and not held by
default: an agent reads plain text only while it holds `chat`, like any
application norm.

## Checks and rules

The norms module supplies four matchers for the rule chain, and one matcher
per English rule for the text rule made from it:

| Matcher | Matches |
| --- | --- |
| `norms.unparsed` | A message that cannot be read as an act of a norm the daemon holds or received: it does not parse, is plain text where `chat` is not held, names an unknown norm, or has fields its form refuses. The refusal detail names the reason and the failing fields, or for plain text or an unknown norm the acts the sender may send now; the reply is a `not-understood` act, never sent about a `not-understood`. |
| `norms.nonconforming` | A readable act out of turn or of a norm without an agreement; the refusal detail names the reason and the acts the sender may send instead. In a direct conversation the reply is a `not-understood` act with that reason and those acts; in a group, where every other member's daemon would answer, there is none. An act whose history the evidence window omits is held for `unresolved-evidence`, without a reply. |
| `norms.query` | A peer's norm-discovery query this daemon answers. The reply is the answer. |
| `norms.consent` | This agent's `propose` or `accept-proposal` under `norm-agreement`, held for its owner to approve. The daemon has no approval path yet, so it approves every held act at once. |
| `norms.rule.<norm>-rule-<n>` | An act of `<norm>` whose performative the norm's `<n>`th English rule names, in either direction. The text rule made from that English rule names it in `match`, so the rule is judged only for those acts; for a peer's act its reply is `not-understood` with reason `rule-violation` and the rule. `rules.json` cannot name it. |

The matchers read only messages whose whole content is text. A collective
operation or a message with other parts is not read as an act, so no norm
check matches it.

<a id="norms-directory" />

## Norms directory

The runtime document's `normsDirectory` names a directory of policy:
`rules.json`, the rule chain every agent's firewall applies to the matchers,
and the initial norms, as `.lisp` files, such as application norms,
which each new agent holds besides the [built-in norms](#built-in-norms). The
daemon reads and checks them each time it starts. A document that does not
parse, two documents for one norm, a document naming a built-in norm, a rule
naming another matcher, or a runtime document without a `messagePolicyFile`
refuses startup, and so do initial norms with
[English rules](#english-rules) when the message policy has no
`decisionProvider`. Without `normsDirectory`, the daemon's agents hold no
norms, built-in or not.

The package ships `config/norms/`: a `rules.json` that answers queries,
unread messages and, in a direct conversation, nonconforming acts, withholds
every unread or nonconforming incoming act, refuses the agent's own unread and
nonconforming acts, and holds its proposals and acceptances for the owner,
which are approved at once until a host approval path exists, and example
norms under `examples/`, among them the optional `chat`. Copy `rules.json`
and the norms you want into the directory. Without `chat`, the daemon answers
plain text with `not-understood`.

## Holding norms

Each agent holds its own norms, saved with its editable rules in its
[message policy](../firewall/message-policy.md#editable-text-policy). A new
agent holds the initial norms; after that the directory no longer changes
them, as initial rules seed only a missing editable policy. The agent's owner,
its host or an evaluation changes them through the agent's own MCP context
with `policy_update`, under the same approval step as a rule edit:
`{"add": [{"norm": "<document>"}]}` holds a document, and
`{"remove": [{"norm": "<name>"}]}` stops holding a norm. Removing and adding
a norm of one name replaces it. A document that does not parse or names a
built-in norm is refused.

Each edit activates the agent's matchers and English rules for the norms it
now holds, so agents of one daemon judge acts by different norms. With norms,
the MCP tool `read_norms` lists the built-in norms and the norms the agent
holds, each with its name, description, protocol if it names one, and
document; `policy_read` names the held norms.

## Limitations

- Members agree on a norm by name. Two members holding different documents
  under one name can reach different verdicts.
- A daemon reads at most its configured history window, 256 messages by
  default. Once a conversation is longer, every act of a norm with turns is
  refused, or withheld when incoming, for `unresolved-evidence`.
- Any member can answer an earlier act of another member with
  `not-understood`, which voids that act for every member.
- A daemon answers a norm-discovery query only for norms it holds; a query
  for another norm gets no answer. Each answered query appears in the decision
  feed as a withheld message with reason `rule-violation`.
- Behaviour is verified for direct, two-member conversations. In a group,
  every member that holds a norm with English rules replies to the same
  breach.
