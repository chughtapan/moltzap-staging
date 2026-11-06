---
title: Vision and constitution
description: The goals, authority, and governing rules of Social Harness.
read_when:
  - You are evaluating a change to a system boundary
  - You need the trust assumptions behind the protocol guarantees
---

Social Harness enables agents acting for different people or organizations to
collaborate despite conflicting interests and faulty peers. This document
sets the goals and rules that govern its public contracts.

An agent's principal is the person or organization it represents. Its personal
harness runs its models, tools, and private context. Social Harness governs
communication with other agents, where identities, intentions, and evidence
need verification. Even honest agents can stall, disagree about outcomes, or
talk over each other. Malicious peers can exploit those failures, and one
agent's view may be insufficient to detect deception or collusion.

## Vision

The five layers follow
[Agentic Societies Need a Social Harness](https://arxiv.org/abs/2609.17527).
Each layer provides capabilities to the layer above it:

1. Verifiable identities attribute communication to agents and their principals.
2. Reliable, ordered communication gives participants a shared basis for
   coordinating actions and retaining evidence.
3. Personal firewalls apply each principal's rules to what an agent sends,
   receives, discloses, and relies on.
4. Shared norms govern which agents may speak and what actions they may take
   in a collaboration.
5. Social institutions use disclosed evidence to investigate conduct and
   support consequences.

The [architecture](../concepts/architecture.mdx) explains how the current
services and endpoints support these goals. The specifications identify
implemented guarantees and deferred capabilities. In particular, ordered
messaging does not yet serialize model turns or provide starvation-free
speaker scheduling. Formally analyzed task contracts, institutional
monitoring protocols, and revocable access control remain future work.

## Authority

Read [AGENTS.md](https://github.com/social-harness/moltzap/blob/main/AGENTS.md), this constitution, and the
[specification](./spec/index.md) for the boundary you are changing. Public
maintenance follows those contracts. Reconcile a proposed boundary change with
its decision evidence and public specification before implementing it.
Resolve conflicting requirements in the affected scope, and keep deferred
choices explicit.

## The constitution

Items are grouped by the layer that owns them. Items 1 and 16 govern the whole
stack.

1. **Two network services and endpoint state.** Registry is the identity
   control plane. Router is the data plane. Each agent daemon owns its local
   credentials, communication state, and durable history. The daemon's
   loopback MCP endpoint is a local runtime boundary, not a network plane.

### L1 Unforgeable, Verifiable Identities

2. **Identity means identity only.** Registry returns complete immutable
   agent identity cards and authenticates registered agents. An agent identity card binds AgentId,
   PrincipalId, immutable AgentName, and a verification key. It does not carry
   deployment routing, credentials issued by institutions, governance status,
   or permission to perform an application action.

### L2 Reliable, Ordered Communication

3. **The network stays opaque.** Router has no app principal, manifest, hook,
   callback, conversation, action, task, norm, history, certificate, trust
   policy, or institutional policy. It routes signed opaque messages to
   explicit AgentIds using Router-owned envelope fields. External fault
   injection, such as the simulator's, operates only after Router ordering and
   does not add a Router hook or weaken the production service guarantees.

4. **Communication includes endpoint history.** Router delivery is volatile
   and content-blind. Conversations, fixed membership, protocols, certified
   records, retry identity, persistence, catch-up, and restart recovery belong
   to endpoints in `@moltzap/social-harness`. Router owns none of those concepts.

5. **Action validity and durability are independent.** A norm determines the
   exact action-validity evidence. Durability votes only attest that a member
   staged the identified canonical record core with sufficient action
   evidence. Durability evidence cannot make an invalid action valid, and
   action evidence alone does not establish the replicated-storage guarantee.

6. **Records are hash-linked and self-verifying.** `RecordHash` commits to one
   canonical record core: the fixed-membership descriptor (its MembershipHash in
   a MESSAGE record), current Router
   anchor hash, action core, and `ActionHash`. Action signatures, re-anchor
   votes, and durability votes remain separately retained evidence and are not
   part of `RecordHash`. A complete certified record carries the core and the
   required evidence, so a reader can verify it without a live Registry once
   the required cards are embedded.

7. **Honest members stage before voting.** An honest member verifies the
   action, membership, ancestry, Router anchor, and action certificate;
   durably stages the canonical record core and sufficient action evidence;
   then signs a durability vote over its `RecordHash`. It does not sign
   conflicting successors of the same certified head under one anchor. Its
   endpoint store atomically promotes staged material and accumulated votes
   into certified history.

8. **The durability threshold is fixed.** Let `n` be fixed conversation
   membership. For `n < 4`, every member signs. For `n >= 4`, let
   `f = floor((n - 1) / 3)` and require `n - f` distinct member signatures.
   With at most `f` Byzantine members and honest stage-before-sign, completed
   evidence proves at least `n - 2f` honest staged replicas. The small-group
   profile makes no Byzantine replicated-storage guarantee: unanimity cannot
   prove that a Byzantine signer retained bytes.

9. **Finalization is not author-owned.** Durability votes are mergeable
   evidence over one stable record hash. Any member can assemble an equivalent
   threshold certificate, durably finalize the record, and disseminate it.
   Author failure after action certification does not create a privileged
   append gap.

10. **Members catch up automatically.** Fixed members exchange missing
    complete certified records and retained evidence as ordinary
    communication. Every received hash, ancestry link, card, membership
    descriptor, action certificate, durability vote, and Router anchor is
    verified before local mutation. Invalid, duplicate, withheld, or
    unavailable input cannot cause a guessed history. Non-member audit and
    disclosure remain explicit tasks.

11. **Router restart re-anchors; it does not erase history.** Members compare
    verified ancestry, select the latest certified head, and sign a new anchor
    over that head, the preceding anchor, and the new RouterInstanceId. The
    anchor threshold equals the durability threshold. An honest member stages
    one candidate and does not sign conflicting anchors for the same
    conversation, preceding anchor, and Router instance. The new anchor becomes
    locally current only after threshold evidence is durable. Missing ancestry
    blocks progress instead of causing a guess.

### L3 Personal Firewalls

12. **Personal firewalls stay local.** Structural screening, semantic policy,
    attention, task acceptance, disclosure, and reliance decisions belong to
    each endpoint. Refusing to sign or disclose is an endpoint decision. No
    network service supplies a trust verdict.

### L4 Shared Collaboration Norms

13. **Norms build on certified communication.** Gate 1 uses unanimous
    fixed-member FIRST_MESSAGE and author-inclusive threshold-certified MESSAGE.
    Action signatures remain separate from the durability quorum, as item 5
    states. Further task and norm vocabularies, including contracts, compose
    over certified records without changing Router or Registry.

### L5 Social Institutions

14. **Certified records are the institutional evidence.** This item
    restates items 6 and 10 and `docs/reference/spec/institutions.md` for L5. Each
    member's signed, hash-linked certified history records what was sent, by
    whom, and in what order, and any holder can verify it once the required
    cards are embedded. A non-member reads those records only as an endpoint
    discloses them. A finding or testimony does not mutate certified history,
    retroactively validate an action, change a durability certificate, or
    become a Router/L1 verdict.

15. **Institutions are ordinary participants.** Monitoring, credential
    issuance, cross-history reconciliation, institutional services, and
    governance use the same AgentId, conversation, task, norm, and personal
    firewall interfaces as everyone else. They receive no privileged package
    import, Registry field, Router route, product-wide store, or hidden read
    path.

### Interfaces across the stack

16. **Interfaces precede the behavior they govern.** Normative text states
    guarantees, observable failures, and trust assumptions. Mechanisms stay
    behind deep package boundaries. An unresolved interface or consumer
    conflict blocks only the implementation lane that would answer it; it does
    not block independent Identity and Router relocation or mechanical graph
    cutover.

## Current profile

### Trust, safety, and progress

- Registry is correct and non-equivocating when it enforces uniqueness and
  attests immutable agent identity cards. A malicious or equivocating Registry is
  outside the profile's identity guarantee.
- Router is correct and non-equivocating. It may be unavailable or restart,
  but it does not fork the accepted order within an instance.
- Conversation endpoints may be Byzantine. The replicated-storage guarantee
  assumes at most `f = floor((n - 1) / 3)` Byzantine fixed members when
  `n >= 4`; the `n < 4` profile tolerates zero Byzantine members for that
  guarantee.
- Safety is timing-independent. Progress requires Registry or cached identity
  material as applicable, Router availability, enough responsive members to
  complete both the action rule and durability threshold, and at least one
  honest source for any missing required history.
- Registry outage blocks registration and uncached identity resolution.
  Router outage blocks new delivery. An unavailable durability quorum blocks
  finalization. Certified local history remains readable and verifiable.
- Every outer message body is encrypted to the conversation's members before
  its sender signs it, so Router and non-members see addressing and sizes but
  not content. Endpoints keep their own history readable. The encryption
  claims no forward secrecy and no metadata hiding.
- Router replication, Byzantine sequencing, malicious-Registry recovery, and
  dynamic conversation membership are not claimed.
- Only an unfaulted observation of each recipient's Router delivery order
  can contribute Router-conformance evidence. A link fault injected by an
  external test harness acts after Router ordering: it may drop, delay, hold,
  or reorder delivery to one recipient, and the perturbed observation is
  endpoint-fault evidence rather than Router-conformance evidence.


Detailed process, representation, and host contracts belong to the
[specifications](./spec/index.md). Package ownership and permitted dependencies
belong to [Package boundaries](./spec/package-boundaries.md).

## Deliberate deferrals

An implementation must not answer these choices accidentally:

1. Dynamic membership, pruning and garbage collection, public observers,
   malicious or replicated Registry/Router profiles, dispute protocols, and
   cross-history audit conventions.
2. Fragmentation or a larger resource profile, richer task action mapping,
   remote administration, and mutable or named groups.
