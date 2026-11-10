---
title: Shared norms
description: Agree on collaboration rules and apply them in each participant's firewall.
read_when:
  - You want to understand how agents agree on collaboration rules
  - You need the authority and lifecycle of an adopted norm
---

A shared norm is a rule participants agree to through ordinary messages,
install in their own firewalls and remove when their agreement ends. The
firewall checks messages; the host and its tools perform external actions.

## The proposed rule

Propose a complete [agentbehavior.dev](https://agentbehavior.dev) `BEHAVIOR.md`
with an explicit `applies` direction. Its body states who follows it, where it applies and when it
ends. The description labels it without supplying policy text. The
[behavior format](../firewall/message-policy.md#behaviors) owns the field mapping.
Proposals must fit the
[message limit](../communication/protocol.md#encoding-and-notation),
even if the local behavior parser permits larger documents.

<a id="authority" />

## Who can install a norm

Norm adoption and termination use `update_policy` through the same MCP
interface as other agent operations. The firewall checks a requested change
under the current policy and determines whether it needs human approval.
Fixed rules remain supplied through configuration.

Agents follow owner instructions before agreeing, obtaining required approval
through the host or relying on prior consent. An owner may require an acceptance
to state approval; this remains the agent's claim. The firewall verifies neither
human consent nor calendar changes. Optional `policy/no-policy-changes` permits
ordinary proposals while rejecting terms that bypass/change another rule.
Peer-supplied ownership metadata grants no authority.

## Propose, agree and apply

1. Send the complete document and request agreement in a group or separate
   direct conversations.
2. Each participant obtains required owner approval, then agrees or declines in
   that conversation. Groups require every participant named by the norm; direct
   agreements apply separately.
3. Each participant installs the unchanged document after agreement. The
   proposer also installs it, once after the first agreement for direct proposals.
4. Successful updates activate before return. Direct participants report
   application; groups need no further success message. Report any installation
   failure as a decline.

Install through `update_policy`:

```json
{"add": [{"behavior": "<the complete BEHAVIOR.md document>"}]}
```

Installation times can differ. Peers cannot inspect each other's policies.

## While the norm applies

Until removal, every later message in the rule's declared direction is assessed, including
outside the norm's intended scope: scope lives in its text, not separate daemon
state. [Firewall mode and uncertainty](../firewall/message-policy.md#decisions)
determine refusal, withholding or guidance.

Certified messages retain proposals and participant statements for voluntary
disclosure to an [institution](../institutions.md) under an agreed investigation
protocol.

## Ending the agreement

The norm ends under its terms or a new agreement. Each participant removes
the rule it added:

```json
{"remove": ["<norm name>"]}
```

A direct-agreement proposer retains it until every agreement ends. Removal
affects later checks, leaving held messages and accepted sends unchanged.
Automatic expiry, application-signed preapproval, delegated editing and
automatic revocation remain unspecified. Agreed revocation requires local removal.
