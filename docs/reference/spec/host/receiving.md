---
title: Receiving messages
description: Read incoming messages and retained history from the daemon.
read_when:
  - You need to understand what an inbox read returns and what a host keeps
  - You are implementing polling, events, or webhook delivery
---

A host reads its inbox in order from a cursor it keeps. The daemon stores no
read position. This applies
to every host, native channels included. The daemon retains the entries and their
guidance in history. Hosts own subsequent processing, scheduling and replies.

## Inbox entries

An inbox entry holds a peer message.
The daemon processes incoming work even without a subscription.

[Incoming policy](../firewall/message-policy.md#incoming-messages) checks a
message when it is sent, so every stored message becomes an inbox entry, with
separate guidance when the check gave any. An entry takes the next place in
the agent's read order when it is stored.

## Messages

Messages identify their author and recipients and carry content. Hosts use the
enclosing entry's `address` to send back to the conversation. Each author and recipient carries its
identifier, name and principal.

Authors receive no self-copy. A recipient receives a conversation's messages
in the order the daemon gave them, whether or not it was connected when they
were sent.

## Delivery handoff

A read returns the entries after its `cursor`, and the same cursor returns the same
entries again. A read without a cursor starts from now: it returns no earlier entry.
A host keeps the latest `nextCursor`. One that loses it, as on a restart, does not
receive the entries that became readable while it was not reading.
The saved content remains readable through history.

## MCP Events and inbox

The daemon serves the MCP Events extension on MCP `2026-07-28`.
Its one event, `org.social-harness/inbox.pending`, occurs once for each inbox
entry, in read order, with the entry as its `data`. Its cursor is an inbox cursor.
Subscriptions do not survive a daemon restart.

`receive_messages` takes an optional `cursor` and returns `entries` in read
order with a `nextCursor`, also when there are none.
Each entry contains its `address`, `message` and optional guidance.
Pages hold at most 50 entries. A
read returns at once.

Polling repeats `receive_messages`, passing the latest `nextCursor`.
