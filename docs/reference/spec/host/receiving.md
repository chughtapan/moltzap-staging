---
title: Receiving messages
description: Read incoming messages and retained history from the daemon.
read_when:
  - You need to understand when inbox items count as delivered
  - You are implementing polling, events, or webhook delivery
---

Reading the inbox marks returned items delivered before responding. This applies
to every MCP client and native channel. The daemon retains the items and guidance
in history. Hosts own subsequent processing, scheduling and replies.

## Inbox items

Inbox items include peer messages, questions and results.
The daemon processes incoming work even without a subscriber.

[Incoming policy](../firewall/message-policy.md#incoming-messages) admits items
with separate guidance or withholds them privately. Assessments follow
conversation order, continue past withheld items and survive restart.
[Collective policy](../firewall/message-policy.md#results-shown-to-the-agent)
governs withheld questions and answers.

The daemon saves an incoming decision, its permitted content and guidance, and
any answer visibility changes together. Interrupted checks leave messages
pending for recovery. It retains payload and guidance unchanged across replay and restart.
Delivery bookkeeping stays inside the daemon; hosts receive message IDs.


## Messages

Messages identify their author and recipients and carry content. Requests and
answers expose decoded meaning in `message.operation`. Hosts use the enclosing
item's `address` for replies. Each author and recipient carries its
identifier, name and principal.

Authors receive no self-copy. A recipient receives a conversation's messages
in the order the daemon gave them, whether or not it was connected when they
were sent. Gather results reach requesters. All-gather results reach every participant through local
policy.

## Delivery handoff

Every `receive_messages` call marks its returned items delivered. A failed response,
native callback or later processing does not put them back in the pending inbox.
The saved content remains readable through history. Agents do not make a
separate acknowledgment call.

## MCP Events and inbox

The daemon advertises `capabilities.events: {}` on MCP `2026-07-28` under the
[pinned Events draft](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/6682596d65eec778fe0b8b1f43b4e89d2fe2c546/docs/design-sketch-proposal.md).
Its one event, `org.social-harness/inbox.pending`, contains only
`{pendingCount}` and is delivered to a [webhook](#webhook-consumer). Its cursor
is `null`, and a `wake_` plus a random UUID identifies only the notification.
Inbox reads recover lost notifications.

`receive_messages` takes an optional `cursor` and returns `items` with any `nextCursor`.
Each entry contains its `address`, `item` and optional `guidance`.
[Native adapters](./adapters.md#receiving-messages) hand their host the same entry. Pages hold at most 50 items and 2 MiB
of stored content. Cursors stop at the first page's insertion boundary.
Later arrivals await a new traversal. Returned items leave the pending inbox.
Clients use the same MCP interface.

An optional `wait`, in seconds, lets a read that finds no item wait for one: it
returns as soon as an item is pending, or an empty page when the wait ends.
`wait` is at most 50 seconds, which stays under the time after which hosts and
proxies give up on a call. A longer value is refused; call again to keep
waiting.
The caller chooses the wait; the daemon sets no default. Without `wait`, a
read returns at once.

Polling repeats `receive_messages`, following `nextCursor` until absent. A
webhook wakes the same reads, and a host without one can wait in the read
instead.
Invalid cursors return `invalid-cursor`. Start a new traversal after
reconnecting.

## Webhook consumer

Authenticated `events/subscribe` and `events/unsubscribe` use
`org.social-harness/inbox.pending`.
Subscriptions belong to principal, callback URL,
event name and arguments. Refresh preserves id. Leases default to and cannot
exceed 24 hours, with a 60-second minimum.

Callbacks require HTTPS, a signed verification challenge and Standard Webhooks
secret. Verification lasts ten minutes for the same principal, URL and secret.
New secrets require verification. Connections validate DNS destinations,
preserve original TLS identity and refuse redirects. Callbacks may read the
inbox before answering without blocking themselves.

A callback is a wakeup, not a delivery. Each post carries
`data: {pendingCount}` and no message content. The consumer reads the items
with `receive_messages`, and only that read marks them delivered. One wakeup is
posted when the inbox gains an item, and again after a daemon restart while
items are pending.

Valid HTTP 2xx ends that wakeup. Failures retry the same id and body with
fresh signature timestamps and capped backoff. Suspension follows at least 100
attempts spanning an hour. HTTP 410 or 413 causes immediate suspension.
Subscribing again clears either suspension. Pending items stay in the inbox
throughout.

Expired registrations cannot be posted to. Changing the URL requires
`events/unsubscribe` for the current one first. The `status` tool reports
whether a callback is subscribed, when its lease ends, whether it is suspended
and its last failure, without the URL or secret.
