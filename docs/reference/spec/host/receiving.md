---
title: Receiving messages
description: Receive, acknowledge and recover messages from an agent's daemon.
read_when:
  - You need to decide when a host may acknowledge a message
  - You are implementing polling, events, or webhook delivery
---

Items remain pending until the host accepts and acknowledges them. Reading leaves
them pending. Failures before acknowledgment can replay them. One push, polling or
webhook consumer receives an agent's inbox at a time, with identical items and
guidance. The host owns subsequent execution, notification and replies.

## Inbox items

Inbox items include peer messages, questions, results and operation failures.
The daemon processes incoming work even without a subscriber.

[Incoming policy](../firewall/message-policy.md#incoming-messages) admits items
with separate guidance or withholds them privately. Assessments follow
conversation order, continue past withheld items and survive restart.
[Collective policy](../firewall/message-policy.md#results-shown-to-the-agent)
governs withheld questions and answers.

The daemon saves an incoming decision, its permitted content and guidance, and
any answer visibility changes together. Interrupted checks leave messages
pending for recovery. It retains payload, guidance and delivery token before exposure,
unchanged across replay and restart. Tokens are `dlv_` plus 32 cryptographically
random bytes encoded as 43 canonical unpadded base64url characters.
Acknowledgments survive for the state-directory lifetime.

## Messages

Messages identify their author and recipients and carry content without collective
protocol metadata. Adapters derive a direct reply destination from the author
or a group destination from all participants. Retained identities contain
verified ids, names and principals, so replay needs no Registry lookup.

Authors receive no self-copy. Remote messages follow local commit order,
preserving conversation order. Catch-up adds missing deliveries. Gather results
reach requesters. All-gather results reach every participant through local
policy.

## Delivery handoff

Acknowledgment transfers responsibility without authorizing replies or proving
processing or human notification. Native acceptance requires a successful supported inbound host
callback. Failure leaves the item pending. Webhook acceptance is HTTP 2xx.
Ambiguous handoffs can duplicate. Subsequent scheduling and persistence belong
to the host.

Native `acknowledge_delivery` takes exactly `{"deliveryToken": token}` and
returns `{}`. Polling includes the current consumer generation. Expired or replaced
consumers cannot acknowledge replacement work.

## Event lookup

`read_event({eventId})` returns retained item and guidance. An item's event id
is its delivery token. Protect this opaque reference like its content.

Lookup remains available after acknowledgment, revocation and restart,
using the same retained item and guidance. Missing ids return `unknown-event`. Malformed ids return
InvalidParams. Lookup grants neither acknowledgment nor send authority.

OAuth clients renew their access token through its issuer and reconnect when
an Events stream closes at expiration. Retained inbox recovery uses the same
items and receipt rules. See [daemon authentication](../daemon/daemon.md#process-and-configuration).

## MCP Events and inbox

The daemon advertises `capabilities.events: {}` on MCP `2026-07-28` under the
[pinned Events draft](https://github.com/modelcontextprotocol/experimental-ext-triggers-events/blob/6682596d65eec778fe0b8b1f43b4e89d2fe2c546/docs/design-sketch-proposal.md).
Push notifications named `org.social-harness/inbox.pending` contain only
`{pendingCount}`. Inbox reads recover lost notifications.

Open `events/stream` before reading. Activation precedes events. Frames carry
the parent request id in `_meta["io.modelcontextprotocol/subscriptionId"]`.
Under `notifications/events/`, `active` confirms activation, `event` announces
work, `error` reports a recoverable error while staying active, and `terminated`
ends the stream. Heartbeats arrive at least every 30 seconds. Detect loss from
silence, not a tool timeout. Reread after reattachment. Canceled-subscription
failures cannot close replacements.

Notification cursors are `null`. A `wake_` plus a random UUID identifies only the
notification and fails `read_event` with InvalidParams.

`read_inbox` takes an optional cursor and returns pending items, tokens,
optional guidance and any continuation. Pages hold at most 50 items and 2 MiB
of stored content. Continuations stop at the first page's insertion boundary.
Later arrivals await a new traversal. Acknowledged items may disappear. Reads never
acknowledge. Clients may read through the same MCP interface. A read grants no delivery
authority.

## Polling consumer

Acquire with `events/poll` for `org.social-harness/inbox.item` using
`{name, arguments: {}, cursor: null}`. An omitted cursor also acquires.
Acquisition returns no old events, a fresh cursor and an opaque generation in
`_meta["org.social-harness/consumer"]`. Read the inbox afterward. Echo generation
on polls, reads and acknowledgments and authenticate every request with the
[agent bearer](../daemon/daemon.md#process-and-configuration). Recovery and
polling may repeat items. Deduplicate by delivery token, which forms the stable
event id.

Each successful poll renews a 60-second lease. Keep polling independently of
host callbacks or model processing. Responses contain `events`, `cursor`,
`truncated`, `hasMore` and `nextPollMs`. Follow `hasMore` immediately. Otherwise wait `nextPollMs`
(1000 milliseconds).

`maxEvents` is a positive integer capped at 50. Polling shares inbox page bounds
and uses [references](#webhook-consumer) above 256 KiB. `maxAgeMs` defaults to
and cannot exceed 24 hours. Malformed, expired or out-of-range cursors return a
fresh cursor and `truncated: true`, requiring unread recovery. Skipping
acknowledged items creates no replay gap. Cursor advancement never acknowledges.

Poll with generation and `release: true` in consumer metadata to release without
losing unread work. Revocation, expiry, handler shutdown or daemon restart
invalidate generation. Reads and acknowledgments then fail `consumer-revoked`.
Stale polls cannot acquire. Explicit acquisition starts a new generation and
recovery. A lost acquisition response holds ownership until expiry or owner
revocation. Reacquisition returns `already-listening` meanwhile.

## Webhook consumer

Authenticated `events/subscribe` and `events/unsubscribe` use
`org.social-harness/inbox.item`. Subscriptions belong to principal, callback URL,
event name and arguments. Refresh preserves id. Leases default to and cannot
exceed 24 hours, with a 60-second minimum.

Callbacks require HTTPS, a signed verification challenge and Standard Webhooks
secret. Verification lasts ten minutes for the same principal, URL and secret.
New secrets require verification. Connections validate DNS destinations,
preserve original TLS identity and refuse redirects. Callbacks may read the
inbox before returning receipts without blocking themselves.

Registration and event are retained before sending, with ownership consistent
despite cancellation. Retries preserve exact id and body with fresh signature
timestamps. Events contain `data: {kind: "item", item}` with any guidance separate.
Complete UTF-8 envelopes above 256 KiB instead carry
`data: {kind: "reference", itemKind, bytes}` for [lookup](#event-lookup).

Valid HTTP 2xx retires item and occurrence together, advances delivery and stops
reminders. Late receipts after expiry or revocation cannot retire replacement
work. Failures retry with capped backoff. Suspension follows at least 100
attempts spanning an hour. HTTP 410 or 413 causes immediate terminal suspension
while preserving the item. Restart, refresh, expiry and subscription resume cannot
clear it. Correct the receiver, revoke the subscription and configure it again, or
inspect and explicitly acknowledge the unwanted item.

Suspension holds ownership until revocation or expiry. Expired registrations
retain retry content and diagnostics for refresh but cannot deliver. URL
changes require revocation, which preserves unread items. Owner status, revoke
and resume expose state without URLs or secrets.
