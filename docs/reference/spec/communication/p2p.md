---
title: P2P mode
description: Daemons owned by different people exchange messages through a Router they trust.
read_when:
  - You run a daemon that exchanges messages with other people's daemons
  - You operate a Router, or need to know what it is trusted for
---

In P2P mode each owner runs their own daemon, with its own database and its
own firewall. Daemons do not connect to each other. Each connects to one
Router, which checks who is sending, orders each conversation and holds
messages until the recipient's daemon collects them.

## Parties and trust

| Party | What it does | Trusted for |
| --- | --- | --- |
| Identity provider | An OAuth 2.0 authorization server that owners and daemon operators sign in to | Saying who a token belongs to |
| Daemon | Hosts its owner's agents, applies their firewall, keeps their history | Its own agents' messages and its own database |
| Router | Admits daemons, names agents, orders and relays messages | The sender of each message, the order within a conversation, delivery, and the content it relays |

A daemon trusts the Router the way a client trusts its daemon in
[client/server mode](../client-server.md). A Router that misbehaves can read,
drop, delay, reorder or forge messages. Nothing in a message lets a third
party check who wrote it.

## Signing in

Every party proves who it is with an OAuth access token from the identity
provider.

- **Owner to daemon.** A host presents the owner's token to the daemon, as in
  every mode. See [MCP authentication](../daemon.md#mcp-authentication).
- **Daemon to Router.** A daemon's operator authorizes the daemon once with
  the device authorization grant. The daemon keeps the refresh token, sealed
  in its database, and obtains access tokens whose audience is the Router.
  It can then send and receive while its operator and its owners are offline.

The Router accepts tokens from one configured issuer. It identifies a daemon
by the token's issuer and subject.

## Agents and names

A daemon registers each agent it hosts with the Router: it gives a name, and
the Router returns the agent's identifier. A name belongs to the first daemon
that registers it. Registering the same name again from the same daemon
returns the same identifier, so a daemon can repeat a registration it is
unsure completed.

The Router answers lookups by name or identifier with the agent's name and
identifier. An agent has no key and no signed identity document.

## Sending

A daemon sends a message by posting it to the Router with its access token.
The message names its author, its recipients and a message identifier, and
carries content. The Router:

1. Refuses the message unless the author is an agent the calling daemon
   registered.
2. Refuses it unless every recipient is a registered agent.
3. Gives it the next position in its conversation. A conversation is the set
   of its author and recipients.
4. Stores one copy for each recipient's daemon and answers with the position.

Sending the same author and message identifier again with the same content
returns the first answer. Sending them with different content is refused. A
daemon that does not get an answer repeats the request; it cannot create a
second message that way.

## Receiving

A daemon asks the Router for messages addressed to its agents after a cursor.
The request waits when there are none. Each message arrives with its
conversation position. The daemon stores what it receives, then asks again
with the new cursor, which tells the Router the earlier messages can be
discarded.

The Router keeps an undelivered message for a configured time. A daemon that
stays away longer misses those messages, and its history reports the gap in
that conversation.

## Order and history

The position the Router assigns is the order every member sees. Each daemon
keeps its own copy of the conversations its agents take part in; the Router
keeps a message only until it is collected. Reading history never contacts the
Router.

## Groups and questions

A message to a group reaches every member, and
[questions and answers](./collectives.md) work as they do for two agents. The
Router treats them as messages; the daemons interpret them.

## After a restart

The Router stores accepted messages and registered names durably, so a
restart loses neither. A daemon stores each message before it advances its
cursor, so a restart repeats at most the last request.

## Not provided

- Evidence of authorship or order that someone other than the Router can
  check.
- Protection from a Router that reads or alters what it relays. Messages
  travel over TLS and are readable by the Router.

## Versions

Daemons and the Router speak one wire version. A request with another version
is refused, and the answer names the version the Router speaks.
