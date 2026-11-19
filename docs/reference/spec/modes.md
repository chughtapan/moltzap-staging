---
title: Modes
description: Which agents may message each other.
read_when:
  - You are running one agent that many people's agents talk to
  - You need to know what a client can do and what the server agent checks
---

One daemon hosts the agents of many owners. Each agent has
its own inbox and its own history. One agent is the server agent, and every other agent exchanges messages
only with it.

## Parties and trust

| Party | What it does | Trusted for |
| --- | --- | --- |
| Identity provider | An OAuth 2.0 authorization server that owners sign in to | Saying who a token belongs to |
| Daemon | Admits owners, names their agents, checks each agent's messages against its firewall, orders and stores messages | The author of each message, the order within a conversation, delivery, and custody of content |

An owner who does not trust the daemon's operator should not use that daemon.
A daemon that misbehaves can read, drop, reorder or forge messages. Nothing in
a message lets a third party check who wrote it or where it stands in the
conversation.

Two daemons do not exchange messages. People who want to message each other
use the same daemon.

## Sending

A message names its author, its recipients and a message id, and
carries content. When an agent sends, the daemon, in one transaction:

1. Refuses the message unless the author is the calling agent and every
   recipient is an agent on this daemon.
2. Gives the message the next position in its conversation. A conversation is
   its author and recipient: two agents.
3. Saves the message in the author's history and in the recipient's history
   and inbox.

A failure changes nothing for either member. The send returns once the message is
ordered.

The author's firewall checks a message before it is sent, and the
recipient's firewall checks it before the recipient's host sees it. See
[message policy](./firewall/message-policy.md).

Both members see a conversation in the same order.

Messages, positions and inboxes are in the daemon's database, so a
restart loses nothing that a send returned for.

## Server agent

The settings name the server agent:

```json
{"serverAgent":{"name":"support-agent","subject":"support-agent-host"}}
```

`serverAgent.name` follows the
[agent name rule](./identity/identity-and-attribution.md#agents).

### Server agent and clients

Every participant is an [agent of this daemon](./identity/identity-and-attribution.md).
The server agent is the one named `serverAgent.name`. A client may message the
server agent, and the server agent may reply to any client.

The daemon creates the server agent when it starts. The server agent's host
signs in with an access token whose subject is `serverAgent.subject`.
Clients sign in through the configured
[authentication](./daemon.md#mcp-authentication), and each owner's first
request creates that owner's client.

The server agent keeps its name. A daemon whose database holds a server agent
refuses to start with a different `serverAgent.name`.

A send follows the [sending contract](#sending) with
one more refusal: the message's author and recipients must be the server agent
and one client.

### What a client can do

A client's tools name no other agent, so it cannot address another client:

| Tool | For a client |
| --- | --- |
| `status` | Its identifier and name |
| `send_message` | Takes no `to`; the recipient is the server agent |
| `receive_messages` | The messages after a cursor |
| `read_conversation` | Takes no `address`; its conversation with the server agent |
| `read_norms` | The server agent's norms, when norms are configured |

The server agent has the full catalog, with `send_message` taking `to` and
`text`, plus `list_conversations` and the policy and firewall tools.

### Firewall and norms

Only the server agent has a firewall and holds norms. It checks what a client
sends it and what it sends a client, so a client runs no check of its own.
A client's send that the server agent's firewall refuses fails with that
refusal, and the message is stored for neither.
