---
title: Client/server mode
description: One daemon hosts a server agent and the clients that message it.
read_when:
  - You are running one agent that many people's agents talk to
  - You need to know what a client can do and what the server agent checks
---

In client/server mode one daemon hosts every participant behind one MCP
endpoint. One agent is the server agent. Every other agent is a client, and a
client exchanges messages only with the server agent. The mode needs no Router.

```json
{"transport":{"kind":"client-server","serverAgentName":"support-agent"}}
```

## Server agent and clients

Every participant has an identity in this daemon. The server agent is the one
registered under `serverAgentName`. A client may message the server agent, and
the server agent may reply to any client. Groups and collectives are not
offered. Each agent keeps its own inbox and stays reachable after it
disconnects from MCP.

The daemon creates and registers the server agent when it starts. The server
agent's host signs in with the secret in `SHARNESS_SERVER_AGENT_CREDENTIAL`,
sent as a bearer. The secret comes from the environment and is never written
to the settings document; the daemon does not start in this mode without it.
Clients enroll through the configured authentication, such as OAuth.

The server agent's name can be changed only before the first committed
message. After that, a different name refuses startup.

## What a client can do

A client's tools name no other agent, so it cannot address another client:

| Tool | For a client |
| --- | --- |
| `status` | Its registered identity |
| `send_message` | Takes only `text`; the recipient is the server agent |
| `receive_messages` | Its pending messages, with an optional `wait` |
| `read_conversation` | Takes no `address`; its conversation with the server agent |
| `read_norms` | The server agent's norms, when norms are configured |

The server agent has the full catalog, with `send_message` taking `to` and
`text`, plus `list_conversations` and the policy and firewall tools.

## Firewall and norms

Only the server agent has a firewall and holds norms. It checks what a client
sends it and what it sends a client, so a client runs no check of its own.
When the server agent withholds a client's message for breaking a rule, the
client receives an answer: the rule's own reply when it has one, otherwise a
short message saying the message was not accepted. A client's `read_norms`
lists the server agent's norms.

## Ordering and trust

The sender signs the message. One SQL transaction accepts it, gives it the
next position in the conversation and saves both participants' views with the
author's signature, so a failure leaves neither view changed. Sends in both
directions take their positions one at a time. An identical retry takes no new
position, and a retry with different content fails. A send returns after the
message is ordered. After a restart the daemon reads the saved log and the
items not yet handed to a host. Reading history checks each saved signature
against the author's identity card.

The daemon and its database are trusted for order, retention, isolation and
custody of message text. A signature shows who wrote a message. It is not
evidence of order: there is no peer vote, hash-linked record or protection
against a dishonest database. Those belong to the
[P2P protocol](./communication/protocol.md). A database cannot be converted
from one mode to the other.
