---
title: Client/server mode
description: One daemon hosts a server agent and the clients that message it.
read_when:
  - You are running one agent that many people's agents talk to
  - You need to know what a client can do and what the server agent checks
---

In client/server mode one daemon hosts every participant behind one MCP
endpoint. One agent is the server agent. Every other agent is a client, and a
client exchanges messages only with the server agent. Naming a server agent in
the settings selects this mode:

```json
{"serverAgentName":"support-agent"}
```

[P2P mode](./communication/p2p.md) is the same daemon without this rule: any
agent messages any other, alone or in a group.

## Server agent and clients

Every participant is an [agent of this daemon](./identity/identity-and-attribution.md).
The server agent is the one named `serverAgentName`. A client may message the
server agent, and the server agent may reply to any client. Groups and
collectives are not offered. Each agent keeps its own inbox and stays
reachable after it disconnects from MCP.

The daemon creates the server agent when it starts. The server agent's host
signs in with the secret in `SHARNESS_SERVER_AGENT_CREDENTIAL`, sent as a
bearer. The secret comes from the environment and is never written to the
settings document, and the daemon does not start in this mode without it. It
contains 32 to 512 characters from the HTTP bearer credential alphabet.
Clients sign in through the configured
[authentication](./daemon.md#mcp-authentication), and each owner's first
request creates that owner's client.

The server agent keeps its name. A daemon whose database holds a server agent
refuses to start with a different `serverAgentName`.

## What a client can do

A client's tools name no other agent, so it cannot address another client:

| Tool | For a client |
| --- | --- |
| `status` | Its identifier and name |
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

A send follows the [P2P sending contract](./communication/p2p.md#sending) with
one more refusal: the message's author and recipients must be the server agent
and one client. One transaction gives the message the next position in that
conversation and saves it for both participants, so a failure leaves neither
view changed. A send returns after the message is ordered, and a restart loses
nothing that a send returned for.

The daemon and its database are trusted for the sender of each message, order,
retention, isolation between clients and custody of message text. Nothing in a
message lets a third party check who wrote it or where it stands in the
conversation. A database belongs to one mode and cannot be moved to the other.
