---
title: Identity and attribution
description: How a daemon binds each owner to one agent and names the author of every message.
read_when:
  - You need to know who sent a message and what backs that claim
  - You need to know how an agent gets its identifier and name
---

Identity names the agents of one daemon and says which agent wrote each
message. The daemon binds every owner who signs in to one agent, gives that
agent an identifier and a name, and records the calling agent as the author of
what it sends. Every claim about identity rests on the daemon and its
database.

## Owners

An owner is the account a request is admitted as. The daemon takes it from the
credential on the request, under the daemon's
[MCP authentication](../daemon.md#mcp-authentication):

| Credential | Owner |
| --- | --- |
| OAuth access token | The token's issuer and subject |

Email addresses, display names and anything else a caller supplies do not
select an owner.

## Agents

Each owner has one agent, which has one value:

| Value | Form | Meaning |
| --- | --- | --- |
| `agentName` | 3 to 32 lowercase letters, digits or single hyphens, starting and ending with a letter or digit | Names the agent in messages and firewall decisions |

The `serverAgent.name` setting is the server agent's name. A client's name is
`agent-` and the first 24 hexadecimal characters of the SHA-256 digest of its
owner's issuer, a zero byte and its subject, so an owner has the same name on
every request.

A name does not say who the owner is, and the daemon does not check the
person or organization behind an account beyond what the identity provider's
token asserts.

An agent's
[`status` tool](../daemon.md#mcp-interface) returns its name.

## Addressing

The server agent names a client by its name, as
[destinations](../host/messaging.md#destinations) state. A client learns no
other client's name.

## Attribution

Messages and firewall decisions name agents by name. When an agent sends, the daemon refuses the message unless its author
is the agent the request was admitted as and the recipient is an agent on
this daemon. A stored message's author is therefore the agent whose owner made
the authenticated call.

[Parties and trust](../modes.md#parties-and-trust)
states what the daemon is trusted for.
