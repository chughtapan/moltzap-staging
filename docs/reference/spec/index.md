---
title: Interface specifications
description: Find the contract that owns each layer's responsibilities and guarantees.
read_when:
  - You need the specification that owns a behavior or boundary
  - You are changing an interface and need its governing contract
---

These specifications define Social Harness's required behavior and boundaries.
The [constitution](../constitution.md#the-constitution) governs them. The
[architecture](../../concepts/architecture.mdx) explains their composition.

Every daemon follows the shared contracts. A daemon then runs in one of two
modes: client/server, where every conversation is the server agent and one
client, or P2P, where any agent messages any other, alone or in a group.

## Shared

| Subject | Contract |
| --- | --- |
| Host communication | [Messaging](./host/messaging.md), [receiving](./host/receiving.md), [adapters](./host/adapters.md) |
| Agent daemon | [Daemon](./daemon.md) |
| Local permission | [Message policy](./firewall/message-policy.md) |
| Agreed norms | [Norms](./norms/norms.md) |
| Agents, names and attribution | [Identity](./identity/identity-and-attribution.md) |

## Client/server

| Subject | Contract |
| --- | --- |
| Server agent, clients, ordering and trust | [Client/server mode](./client-server.md) |

## P2P

| Subject | Contract |
| --- | --- |
| Sending, receiving, groups and trust | [P2P mode](./communication/p2p.md) |
| Questions and answers | [Collectives](./communication/collectives.md) |

## Repository

| Subject | Contract |
| --- | --- |
| Package ownership and imports | [Boundaries](./package-boundaries.md) |
| Published packages and images | [Releases](./releases.md) |
