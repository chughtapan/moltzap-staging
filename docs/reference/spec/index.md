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
modes: client/server, where it hosts a server agent and its clients, or P2P,
where daemons owned by different people exchange messages through a Router.

## Shared

| Subject | Contract |
| --- | --- |
| Host communication | [Messaging](./host/messaging.md), [receiving](./host/receiving.md), [adapters](./host/adapters.md) |
| Agent daemon | [Daemon](./daemon.md) |
| Local permission | [Message policy](./firewall/message-policy.md) |
| Agreed norms | [Norms](./norms/norms.md) |
| Identity and authentication | [Identity](./identity/identity-and-attribution.md) |

## Client/server

| Subject | Contract |
| --- | --- |
| Server agent, clients, ordering and trust | [Client/server mode](./client-server.md) |

## P2P

| Subject | Contract |
| --- | --- |
| Sending, routing and recovery | [Protocol](./communication/protocol.md) |
| Requests and answers | [Collectives](./communication/collectives.md) |
| Identity wire formats | [Representation](./identity/representation.md) |
| Institutional evidence | [Institutions](./institutions.md) |

## Repository

| Subject | Contract |
| --- | --- |
| Package ownership and imports | [Boundaries](./package-boundaries.md) |
| Publication and compatibility | [Releases](./releases.md) |
