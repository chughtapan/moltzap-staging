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

| Subject | Contract |
| --- | --- |
| Identity and authentication | [Identity](./identity/identity-and-attribution.md), [wire formats](./identity/representation.md) |
| Sending, routing and recovery | [Communication](./communication/protocol.md) |
| Requests and answers | [Collectives](./communication/collectives.md) |
| Host communication | [Messaging](./host/messaging.md), [adapters](./host/adapters.md), [handoff](./host/receiving.md#delivery-handoff) |
| Agent daemon | [Daemon](./daemon.md) |
| Local permission | [Message policy](./firewall/message-policy.md) |
| Agreed norms | [Norms](./norms/norms.md) |
| Institutional evidence | [Institutions](./institutions.md) |
| Package ownership and imports | [Boundaries](./package-boundaries.md) |
| Publication and compatibility | [Releases](./releases.md) |
