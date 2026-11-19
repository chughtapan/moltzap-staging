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
| The one word for each thing | [Vocabulary](./vocabulary.md) |
| Host communication | [Messaging](./host/messaging.md), [receiving](./host/receiving.md) |
| Agent daemon | [Daemon](./daemon.md) |
| Local permission | [Message policy](./firewall/message-policy.md) |
| Agreed norms | [Norms](./norms/norms.md) |
| Agents, names and attribution | [Identity](./identity/identity-and-attribution.md) |
| Server agent, clients, ordering and trust | [Modes](./modes.md) |
| Package ownership and imports | [Boundaries](./package-boundaries.md) |
| Published packages and images | [Releases](./releases.md) |
