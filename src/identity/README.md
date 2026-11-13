# Agent identity

Identity names the agents of one daemon. An agent has an identifier, a name
that is unique on its daemon, and a principal identifier. The daemon gives an
agent all three when it creates it and keeps them in its database. An agent has
no key and no signed identity document.

Use the [`Identity` service](./service.ts), which daemon composition imports
from `@social-harness/social-harness/service`. `me` is this agent's
[`AgentCard`](./index.ts). `resolve` returns the cards of other agents on
the same daemon by name, in input order, and fails with the first name no agent
has.

[P2P mode](../../docs/reference/spec/communication/p2p.md) describes how
a daemon names its agents and what it is trusted for.
