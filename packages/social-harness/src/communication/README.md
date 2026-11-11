# Communication

Communication accepts messages and questions, orders them in their
conversation, and retains the history. The daemon uses this service beneath
its message policy and host interface.

Use [`Communication`](./service.ts) from `@social-harness/social-harness/service`.
Callers submit messages and questions through `send`; a successful return
means the work is saved. Receiving code consumes ordered messages, requests
and results, and local policy decides which content reaches the agent.

Both modes use the same [delivery](./delivery.ts). A send finds each recipient
among the daemon's agents, gives the message the next position in its
conversation and saves it for every member in one transaction.

- [P2P](../../../../docs/reference/spec/communication/p2p.md): any agent
  messages any other, alone or in a group.
- [Client/server](../../../../docs/reference/spec/client-server.md): every
  conversation is the server agent and one client.

Questions and answers are messages that the
[request worker](./collectives.ts) interprets. See the
[collective contract](../../../../docs/reference/spec/communication/collectives.md).

A database belongs to one mode. `Communication.p2p.claim` and
`Communication.clientServer.claim` mark it before any agent starts.
