# Communication

Communication accepts messages, orders them in their
conversation, and retains the history. The daemon uses this service beneath
its message policy and host interface.

Callers use [`Communication`](./service.ts) and submit messages
through `send`; a successful return means the message is
saved. Receiving code consumes ordered messages, and local policy decides which content reaches the agent.

Both modes use the same [service](./service.ts). A send finds each recipient
among the daemon's agents, gives the message the next position in its
conversation and saves it for every member in one transaction.

- [P2P](../../docs/reference/spec/communication/p2p.md): any agent
  messages any other, alone or in a group.
- [Client/server](../../docs/reference/spec/client-server.md): every
  conversation is the server agent and one client.
