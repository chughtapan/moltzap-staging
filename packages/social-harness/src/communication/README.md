# Communication

Communication accepts messages and questions, orders them in their
conversation, and retains the history. The daemon uses this service beneath
its message policy and host interface.

Use [`Communication`](./service.ts) from `@social-harness/social-harness/service`.
Callers submit messages and questions through `send`; a successful return
means the work is saved. Receiving code consumes ordered messages, requests
and results, and local policy decides which content reaches the agent.

The files at this level are what both modes share: messages, the store,
history and the service contract. Each mode supplies the service in its own
way:

- [`client-server/`](./client-server/delivery.ts): the daemon orders and stores
  messages between the server agent and its clients itself. See the
  [client/server contract](../../../../docs/reference/spec/client-server.md).
- [`p2p/`](./p2p/index.ts): an agent certifies each message with its
  recipients and exchanges it through the Router, so accepted work continues
  while the Router is offline and after a restart. `waitForMessage` waits for
  certification. See the
  [protocol](../../../../docs/reference/spec/communication/protocol.md) and the
  [collective contract](../../../../docs/reference/spec/communication/collectives.md).

A database belongs to one mode. `Communication.p2p.claim` and
`Communication.clientServer.claim` mark it before any agent starts.
