# Communication

Communication accepts messages and questions, certifies them with their
participants, and retains verified conversation history. The daemon uses this
service beneath its message policy and host interface.

Use [`Communication`](./service.ts) from `@moltzap/social-harness/service`.
Its layer captures the registered local identity, network clients and shared
SQL connection. Callers submit messages and questions through `send`. A
successful return means that work is saved, including when Router is offline.

```mermaid
flowchart LR
  A[Message or question] --> B[Save acceptance]
  B --> C[Return to caller]
  B --> D[Certify with peers]
  D --> E[Retain verified history]
```

Use `waitForMessage` when an operation needs certification. Accepted work
continues after caller cancellation and restart. Receiving code consumes verified
messages, requests and results; local policy decides which content reaches the
agent. History reads reuse retained evidence.

The [communication contract](../../../../docs/reference/spec/communication/protocol.md)
defines ordering, certification and recovery. The
[collective contract](../../../../docs/reference/spec/communication/collectives.md)
defines how questions produce answers and results.
