# @social-harness/integration-tests

These tests run a real `sharnessd` process and sign several owners' agents in
to it, including delivery through the OpenClaw adapter. Use this workspace for
tests that start a daemon or a database. Keep component tests beside the code
they exercise.

Complete the [contributor setup](../../CONTRIBUTING.md), then run commands from
the repository root. Nx builds the packages each target needs. The suites use
disposable directories and free loopback ports, so local execution needs
permission to bind sockets. Image scenarios require Docker.

| Command | Runs |
| --- | --- |
| `pnpm nx run @social-harness/integration-tests:test:integration` | Daemon and adapter acceptance suites |
| `pnpm nx run @social-harness/integration-tests:test:live` | Docker image scenarios |
| `pnpm typecheck` | Type checks for the suites and adapter fixture, with the rest of the repository |
| `pnpm nx run @social-harness/integration-tests:lint` | ESLint for this workspace |

A suite gets its daemon from `sharedDaemon` in `daemon/daemon-fixture.ts`. The
owner `SERVER_AGENT_OWNER` signs in as the daemon's server agent and every
other named owner as a client, each with a token from the fixture's own OAuth
issuer. The file's header lists what the fixture offers.

Every daemon keeps its agents on the PostgreSQL server that
`SHARNESS_PG_TEST_URL` names, as the contributor setup describes. Each scenario
creates a schema of its own and a restricted login for its daemon, and removes
them afterward.
