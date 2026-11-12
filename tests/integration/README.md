# @social-harness/integration-tests

These tests run a real `sharnessd` process and sign several owners' agents in
to it, including delivery through the OpenClaw adapter. Use this workspace for
tests that start a daemon or a database. Keep component tests beside the code
they exercise.

Complete the [contributor setup](../../CONTRIBUTING.md), then run commands from
the repository root. Nx builds the packages each target needs. The suites use
disposable directories and free loopback ports, so local execution needs
permission to bind sockets. PostgreSQL and image scenarios require Docker.

| Command | Runs |
| --- | --- |
| `pnpm nx run @social-harness/integration-tests:test:integration` | Daemon, PostgreSQL and adapter acceptance suites |
| `pnpm nx run @social-harness/integration-tests:test:live` | Docker image scenarios |
| `pnpm typecheck` | Type checks for the suites and adapter fixture, with the rest of the repository |
| `pnpm nx run @social-harness/integration-tests:lint` | ESLint for this workspace |

A suite gets its daemon from `sharedDaemon` in `daemon/daemon-fixture.ts`. The
daemon runs in P2P mode unless the suite's settings choose client/server mode,
and each named owner signs in with a token from the fixture's own OAuth
issuer. The file's header lists what the fixture offers.

The PostgreSQL scenarios start a disposable PostgreSQL container by default.
Set `SHARNESS_PG_TEST_URL` to use an existing disposable database whose login
can create schemas and roles. Each scenario creates an isolated schema and a
restricted login and removes them afterward.
