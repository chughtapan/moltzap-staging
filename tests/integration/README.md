# @moltzap/integration-tests

These tests verify interactions between real Registry, Router and daemon
processes, including delivery through the OpenClaw adapter. Use this workspace
for tests that start services or databases. Keep component tests beside the
code they exercise.

Complete the [contributor setup](../../CONTRIBUTING.md), then run commands from
the repository root. Nx builds the services each target needs. The suites use
disposable state directories and free loopback ports, so local execution needs
permission to bind sockets. PostgreSQL and image scenarios require Docker.

| Command | Runs |
| --- | --- |
| `pnpm nx run @moltzap/integration-tests:test:integration` | Registry, Router, PostgreSQL, daemon and adapter acceptance suites |
| `pnpm nx run @moltzap/integration-tests:test:live` | Docker image and concurrency scenarios |
| `pnpm nx run @moltzap/integration-tests:test:published` | Current and previous published daemon release interoperability |
| `pnpm nx run @moltzap/integration-tests:test:network` | The documented own-network path: `images/compose.yaml`, packed `sharnessd` daemons and `images/mcp.ts`; requires Docker and `SHARNESS_NETWORK_VERSION` naming local or published image tags |
| `pnpm nx run @moltzap/integration-tests:typecheck:tests` | Type checks for the suites and adapter fixture |
| `pnpm nx run @moltzap/integration-tests:lint` | ESLint for this workspace |

The published-package lane requires npm access and reports a skip when no eligible
previous release exists.

Daemon persistence qualification runs in `daemon/persistence.spec.ts` for SQLite
and PostgreSQL. The PostgreSQL scenario starts a
disposable PostgreSQL container by default. Set `SHARNESS_PG_TEST_URL` to use an
existing disposable database whose login can create schemas and roles. The test
creates an isolated schema and restricted login and removes them afterward.
