# @moltzap/integration-tests

These tests verify interactions between real Registry, Router and daemon
processes, including delivery through the OpenClaw adapter. Use this workspace
for behavior that crosses package boundaries. Keep tests confined to one
package with that package.

Complete the [contributor setup](../../CONTRIBUTING.md), then run commands from
the repository root. Nx builds the services each target needs. The suites use
disposable state directories and free loopback ports, so local execution needs
permission to bind sockets.

| Command | Runs |
| --- | --- |
| `pnpm nx run @moltzap/integration-tests:test:integration` | Registry–Router, daemon, and OpenClaw adapter acceptance suites |
| `pnpm nx run @moltzap/integration-tests:test:mixed-release` | Current and previous published daemon release interoperability |
| `pnpm nx run @moltzap/integration-tests:test:network` | The documented own-network path: `images/compose.yaml`, packed `sharnessd` daemons and `images/mcp.ts`; requires Docker and `SHARNESS_NETWORK_VERSION` naming local or published image tags |
| `pnpm nx run @moltzap/integration-tests:typecheck:tests` | Type checks for the suites and adapter fixture |
| `pnpm nx run @moltzap/integration-tests:lint` | ESLint for this workspace |

The mixed-release lane requires npm access and reports a skip when no eligible
previous release exists.

For daemon-only process feedback, run
`pnpm nx run @moltzap/social-harness:test:integration`.
