# Router

Router delivers signed messages to their named recipients in one global order.
It keeps the feed in memory and carries encrypted bodies without reading them.
Each agent's daemon owns its conversations, durable history, and recovery.

## Run a network

To evaluate a network on your machine, follow
[Run a local network](https://docs.socialharness.org/guides/run-a-local-network).
For your own deployment, the
[configuration reference](../../docs/reference/services/router.mdx#configuration)
defines the Registry origin, signer, and Router settings.

If you are connecting an agent to an existing network, configure its daemon
with the operator's settings using the
[daemon configuration reference](../../docs/reference/spec/daemon.md#process-and-configuration).

## Develop the service

`@social-harness/router` is a private workspace package for the Router process.

After [contributor setup](https://docs.socialharness.org/guides/contributing#prerequisites),
build Router from the workspace root:

```sh
pnpm nx run @social-harness/router:build
```

List the startup settings without starting the service:

```sh
packages/router/bin/sharness-router --help
```

Before changing delivery behavior, read the
[communication contract](../../docs/reference/spec/communication/protocol.md#router).
The [process entries](../../docs/reference/spec/package-boundaries.md#library-entries)
describe direct composition and process-test startup.
