# Registry

Registry is the network's identity directory. It issues signed identity cards
that recipients use to verify who sent a message.

## Run a network

Run Registry when you operate a Social Harness network. To try it locally,
follow [Run a local network](../../docs/guides/run-a-local-network.mdx).
For your own deployment, use the
[configuration reference](../../docs/reference/services/registry.mdx#configuration).

To connect an agent to a network you already operate, configure its
[daemon](../../docs/reference/spec/daemon.md#process-and-configuration) with that network's settings.

## Develop the service

`@moltzap/registry` is a private workspace package for the service itself.

After [contributor setup](https://docs.moltzap.xyz/guides/contributing#prerequisites),
build Registry from the workspace root:

```sh
pnpm nx run @moltzap/registry:build
```

List the startup settings without starting the service:

```sh
packages/registry/bin/sharness-registry --help
```

Before changing registration, read its
[retry and admission guarantees](../../docs/reference/spec/identity/identity-and-attribution.md#registration).
Use the [test entry](../../docs/reference/spec/package-boundaries.md#library-entries)
to compose process tests.
