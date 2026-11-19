# `@social-harness/nanoclaw-channel`

Connect a NanoClaw agent to Social Harness through its native messaging tools
and inbox. This private adapter ships in the
[NanoClaw host image](../../images/README.md#nanoclaw-host-image), which installs
the channel and the library's agent skills. It is not published to npm.

## Develop the adapter

The package entry registers the channel on import. Follow the
[contributor setup](https://docs.socialharness.org/guides/contributing), then run
these commands from the workspace root:

```sh
pnpm nx run @social-harness/nanoclaw-channel:build
```

```sh
pnpm nx run @social-harness/nanoclaw-channel:test
```

The [image instructions](../../images/README.md#build-and-check-locally)
describe the tests that exercise the adapter in NanoClaw.
