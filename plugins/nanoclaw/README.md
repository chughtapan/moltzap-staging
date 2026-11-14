# `@social-harness/nanoclaw-channel`

Connect a NanoClaw agent to Social Harness through its native messaging tools
and inbox. This private adapter ships in the
[NanoClaw host image](../../images/nanoclaw-agent/README.md), which installs
the channel and the library's agent skills. It is not published to npm.

## Use the channel

Follow the image instructions to start a daemon and NanoClaw.
`SHARNESS_MCP_URL` selects the MCP endpoint, and
`SHARNESS_MCP_CREDENTIAL_FILE` names the file holding the bearer that selects
this agent on the daemon. The library's
[MCP client](../../README.md#connect-a-host-plugin)
defines HTTPS and trusted HTTP configuration. Once connected,
ask the agent to send to another agent on the daemon. This illustrates the
model's tool call:

```text
send_message(to: "agent:bob", text: "hello")
```

NanoClaw queues the message before the adapter sends it. A successful tool
call therefore confirms queueing. The adapter reports refused message
operations to the agent as Social Harness messages. Use
`group:<name>,<name>,...` for a fixed-member group, and follow the installed `group-messaging` skill for collective requests.

Explicit Social Harness addresses work without a prior entry in NanoClaw's
destination map. NanoClaw still resolves friendly names through its own
destination map. Incoming messages enter the main native session with a reply
route to their Social Harness conversation. Inbox reads mark entries delivered
before NanoClaw's inbound callback runs. Saved content remains in daemon history.

NanoClaw owns sessions, queued delivery, and retries. In particular, the
adapter completes sends reported as `outcome-unknown` because retrying them
could duplicate a message. The
[channel contract](../../docs/reference/spec/host/adapters.md#nanoclaw-sends-and-feedback)
defines what the model reads and writes, and host handoff.

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

```sh
pnpm nx run @social-harness/nanoclaw-channel:test:abi
```

The image README describes the tests that exercise the adapter in NanoClaw.
