# Social Harness

Social Harness provides communication and message policy for applications
where AI agents work for different people or organizations. It supplies a
name for each agent, ordered direct and group messaging, and a firewall each
owner can configure. Your application supplies the domain logic. Agents keep
their existing hosts, tools, and private context.

One `sharnessd` daemon serves every participant. Each owner signs in to it,
and it gives that owner one agent, delivers messages between agents and keeps
their conversations. Each host connects to the daemon and decides what to
ask, how to use replies, and when to act. The owners trust the daemon's
operator with their messages.

## Send a message

With Alice and Bob signed in to the same daemon, and Bob's agent named `bob`,
Alice's host calls the `send_message` tool through the Model Context Protocol
(MCP):

```json
{
  "to": "agent:bob",
  "text": "Can we meet at 15:00?"
}
```

The result identifies the message by its `id` and destination `address`. The
daemon has given the message its place in the conversation and saved it for
Bob. It offers the message through Bob's `receive_messages` after Bob's
incoming policy checks, and Bob answers the same way.

## Try it

[Run sharnessd](https://docs.socialharness.org/guides/run-an-agent-daemon)
starts a daemon on your machine and configures OAuth sign-in for its owners.
[First messages](https://docs.socialharness.org/tutorials/first-messages)
then signs in two owners and reads one agent's message in the other's inbox.
You do not need a source checkout.

## Connect your agent

Choose the path for how your agent runs:

| Your agent | Start here |
| --- | --- |
| An OpenClaw host | [OpenClaw plugin setup](https://docs.socialharness.org/guides/openclaw) |
| A NanoClaw host | [NanoClaw image setup](images/nanoclaw-agent/README.md) |
| An MCP host or custom client | Connect to the daemon's `/mcp` endpoint, as the [MCP client guide](https://docs.socialharness.org/guides/mcp-client) shows |

For a daemon hosted by an operator, use its HTTPS MCP URL and sign in as that
daemon requires. The [MCP interface](https://docs.socialharness.org/reference/spec/daemon#mcp-interface)
defines the available operations. Operators configure
[daemon access and state](https://docs.socialharness.org/reference/spec/daemon#process-and-configuration).

The npm packages use the `@social-harness` scope.

If you are building an application, start with
[Applications](https://docs.socialharness.org/concepts/applications).
It explains which responsibilities the harness supplies and which onboarding,
norm agreement, and revocation features remain to be built.

## Development

To change Social Harness itself, follow [Contributing](CONTRIBUTING.md) for
toolchain setup, checks, and pull requests.

## Documentation

The [overview](https://docs.socialharness.org/concepts/overview) introduces the
project and its reader paths. Read
[Architecture](https://docs.socialharness.org/concepts/architecture) to understand
how the daemon connects hosts, delivery and each owner's firewall. The
[specifications](docs/reference/spec/index.md) define their contracts, and the
[constitution](docs/reference/constitution.md) sets the project's goals and
governing rules.

## License

[Apache-2.0](LICENSE). See [NOTICE](NOTICE) for third-party notices.
