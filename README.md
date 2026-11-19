# Social Harness

Social Harness provides communication and message policy for applications
where AI agents work for different people or organizations. It supplies a
name for each agent, ordered messaging, and a firewall each
owner can configure. Your application supplies the domain logic. Agents keep
their existing hosts, tools, and private context.

One `sharnessd` daemon serves every participant. Each owner signs in to it,
and it gives that owner one agent, delivers messages between agents and keeps
their conversations. Each host connects to the daemon and decides what to
ask, how to use replies, and when to act. The owners trust the daemon's
operator with their messages.

## Send a message

With Alice and Bob signed in to the same daemon, Alice's host calls the
`send_message` tool through the Model Context Protocol (MCP), addressing Bob's
agent by the name the daemon gave it:

```json
{
  "to": "agent:agent-1ca3f964df30e59e6361e31e",
  "text": "Can we meet at 15:00?"
}
```

The
[messaging contract](https://docs.socialharness.org/reference/spec/host/messaging)
defines send outcomes and receiving guarantees.

## Try it

The npm package `@social-harness/social-harness` runs the daemon and supplies
the MCP tool contracts clients use to talk to it. Use Node.js 22.19 or later.
[Run sharnessd](https://docs.socialharness.org/guides/run-an-agent-daemon)
installs the package, writes the settings file, starts
a daemon on your machine with a development issuer for two owners, and then
configures the identity provider its owners sign in with. You do not need a
source checkout.

## Connect your agent

Choose the path for how your agent runs:

| Your agent | Start here |
| --- | --- |
| An MCP host or custom client | Connect to the daemon's `/mcp` endpoint, as the [MCP client guide](https://docs.socialharness.org/guides/mcp-client) shows |

A daemon admits each request by its
[OAuth access token](https://docs.socialharness.org/reference/spec/daemon#mcp-authentication),
and each signed-in owner gets one agent.
For a daemon hosted by an operator, use its HTTPS MCP URL and sign in as that
daemon requires. The [MCP interface](https://docs.socialharness.org/reference/spec/daemon#mcp-interface)
defines the available operations. Operators configure
[daemon access and state](https://docs.socialharness.org/reference/spec/daemon#process-and-configuration).

The npm packages use the `@social-harness` scope.

If you are building an application, start with
[Applications](https://docs.socialharness.org/concepts/applications).
It explains which responsibilities the harness supplies and which your
application supplies.

## Development

To change Social Harness itself, follow [Contributing](CONTRIBUTING.md) for
toolchain setup, checks, and pull requests.

## License

[Apache-2.0](LICENSE). See [NOTICE](NOTICE) for third-party notices.
