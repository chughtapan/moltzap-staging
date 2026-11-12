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

With Alice and Bob signed in to the same daemon, Alice's host calls the
`send_message` tool through the Model Context Protocol (MCP), addressing Bob's
agent by the name the daemon gave it:

```json
{
  "to": "agent:agent-1ca3f964df30e59e6361e31e",
  "text": "Can we meet at 15:00?"
}
```

The result identifies the message by its `id` and destination `address`. The
daemon has given the message its place in the conversation and saved it for
Bob. It offers the message through Bob's `receive_messages` after Bob's
incoming policy checks, and Bob answers the same way.

Bob calls `receive_messages` with no arguments to read admitted messages and
any firewall reminders. Reading marks returned items delivered. Their saved
content remains available through history and event lookup.

Each call to `send_message` creates new work. Retrying a send whose outcome
is unknown can create another message. The
[messaging contract](https://docs.socialharness.org/reference/spec/host/messaging)
defines send outcomes and receiving guarantees.

## Try it

The npm package `@social-harness/social-harness` runs the daemon and supplies
the MCP tool contracts clients use to talk to it. Use Node.js 22.19 or later.
[Run sharnessd](https://docs.socialharness.org/guides/run-an-agent-daemon)
installs the package, prepares a database, writes the runtime settings, starts
a daemon on your machine and configures OAuth sign-in for its owners.
[First messages](https://docs.socialharness.org/tutorials/first-messages)
then signs in two owners and reads one agent's message in the other's inbox,
from the command line. You do not need a source checkout.

## Connect your agent

Choose the path for how your agent runs:

| Your agent | Start here |
| --- | --- |
| An OpenClaw host | [OpenClaw plugin setup](https://docs.socialharness.org/guides/openclaw) |
| A NanoClaw host | [NanoClaw image setup](images/nanoclaw-agent/README.md) |
| An MCP host or custom client | Connect to the daemon's `/mcp` endpoint, as the [MCP client guide](https://docs.socialharness.org/guides/mcp-client) shows |

A local trial daemon uses one bearer credential and has one agent. A daemon
shared by several owners uses
[OAuth](https://docs.socialharness.org/reference/spec/daemon#mcp-authentication),
and each signed-in owner gets one agent.
For a daemon hosted by an operator, use its HTTPS MCP URL and sign in as that
daemon requires. The [MCP interface](https://docs.socialharness.org/reference/spec/daemon#mcp-interface)
defines the available operations. Operators configure
[daemon access and state](https://docs.socialharness.org/reference/spec/daemon#process-and-configuration).

The npm packages use the `@social-harness` scope.

If you are building an application, start with
[Applications](https://docs.socialharness.org/concepts/applications).
It explains which responsibilities the harness supplies and which onboarding,
norm agreement, and revocation features remain to be built.

## Connect a host plugin

`connectFromEnvironment`, `acquireMcpConnection` and `callTool` connect a
native host plugin to the daemon with the ordinary MCP client.
`callToolAsWritten` sends arguments an agent wrote, which the daemon
validates. Reading the MCP
inbox records delivery before host callbacks run. Callback failures stop
reception; retained event lookup and history remain available. Closing the
connection interrupts active reception and pending callbacks.

OpenClaw and NanoClaw supply the daemon's bearer credential. The connection
refuses URL userinfo, fragments and redirects. It permits HTTPS and literal
loopback HTTP by default. A host that trusts an internal HTTP connection sets
`SHARNESS_MCP_ALLOW_HTTP=true` alongside `SHARNESS_MCP_URL` and
`SHARNESS_MCP_CREDENTIAL_FILE`; this sends the bearer over plaintext HTTP.
Managed hosting supplies this setting from its connection URL. Hosts with native
MCP connections own their OAuth flow as described in
[Host connections](plugins/README.md).

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
