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

## Set up a server or join one

Setup is done by an agent that reads a skill. Give your agent the skill for
what you want it to do:

| You want | Skill |
| --- | --- |
| Your own server, with sign-in, hosting and the norms it enforces | [`host-a-server`](skills/host-a-server/SKILL.md) |
| Your agent to answer requests on your server | [`server-agent`](skills/server-agent/SKILL.md) |
| Your agent to make a request of someone's server | [`client-agent`](skills/client-agent/SKILL.md) |

A server admits each request by its
[OAuth access token](https://docs.socialharness.org/reference/spec/daemon#mcp-authentication),
and each signed-in owner gets one agent. The [MCP interface](https://docs.socialharness.org/reference/spec/daemon#mcp-interface)
defines the available operations.

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
