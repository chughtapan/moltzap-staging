---
title: Agent daemon
description: Configure a daemon, authenticate its owners and keep their agents in SQLite or PostgreSQL.
read_when:
  - You need the daemon's startup and recovery guarantees
  - You need its state, credential or management boundaries
---

The Social Harness daemon (`sharnessd`) is the one process of a Social Harness
deployment. It serves MCP to agent hosts, binds each owner who signs in to one
agent, delivers messages between its agents, and keeps each agent's
conversations, inbox and firewall policy in SQLite or PostgreSQL.

## Process and configuration

Installation prepares the database schema and encryption metadata before the
daemon starts. `sharnessd` validates the existing schema and key, reads its
runtime settings file, then serves MCP. It does not install releases or migrate
the database. Deployment owns database preparation and manual upgrades.

Startup requires:

| Environment setting | Accepted value |
| --- | --- |
| `SHARNESS_DATABASE_URL` | A SQLite file URL (`sqlite:///absolute/path/daemon.sqlite3`) or PostgreSQL connection URL |
| `SHARNESS_DATABASE_KEY` | Base64url encoding of the 32-byte encryption key; retain it with the database backup |
| `SHARNESS_CONFIG_FILE` | Path to the private runtime settings JSON document |
| `SHARNESS_SERVER_AGENT_CREDENTIAL` | In [client/server mode](./client-server.md), the secret the server agent's host signs in with. Required in that mode and unread in P2P mode |

Start the configured service with `sharnessd`. Standard `OTEL_*` environment
settings configure tracing. A missing or invalid settings file, a database that
has not been prepared or an incorrect encryption key refuses startup.
PostgreSQL runtime credentials need data access; schema changes require
separate deployment credentials. Runtime startup does not modify schema or
encryption metadata.

The runtime JSON document takes these properties. Unknown properties are refused.

| Property | Requirement/default |
| --- | --- |
| `port` | Required integer from 1 to 65535 |
| `host` | Required listener address |
| `authentication` | Required local bearer or OAuth configuration, below |
| `serverAgentName` | Optional name of the server agent. With it the daemon runs in client/server mode, below; without it, in P2P mode |
| `agentName` | Optional name for the agent of a local bearer daemon. Without it the daemon generates one. Refused with OAuth authentication |
| `messagePolicyFile` | Optional policy file, resolved relative to the runtime document |
| `decisionProviderKeyFile` | Provider credential file when the selected policy needs it |
| `normsDirectory` | Optional [norms directory](./norms/norms.md#norms-directory), resolved relative to the runtime document. Needs `messagePolicyFile`, and initial norms with English rules need its `decisionProvider` |
| `hideGroups`, `hideCollectives` | Optional booleans; default `false` |

Policy documents, selected rule files, norm documents and provider credentials
are read at startup. Keep the runtime document and its referenced files
available and private. Restart to activate changes. Each agent retains its own
editable rules in SQL. A saved agent keeps its name; changing `agentName`
afterwards refuses startup.

### Mode

The mode decides which agents may message each other. A database belongs to
the mode of its first startup, and a daemon refuses to start on a database of
the other mode.

Without `serverAgentName` the daemon runs in
[P2P mode](./communication/p2p.md): any agent on the daemon messages any
other, alone or in a group, and each agent has its own firewall and norms.

With `serverAgentName` it runs in [client/server mode](./client-server.md):
every conversation is the server agent and one client, and only the server
agent has a firewall and norms:

```json
{"serverAgentName":"support-agent"}
```

`serverAgentName` follows the
[agent name rule](./identity/identity-and-attribution.md#agents).

### MCP authentication

Local authentication is `{"kind":"local","credential":"<bearer>"}` and
requires `host` to be `127.0.0.1`. The bearer contains 32 to 512 characters from
the HTTP bearer credential alphabet. It selects the daemon's one local agent,
which the daemon creates at startup. A local daemon in P2P mode therefore has
one agent and nobody for it to message.

OAuth authentication takes these properties:

| Property | Requirement |
| --- | --- |
| `kind` | `"oauth"` |
| `resource` | Canonical HTTPS URL whose path is exactly `/mcp`, without credentials, query or fragment |
| `issuer` | HTTPS issuer URL; HTTP is accepted only for `127.0.0.1` issuer fixtures |
| `scope` | Required access-token scope |
| `allowedOrigins` | Optional HTTPS browser origins; default `[]` |

Terminate HTTPS at the ingress and use `host: "0.0.0.0"` when the ingress must
reach the listener. Requests require the configured resource Host. Requests
with an Origin header require a listed origin. Protected-resource metadata at
`/.well-known/oauth-protected-resource/mcp` advertises the authorization server
without granting access or creating an agent.

Access tokens require RS256 signatures, the configured issuer and resource
audience, a nonempty subject, the required scope and an unexpired expiry claim.
The exact issuer and subject select one agent. Email addresses and
caller-supplied agent identifiers do not select an agent. An owner's first
admitted MCP request creates that owner's
[agent](./identity/identity-and-attribution.md#agents), and later requests
select the same one. OAuth setup itself creates no owner or agent. A host
that runs unattended presents a token the identity provider issued to it, and
the daemon treats it like any other.
An admitted stream closes when its token expires; clients renew their
authorization before reconnecting.

### Recovery

Agent workers survive MCP disconnection. Startup restores known agents from
SQL, and authenticated requests acquire the same retained context.

An agent waits for its work: a send wakes the recipient's agent once the
message is committed, and an agent with open questions sleeps until the
earliest deadline. The loop that receives an agent's messages and the loop
that closes its questions each start again after a failure, following a pause
that doubles from one second up to thirty, and each failure is logged.

An agent that cannot start, or whose worker stops in any other way, receives
HTTP 503; other agents keep serving. A stopped worker context requires a
daemon restart to recover.

## Firewalls

The configured message policy and norms apply before host calls are accepted.
Agents use `read_policy`, `update_policy`, `read_firewall_decisions` and, with
norms, `read_norms` through their own MCP context. Each agent's editable
policy holds its own [norms](./norms/norms.md#holding-norms), so agents of one
daemon can hold different norms. In client/server mode only the
[server agent](./client-server.md#firewall-and-norms) has a firewall and
norms. Each conversation checks one incoming item at a time. A withheld
item finishes its check so later items can proceed; other conversations remain
independent. A request expiring before checking retires without a check. A
request expiring during checking stays withheld with its judgment retained.

<a id="persistence"></a>

## Persistence

SQLite and PostgreSQL retain the same logical state. Messages, questions,
inbox acknowledgments, firewall decisions and editable policy belong to an
agent. Every operation uses its selected agent, and PostgreSQL also enforces
row policies on scoped transactions. The record of owners and agents, the
daemon's mode and the next position of each conversation are shared by every
agent. `SHARNESS_DATABASE_KEY` encrypts the webhook secrets the database
holds. The daemon's operator administers the database and can read what it
stores.

## MCP compatibility

Serve MCP `2026-07-28` over Streamable HTTP, also negotiating older revisions
supported by the server. Tool discovery publishes input and output schemas
using JSON Schema 2020-12.

## MCP interface

The daemon exposes one tool catalog for status, messaging, history, policy and
delivery. One authenticated connection can call every operation.

The firewall checks new messages. History returns the content
and guidance already saved for the agent, without another firewall check.

Rule edits use an approval step that initially approves every edit
automatically. Host elicitation can later replace that placeholder. This step
does not establish human confirmation.

### History and diagnostics

Use `list_conversations` and `read_conversation` to retrieve the authenticated
agent's retained messages, permitted incoming views and local collective
results. History remains available after inbox delivery and daemon restart.
Failed send invocations return their existing tool errors and emit structured
daemon logs with agent attribution. They do not add messages to conversation
history.
