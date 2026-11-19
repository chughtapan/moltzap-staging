---
title: Agent daemon
description: Configure a daemon, authenticate its owners and keep their agents in PostgreSQL.
read_when:
  - You need the daemon's startup and recovery guarantees
  - You need its state, credential or management boundaries
---

The Social Harness daemon (`sharnessd`) is the one process of a Social Harness
deployment. It serves MCP to agent hosts, binds each owner who signs in to one
agent, delivers messages between its agents, and keeps each agent's
conversations, inbox and firewall policy in PostgreSQL.

## Process and configuration

Installation prepares the database schema before the daemon starts.
`sharnessd` checks that the database is prepared, reads its settings file,
then serves MCP. It does not install releases or migrate
the database. Deployment owns database preparation and manual upgrades.

Startup requires:

| Environment variable | Accepted value |
| --- | --- |
| `SHARNESS_DATABASE_URL` | A PostgreSQL connection URL (`postgres://user:password@host:5432/database`) |
| `SHARNESS_CONFIG_FILE` | Path to the private settings file |

Start the daemon with `sharnessd`. Standard `OTEL_*` environment
variables configure tracing. A missing or invalid settings file or a database
that has not been prepared refuses startup.
The daemon's PostgreSQL credentials need data access; schema changes require
separate deployment credentials.

The settings file is a JSON document that takes these properties. Unknown properties are refused.

| Property | Requirement/default |
| --- | --- |
| `port` | Required integer from 1 to 65535 |
| `host` | Required listener address |
| `authentication` | Required OAuth configuration, below |
| `serverAgent` | Optional `name` and `subject` of the server agent. With it the daemon runs in [client/server mode](./modes.md#server-agent-and-clients); without it, in P2P mode |
| `messagePolicyFile` | Optional policy file, resolved relative to the settings file |
| `decisionProviderKeyFile` | Provider credential file when the selected policy needs it |
| `normsDirectory` | Optional [norms directory](./norms/norms.md#scope-and-installation), resolved relative to the settings file. Needs `messagePolicyFile`, and initial norms with English rules need its `decisionProvider` |
| `hideGroups` | Optional boolean; default `false`. With `true`, a daemon in P2P mode offers no group addresses |
| `maximumHistoryRecords` | Default 256; [window bounds](./firewall/message-policy.md#available-history). |

Policy documents, selected rule files, norm documents and provider credentials
are read at startup. Restart to activate changes.

### MCP authentication

OAuth authentication takes these properties:

| Property | Requirement |
| --- | --- |
| `resource` | Canonical HTTPS URL whose path is exactly `/mcp`, without credentials, query or fragment |
| `issuer` | HTTPS issuer URL; HTTP is accepted only for `127.0.0.1` issuer fixtures |
| `scope` | Required access-token scope |

A request's Host header names the resource's hostname, and so does its Origin
header when it sends one.

The exact issuer and subject select one agent. A host
that runs unattended presents a token the identity provider issued to it, and
the daemon treats it like any other.
An admitted stream closes when its token expires; hosts renew their
authorization before reconnecting.

### Recovery

Agent workers survive MCP disconnection. Startup restores known agents from
SQL, and authenticated requests acquire the same retained context.

An agent that cannot start receives HTTP 503; other agents keep serving.

## Firewalls

The configured message policy and norms apply before host calls are accepted.
Agents use `read_policy`, `update_policy`, `read_firewall_decisions` and, with
norms, `read_norms` through their own MCP context.

<a id="persistence"></a>

## Persistence

Messages, inbox entries, firewall decisions and editable policy
belong to an agent. The record of owners and agents and the
next position of each conversation are shared by every agent. The database
stores everything as it was written, including the signing secret of a
subscription. The daemon's operator administers the database and can
read what it stores, so protect it as you would the messages themselves.

## MCP interface

The daemon exposes one tool catalog for status, messaging, history, policy and
delivery. One authenticated connection can call every operation.

The firewall checks new messages. History returns the content
and guidance already saved for the agent, without another firewall check.

Failed send invocations return their existing tool errors and emit structured
daemon logs with agent attribution.
