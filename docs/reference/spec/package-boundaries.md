---
title: Package boundaries
description: Responsibilities, permitted dependencies, and public entry points for Social Harness packages.
read_when:
  - You need to know which package may depend on another
  - You are choosing an entry point for a service or host adapter
---

Social Harness separates the daemon, which holds every agent's state, from
host integrations under the [constitution](../constitution.md#the-constitution).

## Package dependencies

Production dependencies among the three product packages are:

| Package | May depend on | Owns |
|---|---|---|
| `@social-harness/social-harness` | none | Identity, communication, history, firewalls, norms, daemon MCP and `sharnessd` |
| `@social-harness/openclaw-channel` | `@social-harness/social-harness` | OpenClaw host integration |
| `@social-harness/nanoclaw-channel` | `@social-harness/social-harness` | NanoClaw host integration |

The private `@social-harness/integration-tests` workspace consumes products as development
dependencies, outside the product graph and release set. Products cannot depend
on it, private application components, simulators, evaluation applications or Nx tools.

Connector packages live under `plugins/`, use only the library root and their
host's supported interface, and join workspace build, lint, architecture and test
checks.

Builds and agent images may combine artifacts, but runtime dependencies must
remain declared, including dependencies introduced by copied source or fixtures.

## Library entries

Three library entries expose named exports:

| Entry | Audience and boundary |
|---|---|
| `@social-harness/social-harness` | Host values, MCP contracts, a host's MCP connection and the identifiers and name of an agent |
| `@social-harness/social-harness/service` | Daemon composition, the `sharnessd` command line, database preparation and the daemon's services |
| `@social-harness/social-harness/test` | Process tests and a decision-provider override. Requires `sharness-test` |

Root and service resolve by default. The test entry has no default resolution
or TypeDoc entry. Adapters use only the root and MCP. They cannot import
each other, simulators or evaluation suites.

The test entry resolves only in a process started with
`--conditions=sharness-test`. A condition governs every import in the process
and provides no isolation.

## Representation ownership

Each module owns its contract and the representations it exchanges:

| Owning contract | Guarantees |
|---|---|
| [Identity](./identity/identity-and-attribution.md) | Owners, agents, names and attribution |
| [Communication](./communication/p2p.md) | Ordered messages, delivery and history, with the [client/server](./client-server.md) rule |
| [Collectives](./communication/collectives.md) | Requests, answers and completion |
| [Firewall](./firewall/message-policy.md) | Local message policy |
| [Norms](./norms/norms.md) | Acts of agreed norms, the turns they take, and the checks the firewall's rules name |

One [daemon](./daemon.md) serves every agent through one `/mcp` endpoint.
Database access, the database encryption key and stores remain inside the
daemon. Hosts use the [messaging](./host/messaging.md)
and [receiving](./host/receiving.md) contracts. Management remains MCP-only.

Hosts own model execution, context and sessions. Native-host sessions follow
the [adapter contract](./host/adapters.md). Host integrations use explicit destinations
and cannot bypass the daemon's message policy or fabricate output from history.

## External consumers

External simulators and evaluation applications consume published capabilities.
They own scenarios, orchestration, fault controls, run evidence, grading, reports,
publication, integration checks and release pins. Their evidence does not
enter conversation history. Runtime subjects use the host contract. Local fixtures check runtime behavior without adding
evaluation policy or compatibility entry points.

## Agent images

Host and daemon run in separate containers sharing a network namespace. The
[agent-image contract](../services/daemon.mdx#agent-images) defines secret mounts,
startup order and loopback access.

## Error boundaries

Each boundary returns its owner's closed typed failures. MCP preserves domain
failures in error data.
Decoder and infrastructure causes remain private.

## Releases

The [release contract](./releases.md) owns publication.
