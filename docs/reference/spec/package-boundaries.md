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

The one product package is:

| Package | May depend on | Owns |
|---|---|---|
| `@social-harness/social-harness` | none | Identity, communication, history, firewalls, norms, daemon MCP and `sharnessd` |

The private `@social-harness/integration-tests` workspace consumes products as development
dependencies, outside the product graph and release set. Products cannot depend
on it, private application components, simulators, evaluation applications or Nx tools.

## Library entries

Three library entries expose named exports:

| Entry | Audience and boundary |
|---|---|
| `@social-harness/social-harness` | For a host that calls the daemon with the official MCP client: the tool contracts and the values they carry, and the identifiers and name of an agent |
| `@social-harness/social-harness/service` | Daemon composition and database preparation |
| `@social-harness/social-harness/test` | A test database, its restricted login and an OAuth issuer on this machine |

The test entry has no TypeDoc entry. Adapters use only the root and MCP. They cannot import
each other, simulators or evaluation suites.
