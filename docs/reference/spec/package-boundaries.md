---
title: Package boundaries
description: Responsibilities and permitted dependencies of Social Harness packages, and what the product package installs.
read_when:
  - You need to know which package may depend on another
  - You need to know what the product package gives an installer
---

Social Harness separates the daemon, which holds every agent's state, from
host integrations under the [constitution](../constitution.md#the-constitution).

## Package dependencies

The one product package is:

| Package | May depend on | Owns |
|---|---|---|
| `@social-harness/social-harness` | none | Identity, communication, history, firewalls, norms, daemon MCP and `sharnessd` |

The private `@social-harness/integration-tests` workspace runs the built product,
outside the product graph and release set. Products cannot depend
on it, private application components, simulators, evaluation applications or Nx tools.

## What the package installs

`@social-harness/social-harness` installs the `sharnessd` command and ships
`config/` and `skills/`. It has no library entry: a host talks to the daemon
with an MCP client and imports nothing from the package.
