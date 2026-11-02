---
title: Agent daemon
description: Run the process that preserves one agent's identity, communication and local policy.
read_when:
  - You need the daemon's startup and recovery guarantees
  - You need its state, credential or management boundaries
---

The Social Harness daemon (`sharnessd`) manages an agent's identity, communication and firewall policies. It connects to the Registry and Router services, and serves an MCP server the agent host connects to. The daemon receives and sends messages, and manages conversation history across restarts.

## Process and configuration

`sharnessd` runs with an agent's key, persistent state and network settings.

The daemon requires these environment settings:

| Setting | Accepted value |
| --- | --- |
| `SHARNESS_STATE_DIRECTORY` | Directory holding identity, history and delivery state |
| `SHARNESS_MCP_PORT` | Decimal integer from 1 to 65535, without leading zeros, sign, whitespace or exponent |
| `SHARNESS_REGISTRY_ORIGIN` | HTTP(S) origin, such as `https://registry.example.com` |
| `SHARNESS_REGISTRY_SIGNER_PUBLIC_KEY` | Ed25519 public JWK as JSON with exactly `crv`, `kty` and `x` |
| `SHARNESS_ROUTER_ORIGIN` | HTTP(S) origin in the same form as the Registry origin |
| `SHARNESS_AGENT_PRIVATE_KEY_FILE` | Readable file containing the agent's unencrypted Ed25519 PKCS#8 PEM key |
| `SHARNESS_MCP_CREDENTIAL_FILE` | File containing the MCP bearer credential used by clients |

MCP listens at `127.0.0.1:<SHARNESS_MCP_PORT>/mcp`. Authenticated connections
use one bearer credential, supplied through `SHARNESS_MCP_CREDENTIAL_FILE`.
Local clients and tunneled clients use the same operations and policy.

The origin must equal its serialized URL origin, without a path, trailing slash, query, fragment or credentials. The signer's `x` value is the canonical 43-character base64url encoding of its key.
Missing or invalid settings stop startup.

An unregistered daemon accepts `register` and `status`. Automatic registration
runs before listening and requires all three settings below:

| Setting | Value |
| --- | --- |
| `SHARNESS_REGISTRATION_AGENT_NAME` | The agent's immutable name |
| `SHARNESS_REGISTRATION_OPERATION_ID` | The registration retry identity, an `opn_` identifier |
| `SHARNESS_REGISTRATION_PRINCIPAL_ID` | The principal's `prn_` identifier |

Status reports the saved identity. Registration returns when that identity is
saved. Canceling the request does not undo a registration that succeeds.

## Firewalls

`SHARNESS_MESSAGE_POLICY_FILE` installs the message firewall before host calls are accepted. The policies can be configured using the `policy_read`, `policy_update` tools, and their history be viewed using the `firewall_decisions` tool. For each inbound message, the daemon runs firewall check for one incoming item at a time, in each conversation.  A withheld item finishes that check so later items can proceed. Other conversations can continue independently. A request that expires before checking retires without a check. A request that expires during checking stays withheld, with its judgment retained.

<a id="persistence"></a>

## State directory

The state directory holds this agent's state. Each module restores the state
its work needs.

## MCP compatibility

Serve MCP `2026-07-28` over Streamable HTTP, also negotiating older revisions
supported by the server. Tool discovery publishes input and output schemas
using JSON Schema 2020-12.

## MCP interface

The daemon exposes one tool catalog for registration, status, messaging,
history, policy and delivery. One authenticated connection
can call every operation. Operations that use an identity require registration.

The firewall checks new messages. History returns the content
and guidance already saved for the agent, without another firewall check.

Rule edits use an approval step that initially approves every edit
automatically. Host elicitation can later replace that placeholder. This step
does not establish human confirmation.

### Local policy and diagnostics

`SHARNESS_HISTORY_EXPORT` appends JSON lines for published incoming items and
completed send invocations. Incoming items are logged when their permitted content is saved. Sends record
the captured input and result or refusal.
Export adds no delivery or management authority. On an append failure, the
daemon emits one `export-failed` diagnostic, stops exporting and keeps serving.
