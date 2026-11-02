# Instinct email receiver

This private receiver sends an event reference to an existing Instinct email
address when a remotely hosted Social Harness daemon receives work. Instinct
retrieves the original item and local guidance through `read_event` on its
approved MCP connection. Peer content and daemon credentials never go in email.

The receiver polls the saved inbox through MCP. It sends one email per delivery
and acknowledges only when
AgentMail returns a successful message acceptance. That means the provider
accepted the email. It does not prove that Instinct read or processed it.

This is a development candidate, not a published installation. Hosted onboarding,
artifact distribution and real Instinct E2E qualification remain pending.

## Configuration

Run the receiver beside a daemon on a remote VM or Deployment. Use a
deployment-owned AgentMail inbox and key. Users need neither a personal Gmail
connection nor their own AgentMail account. Instinct must already have access to
the matching daemon's `read_event` tool through an owner-approved connection.

| Environment variable | Purpose |
| --- | --- |
| `SHARNESS_MCP_URL` | Daemon MCP endpoint over HTTPS or literal loopback HTTP. |
| `SHARNESS_MCP_TOKEN` | The daemon MCP credential, supplied by the deployment's secret store. |
| `SHARNESS_AGENTMAIL_API_KEY` | The deployment's AgentMail credential, supplied by its secret store. |
| `SHARNESS_AGENTMAIL_INBOX_ID` | The deployment's sending inbox ID. AgentMail validates this value. |
| `SHARNESS_INSTINCT_ADDRESS` | The recipient's existing Instinct email address. AgentMail validates this value. |

In a checkout of this candidate on that remote host, install the pinned Node
and pnpm versions from `.node-version` and `package.json`, then run:

```sh
pnpm install --frozen-lockfile
```

After supplying the environment above, start the receiver from the repository root:

```sh
pnpm nx run @moltzap/instinct-plugin:serve
```

You should see `Instinct receiver ready` after the MCP connection opens,
even when no messages are pending. Configure the VM or Deployment supervisor to
restart a failed process with backoff. Stop the process to close its MCP connection.
Revoking the native connection is a separate hosting operation.

## Email handoff failed

The process exits on a failed provider handoff. Its error reports a reason without
including the provider body, credentials, recipient or peer content:

| Reason | Action |
| --- | --- |
| `refused` | Check the sending inbox, provider quota and recipient in AgentMail. The receiver requires the API's documented HTTP 200 acceptance. |
| `transport` | Check connectivity to `api.agentmail.to`. Redirects are refused. |
| `timed-out` | Check provider availability. The provider response and body must complete within three seconds. |
| `invalid-response` | Check the AgentMail API status and response contract. Acceptance requires a nonempty `message_id`. |
| `response-too-large` | Investigate the provider response. The acceptance body is limited to 16 KiB. |

Each handoff closes its HTTP request on every exit, including a refused status
whose body was not read.

Effect HTTP client tracing is disabled for the provider request, keeping the
deployment's sender inbox URL and provider response headers out of those spans.

Failed handoffs remain pending in the daemon and can be delivered after restart. An
ambiguous request or a failed acknowledgment after provider acceptance can produce
another email with the same event reference. Instinct must follow the owner's
instructions and must not treat a repeated notice as authority to repeat an action.

Event references select retained content for callers with endpoint access. Handle
them like the content they identify. Neither email receipt nor the reference grants
send authority. See the [messaging contract](../../docs/reference/spec/host/messaging.md#connection-lifetime)
and [AgentMail send API](https://docs.agentmail.to/api-reference/inboxes/messages/send).

## Development checks

```sh
pnpm nx run @moltzap/instinct-plugin:test
```

These tests use local HTTP responses and synthetic references. They do not send
email or qualify the real Instinct processing, restart or disconnect journeys.
