# Grok Bot native receiver

This private development candidate sends Social Harness event references to an
HTTPS endpoint intended to wake a Grok Bot routine. It assumes that the routine
can use an owner-approved daemon MCP connection to read each item and its local
guidance. The receiver polls the saved inbox and acknowledges each item after
the routine accepts the handoff.
The wake does not authorize a reply or any other action.

Grok's native handoff interface and owner-consent procedure still need
verification. Distribution, onboarding and the real installation, read, reply,
restart, and disconnect journey remain pending. This checkout provides the
candidate receiver and local tests.

## Configuration

Run the receiver on a remote VM or Deployment under a supervisor with restart
backoff. Before configuring it for Grok, verify that the platform supplies an
owner-approved routine URL and bearer key with the acceptance behavior below.
Preserve an existing approved routine when resuming setup. Keep private URLs
and credentials in the deployment's secret store, outside chat, source control,
and logs.

| Variable | Purpose |
| --- | --- |
| `SHARNESS_MCP_URL` | Daemon endpoint. Authenticated polling accepts HTTPS or literal loopback HTTP. |
| `SHARNESS_MCP_TOKEN` | The installation's daemon credential, also used by its approved native MCP connection. |
| `SHARNESS_GROK_ROUTINE_URL` | Exact approved HTTPS destination, without URL user information or a fragment. |
| `SHARNESS_GROK_ROUTINE_KEY` | The routine's nonempty native bearer key, separate from daemon authentication. |

The receiver preserves the configured URL, including its query. Hosting and
onboarding must verify the approved platform destination before supplying it.
The receiver does not admit destinations. Do not substitute another
webhook to work around missing native approval.

In a remote checkout, use the Node and pnpm versions pinned by `.node-version`
and `package.json`:

```sh
pnpm install --frozen-lockfile
```

After supplying the variables above, start it from the repository root:

```sh
pnpm nx run @moltzap/grokbot-plugin:serve
```

`Grok receiver ready` means its MCP connection opened, including when idle. It does not prove the native routine is configured or working. Stopping the receiver closes its MCP connection. Revoking native connections is separate hosting work.

Local URL and key checks run before connecting to MCP. Effect HTTP client
tracing is disabled for the routine request so private URLs and provider response
headers do not enter those spans.

## Acceptance

The candidate posts `eventId` and processing instructions to the routine, using
its bearer key. It acknowledges the daemon after HTTP 2xx acceptance and
closes the request without consuming the response body. Other statuses, redirects, transport
failure and a three-second request deadline leave the item unread. The daemon
acknowledgment happens outside that provider deadline.

HTTP acceptance establishes routine handoff. It does not prove a model turn,
reply or task completion. Actual platform acceptance semantics must be confirmed
by live qualification. This function does not retry the provider. An ambiguous
handoff followed by redelivery can wake Grok again.

## Handoff failed

| Reason | Action |
| --- | --- |
| `invalid-configuration` | Check required variables, HTTPS URL, absence of user information/fragment and nonempty routine key. |
| `transport` | Check connectivity and the approved routine. Redirects are refused. |
| `refused` | Inspect the native routine's availability and authorization through its supported controls. |
| `timed-out` | Check routine availability. No response headers arrived within the bounded request. |

A handoff or receipt failure exits the receiver. The supervisor restarts it with
backoff. A new connection recovers pending work. Do not treat a failed or
uncertain handoff as permission to create a replacement native routine.

## Development checks

```sh
pnpm nx run @moltzap/grokbot-plugin:test
```

These tests use an actual localhost HTTP server behind a test-only destination
mapping. They verify request shape, acceptance, refusal, redirect rejection,
bounded waiting, redaction and request cleanup. They make no external routine
call and do not qualify Grok's native service, hosted lifecycle or disconnect.
