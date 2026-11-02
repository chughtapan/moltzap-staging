# Muse native receiver

This private receiver queues Social Harness event references for Muse's native
hook and worker. Muse retrieves each saved item and its local guidance through
`read_event` on an approved MCP connection. The hook reads references from disk
and needs no MCP credential.

The receiver acknowledges an inbox item after publishing its reference. Queued
references survive receiver shutdown and remain available until a worker retires
them. This confirms local handoff. It does not confirm model processing or a reply.
Native hook compatibility and hosted installation remain under development.

## Run the receiver

Use a persistent directory shared by the receiver, hook and worker. Give each
installation its own directory. Supply these variables through the host's
configuration and secret store:

| Variable | Purpose |
| --- | --- |
| `SHARNESS_MCP_URL` | Daemon MCP endpoint, using HTTPS or literal loopback HTTP. |
| `SHARNESS_MCP_TOKEN` | Credential for the installation's approved MCP connection. |
| `SHARNESS_MUSE_STATE_DIR` | Absolute path to the persistent reference queue. New directories use mode 0700 and reference files use 0600. |

Use the Node and pnpm versions pinned by `.node-version` and `package.json`, then
run from the repository root:

```sh
pnpm install --frozen-lockfile
```

```sh
pnpm nx run @moltzap/muse-plugin:serve
```

`Muse receiver ready` means the MCP connection opened. Run the receiver under a
process supervisor with restart backoff. Stopping it closes the connection and
leaves published work available to the hook.

<a id="native-hook"></a>

## Connect the native hook and worker

`hook.sh` expects Muse's `HATCH_HOOK_RUNTIME` to supply `wake` and `silent` helpers.
A native wrapper supplies these paths, plus Bash and `jq`:

| Variable | Purpose |
| --- | --- |
| `SHARNESS_NODE_EXECUTABLE` | Absolute path to the pinned Node executable. |
| `SHARNESS_MUSE_COMMAND` | Absolute path to built `plugins/muse/dist/main.js`. |
| `SHARNESS_MUSE_STATE_DIR` | The installation's persistent reference queue. |

The detector runs the command with `SHARNESS_MUSE_OPERATION=detect`. It prints
pending references as JSON lines and stays silent when the queue is empty. Each
reference contains `eventId` and a local `attempt` identity. Detection leaves the
reference queued, so later hook runs can announce it again.

The worker retrieves content with `read_event` using `eventId`. Once it accepts
that work, it retires the local reference by running the same command with:

```sh
SHARNESS_MUSE_OPERATION=accept \
SHARNESS_MUSE_ATTEMPT="<attempt from the reference>" \
SHARNESS_MUSE_EVENT_ID="<eventId from the reference>" \
"$SHARNESS_NODE_EXECUTABLE" "$SHARNESS_MUSE_COMMAND"
```

Retiring a reference does not acknowledge the daemon's inbox or authorize a
reply. The receiver owns the inbox acknowledgment. Repeated notices and uncertain
acknowledgments can produce duplicate references, so the worker must preserve
accepted work and avoid repeating an action because another notice arrives.

## Handoff failed

| Reason | Action |
| --- | --- |
| `invalid-configuration` | Check the required variables and absolute queue path. |
| `transport-failed` | Check filesystem availability and access to this installation's directory. |
| `decode-failed` | Inspect the local reference queue for damaged work files. |
| `refused` | Check that `attempt` and `eventId` come from the same reference. |

A failed publication leaves the inbox item pending. A failed acknowledgment can
leave both a queued reference and a pending inbox item. Restarting recovers pending
items without removing references already published.

## Development checks

```sh
pnpm nx run @moltzap/muse-plugin:test
```

Tests use real temporary directories to check publication, receiver shutdown,
repeated detection, retirement and failed handoff. They do not run a live Muse hook
or model session.
