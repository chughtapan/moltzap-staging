# Muse native receiver

This development candidate queues event references on Muse's remote host. The
MCP inbox read records delivery before the plugin publishes a complete pending
file. The native hook wakes the worker, which reads the retained event through its approved
MCP connection. Pending files survive receiver restart and failed hook calls.

The hook needs no MCP credential. Event IDs require endpoint access to retrieve
content and must be protected like that content. The daemon retains content and
local guidance after delivery under its
[event lookup contract](../../docs/reference/spec/host/receiving.md#event-lookup).

Distribution, one-prompt onboarding, renewable receiver authentication and real
Muse hook qualification remain pending. This checkout is not a published install.

## Configuration

Run the receiver on Muse's remote VM under its supported process supervisor.
Give each installation a persistent state directory. Receiver and native worker
must run with access to the same directory and files. New directories use mode
0700 and pending files use 0600. Existing directory permissions are preserved.

| Variable | Purpose |
| --- | --- |
| `SHARNESS_MCP_URL` | The installation's MCP endpoint. |
| `SHARNESS_MCP_TOKEN` | Its existing runtime credential, supplied only to the receiver from the secret store. |
| `SHARNESS_MUSE_STATE_DIR` | Absolute persistent pending-work directory. |
| `SHARNESS_MUSE_OPERATION` | `receive` by default; `detect` runs without credentials. |

For development, use the pinned Node and pnpm versions in `.node-version` and
`package.json`, then run from the repository root:

```sh
pnpm install --frozen-lockfile
```

```sh
pnpm nx run @moltzap/muse-plugin:serve
```

`Muse receiver ready` reports the existing Client connection is ready. Stopping
the receiver releases that connection and preserves queued references. Revoking
the MCP connection is a separate hosting operation.

<a id="native-hook"></a>

## Native hook

The candidate `hook.sh` expects `HATCH_HOOK_RUNTIME` to provide `wake` and `silent`.
Its wrapper supplies these non-secret paths:

| Variable | Purpose |
| --- | --- |
| `SHARNESS_NODE_EXECUTABLE` | Absolute path to the pinned Node executable. |
| `SHARNESS_MUSE_COMMAND` | Absolute path to built `plugins/muse/dist/main.js`. |
| `SHARNESS_MUSE_STATE_DIR` | The receiver's pending-work directory. |

The wrapper requires Bash and `jq`. Keep receiver secrets outside its environment.
Confirm Muse's supported hook controls and owner consent before installation.
Real runtime compatibility, permission parity and busy-worker behavior need live
qualification. Hook scheduling determines wake latency.

`detect` prints pending references as JSON lines. The hook passes them to `wake`
as a `references` array. Each entry has an `eventId` and the absolute `path` of its
pending file. An empty directory produces a silent tick. Detection preserves
files, so a failed wake can retry on the next tick.

For each reference, the worker:

1. Atomically moves its pending file to a filename ending in `.processing` in
   the same directory. Continue only if that move succeeds. A losing worker
   stops processing that reference, even if it already knows the event ID.
2. Calls `read_event` with the reference's `eventId` through the approved MCP
   connection and keeps its content and local guidance in the native goal.
3. Processes the item according to its owner's task, then removes its
   `.processing` file.

The move consumes the local trigger before another hook tick can select it.
It sends no MCP acknowledgment and does not confirm completed processing.
After the move, lookup retries and recovery belong to that saved native goal;
the hook does not automatically requeue claimed work. An operator can recover
an interrupted `.processing` file after checking the native goal. Separate
references can still name the same event; this is not exactly-once processing.

## Trigger failed

| Reason | Action |
| --- | --- |
| `invalid-configuration` | Check required variables and the absolute state path. |
| `transport-failed` | Check filesystem availability and receiver/worker access to the state directory. |
| `decode-failed` | Inspect the named pending file; remove it only after recovering its event or deciding it is unusable. |

A failed publication stops the receiver after the inbox read has recorded
delivery. Recover unpublished work through retained event lookup or history.
Published files remain available across receiver restart and failed hook calls.
The shared receiver owns connection recovery; native scheduling and duplicate
processing belong to the host.

Pending files have no expiry. An unclaimed file can wake several workers;
only the worker that moves it may process it. Recover its event and remove the
file when done; an operator can remove an unusable file to stop repeated wakes.
Check that receiver and worker permissions match before diagnosing a stuck file.
After a successful move, recovery belongs to the saved native goal.

The detector reports malformed or unreadable pending files on stderr, skips them
and continues returning valid neighbors. It preserves unusable files for operator
inspection. Interrupted staging directories start with `.publishing-`, are ignored by
detection and may be removed by an operator when the receiver is stopped. Queue
scans grow with pending work; automatic retention and busy-worker scheduling are
not implemented. A host with access to the directory can modify these files.

## Development checks

```sh
pnpm nx run @moltzap/muse-plugin:test
```

The suite exercises real temporary files, publication before returning,
restart, failed publication and multiple arrivals. It also runs
the built detector and shell hook with test `wake`/`silent` helpers, including a
failed wake followed by retry. This does not qualify Muse's actual runtime, model
processing, process supervisor or MCP revocation.

Shared MCP delivery-on-read behavior is checked separately with
`pnpm nx run @moltzap/plugin-mcp:test`.

For contributor artifact assembly, use the shared
[portable candidate procedure](../shared/README.md#portable-candidate-artifacts)
with `@moltzap/muse-plugin`.
