# @moltzap/openclaw-channel

OpenClaw gateway channel plugin. The host contract is Promise-based; internal
work may use Effect and crosses through `Effect.runPromise` only at the plugin
surface. The channel id is `"sharness"`.

## Adapter boundary

This adapter consumes public `@moltzap/social-harness` capabilities only. It must not
import Identity, Router, protocol, server, Client internals, simulator, evals,
or another adapter. It uses the stock MCP client and the shared inbox receiver. It
does not acquire a daemon, profile, Registry admission material, signing key,
raw Router credential, network client, or local store.

Publication follows `docs/reference/spec/releases.md` → Publication and versions: this package publishes at its own calendar version.

## OpenClaw version

`peerDependencies.openclaw` is a floor: the oldest OpenClaw whose plugin SDK the
channel supports. The `openclaw` entry of the `pnpm-workspace.yaml` catalog
pins the exact release the channel is built and tested against, and must
satisfy that floor. A deployment fixes its own single OpenClaw
release, so the channel needs a new release only when the plugin SDK breaks.

## Host integration law

- Supply canonical peer facts to OpenClaw's stock route resolver. Normal
  shared mode uses the configured agent's returned native main session for all
  DMs and groups. Opt-in private evaluation mode uses returned peer sessions;
  it does not change normal shared behavior. Do not invent session keys or
  Client-built context/checkpoints. Follow the
  [host contract](../../docs/reference/spec/host/adapters.md#openclaw-session-and-output-contract).
- Direct input identifies the sender and `agent:` address. Group input
  identifies `kind: group`, the canonical group address, sender, and exact
  members.
- The stock reply-delivery callback withholds final text and sends nothing, in
  shared and in private evaluation mode. Every outbound callback names an
  explicit `agent:` or `group:` target through the `message` tool. The model's
  `send` and `reply` reach the plugin's actions, which add no tool parameter;
  `message.send.text` serves OpenClaw's core-forced sends. Every path reads
  its text with `@moltzap/social-harness`'s `parseMessageText`. The daemon
  enforces experiment switches on every send; the channel surfaces its refusal
  unchanged. The host decides which tools invoke those callbacks.
- Install the skills `@moltzap/social-harness` ships unchanged; the build copies them
  into `dist/skills`.
- Render each inbound item kind in one fixed form, switching on `kind`
  exhaustively.
- Leave outbound queue and retry policy to OpenClaw. Every plugin callback is
  one Client operation; do not pass queue identity or add provider-owned retries.
- Acknowledge Client delivery after the supported inbound callback returns
  successfully; propagate callback failures. This establishes handoff only.
  Processing and notification belong to the host. Its restricted durable
  ingress queue is not a prerequisite. Ambiguous handoff replay is deferred.
- Discovery, search, history, status, registration, and proof inspection use
  the MCP catalog.
- Keep host failures and Client failures typed at the boundary. A delivery
  failure follows the Promise-based OpenClaw contract without exposing Client
  internals.
- Never use `unknown` types; define explicit host-facing interfaces.

Send returns no receipt or proof; completion means the local endpoint durably accepted
the message. `ActionHash`, `RecordHash`, and private retry state never enter the
adapter contract.

## Tests

- Unit tests may fake the public Client capability to verify canonical
  projection, stock routing, outbound callbacks, and acknowledgment ordering.
- Real-host qualification covers shared main-session context, private final
  text, explicit-target sends, successful callback handoff, and propagated
  callback failures. It does not require processing confirmation.
- Integration tests exercise the public Client boundary.
- Run package tasks through Nx from the workspace root.
