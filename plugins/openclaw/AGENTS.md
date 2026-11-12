# @social-harness/openclaw-channel

OpenClaw gateway channel plugin. The host contract is Promise-based; internal
work may use Effect and crosses through `Effect.runPromise` only at the plugin
surface. The channel id is `"sharness"`.

## Adapter boundary

This adapter consumes public `@social-harness/social-harness` capabilities only. It must not
import daemon services, Client internals, simulator, evals, or another
adapter. It uses the stock MCP client and the library's inbox receiver. It
does not acquire a daemon, a database, an owner's sign-in or a local store.

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
  identifies `kind: group`, the canonical group address and the sender. A
  group message or question lists its members from the message's author and
  recipients; a result lists none.
- The stock reply-delivery callback withholds final text and sends nothing, in
  shared and in private evaluation mode. Every outbound callback names an
  explicit `agent:` or `group:` target through the `message` tool. The model's
  `send` and `reply` reach the plugin's actions, which add no tool parameter;
  `message.send.text` serves OpenClaw's core-forced sends. Every path sends
  text that is a JSON object with `collective` or `collectiveResponse` as
  those `send_message` arguments, and any other text as a plain message. The
  daemon validates the arguments and enforces experiment switches on every
  send; the channel surfaces its refusal unchanged. The host decides which tools invoke those callbacks.
- Install the skills `@social-harness/social-harness` ships unchanged; the build copies them
  into `dist/skills`.
- Hand each inbox entry to the host as data: a message of one text part with
  no reminder as its text, and any other entry as its JSON. Add no prose.
- Leave outbound queue and retry policy to OpenClaw. Every plugin callback is
  one Client operation; do not pass queue identity or add provider-owned retries.
- Inbox reads record delivery before the native inbound callback. Propagate
  callback failures; they do not restore pending daemon delivery. OpenClaw owns
  processing, notification and task recovery.
- History and status use the MCP catalog.
- Keep host failures and Client failures typed at the boundary. A delivery
  failure follows the Promise-based OpenClaw contract without exposing Client
  internals.
- Never use `unknown` types; define explicit host-facing interfaces.

Send returns no receipt; completion means the daemon durably accepted the
message. Private retry state never enters the adapter contract.

## Tests

- Unit tests may fake the public Client capability to verify canonical
  projection, stock routing and outbound callbacks.
- Real-host qualification covers shared main-session context, private final
  text, explicit-target sends, successful callback handoff, and propagated
  callback failures. It does not require processing confirmation.
- Integration tests exercise the public Client boundary.
- Run package tasks through Nx from the workspace root.
