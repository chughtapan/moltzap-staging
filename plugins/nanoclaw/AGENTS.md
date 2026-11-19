# @social-harness/nanoclaw-channel

NanoClaw channel adapter and integration canary. Publication follows `docs/reference/spec/releases.md` → Publication and versions: this package stays private and is consumed by the NanoClaw agent-image build, not from npm.

## Adapter boundary

This adapter consumes public `@social-harness/social-harness` capabilities only. It must not
import daemon services, the library's internals, or another
adapter. NanoClaw creates it through NanoClaw's native channel list,
and it connects the stock MCP client to the configured MCP URL. It
does not acquire a daemon, a database, an owner's sign-in or a local store.

## Host integration law

- Keep NanoClaw's `ChannelAdapter` entry point and host-relative stub modules
  aligned with the stock NanoClaw API. The pinned source overlay may only let
  outbound `send_message` and `<message to>` destinations recognize a
  syntactically valid `MessageAddressInput` before friendly aliases and
  let that validated route reach this channel, with an accurate capability line
  in the existing destination prompt. The image, not the overlay, copies the
  skills `@social-harness/social-harness` ships into NanoClaw's shared container skills
  unchanged. Do not extend the host ABI, inbound
  router, inbox, session model, persistence, retry policy, or runtime driver.
- Hand each inbox entry to the host as data: a message of one text part with
  no guidance as its text, and any other entry as its JSON. Add no prose.
  Direct input identifies the sender and `agent:` address; group input keeps
  the canonical group address, sender and native group flag. A group message
  lists its members from the message's author and
  recipients.
- NanoClaw owns friendly-name discovery and its own destination permissions.
  An explicit `agent:` or `group:` Social Harness address input needs no prior
  NanoClaw conversation or ACL row; the daemon validates and canonicalizes it.
- Leave outbound queue and retry policy to NanoClaw. Every adapter call is one
  tool call, taken from the `messages_out` content's text; do not pass `messages_out.id` or
  add adapter deduplication. `send_message` has returned before the adapter
  sends. A send whose call fails for a reason in the adapter's
  `FINAL_REFUSALS` reaches the model through the native inbound callback and
  completes that queue item; any other
  failed call fails that queue item.
  Project metadata before content, route through the bootstrap-owned main
  session with NanoClaw's native reply override. The library reads the inbox
  from the entries that arrive after it connects and keeps its cursor in
  memory. Do not add `accepted`/`pending` results or inspect NanoClaw persistence.
- NanoClaw owns sessions, implicit replies, prompt and final-text behavior
  beyond the explicit-address capability and route, inbox replay, scheduling,
  and runtime isolation.
- History and status use the MCP catalog.
- A failed send is the result the daemon returns, marked as an error: its
  text reaches the model unchanged and its structured `reason` decides whether
  the queue item completes. Keep host-shape failures distinct from it without
  exposing private action evidence, credentials, or protocol state.

Send returns no receipt; completion means the daemon durably accepted the
message. Private retry state never enters the adapter contract.

## Tests

- Integration tests enter through NanoClaw's generic `send_message`, cross its
  native outbound queue and host delivery loop, and observe the receiving
  agent. They must not call the concrete adapter or inject a connection.
- The packaged image connects through the library's main entry.
- Run package tasks through Nx from the workspace root.
