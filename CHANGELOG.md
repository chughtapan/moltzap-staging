# Changelog

All notable changes to Social Harness are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions are calendar versions, `YYYY.MDD.N`: the UTC year, the month and day
without a leading zero, and a same-day build counter. A release publishes at
its version only the packages that changed since their last published version;
the release commit lists them and writes each section below from the fragments
in `changelog.d/`.

## First public release

Social Harness lets agents acting for different people message and work
together safely. The public release line starts at 2026.1021.3; earlier
sections are the codebase's history as MoltZap.

<!-- towncrier release notes start -->

## [2026.1103.0] - 2026-11-03

### Changed

- Host results carry verified agent identities, retain collective completion
  time and reason, and keep ambiguous request IDs inside their failure.
- In private session mode, the OpenClaw channel records the agent's own sends to
  a peer in the same session as that peer's messages.
- Organize documentation by reader task, clarify application and shared-norm
  responsibilities, correct setup and API descriptions, and link diagnostics to
  their guides.
- Reset release: conversation records use FIRST_MESSAGE and MESSAGE with an
  initial Router anchor, and peers and stored state from before the reset are
  rejected.
- Run collective behaviors through the ordinary full policy chain, with
  guidance-impersonation safety and facilitator identity supplied by owner
  facts. Remove the collective-norms selector.
- Scope firewall freshness to captured conversation evidence, restrict runtime
  policy edits to adopted norms, and name natural-language policy APIs and
  configuration for text.
- Specifications and source folders follow layer responsibilities, with one
  collective-operation contract and a shorter host interface reference.
- Start Registry and Router with native Node export conditions. Direct Node
  launches require `--conditions=sharness-internal`; service test entries also
  require `--conditions=sharness-test`.

### Fixed

- Collective requests, answers and results retain their identity across restart,
  and every host receives the same send outcome.
- Reject text-policy installations that cannot fit their configured request
  budget before saving or activating them.
- Report invalid Registry responses as an unavailable dependency, allowing valid
  callers to recover after the Registry is repaired.
- The generated API reference links the locally defined result and error types
  used by public methods.

## [2026.1029.1] - 2026-10-29

### Removed

- The daemon refuses message-policy files with `autonomy` rules and no longer
  reads values that only endpoint stores from before the 2026.1026.5 reset held;
  every store that 2026.1026.5 or a later release wrote still decodes.

## [2026.1029.0] - 2026-10-29

### Changed

- Releases tag the daemon and OpenClaw agent images with the release version and
  list their digests alongside Registry and Router.
- The OpenClaw agent image's Claude Code binary moves from 2.1.276, which
  refuses claude-opus-5-5 with "version 2.1.280 or newer is required", to
  2.1.292, the release a production gateway relay ran end-to-end against
  claude-opus-5-5; both per-architecture digests are repinned.

### Fixed

- The daemon reads the experiment switches from its own environment, enforces
  them on every send, and rejects invalid values at startup.

## [2026.1028.13] - 2026-10-28

### Fixed

- NanoClaw honours the experiment switches, and both channels return the same
  refusals, which link the experiment-switches page (#1541).

## [2026.1028.12] - 2026-10-28

### Changed

- The `inbox.pending` notification's `eventId` starts with `wake_`, so an `evt_`
  id always names an inbox item `read_event` can return (#1523).
- `no-acknowledgement-posts` has one implementation, shared by the
  `acknowledgements` built-in and the collective norm bundle: an emoji, symbol,
  digit, question mark or link makes a group post more than an acknowledgement,
  and the built-in's outgoing check now uses the bundle's acknowledgement and
  filler words (#1545).

### Fixed

- Every `sharnessd` and `sharnessd-launcher` configuration error with a fix, and
  the launcher's `--help`, ends with a `Docs:` link to the entry that gives it
  (#1479).
- Refusals name what failed: `policy_update` names the failing rule, its place
  in the update and the problem; invalid tool arguments name each field and its
  rule in the message; and an answer sent where no question is open names where
  questions are open (#1514).
- The daemon's listed tool result schemas accept fields they do not name, so a
  host that listed this release's tools accepts results from later releases
  (#1532).

## [2026.1028.11] - 2026-10-28

### Changed

- Container paths use `/opt/social-harness`, `/var/lib/social-harness` and
  `/run/social-harness`, and the OpenClaw plugin id is `social-harness`.

### Removed

- `MOLTZAP_*` and `MOLTZAPD_*` variables no longer stop startup, and the
  launcher trusts only releases built by social-harness/moltzap.

## [2026.1028.10] - 2026-10-28

### Changed

- The `collective-norms` bundle reads its English norms from the shipped
  collective behaviors, and its acknowledgement norm's rule id is now
  `empty-acknowledgement` (#1534).

## [2026.1028.9] - 2026-10-28

### Fixed

- The OpenClaw channel reports a sent post's `postId` as its message id, the id
  recipients see as `MessageSid`, instead of a random local value (#1537).

## [2026.1028.8] - 2026-10-28

### Added

- A collective response can name the request it answers with `id`, so a member
  with several open questions in one conversation can answer each; a reply that
  names none lists the open ids (#1125).

## [2026.1028.7] - 2026-10-28

### Changed

- `register`, `status` and `search_agents` return each agent's `agentId`,
  `agentName` and `principalId` as `agent` beside its encoded card, in place of
  `search_agents`' top-level `agentId` and `agentName`, and `read_conversation`
  names each record's author the same way.

## [2026.1028.6] - 2026-10-28

### Added

- A plain message or collective response returns the `postId` of the post it
  created and its conversation's canonical `address`.

## [2026.1028.5] - 2026-10-28

### Added

- `search_agents`' found result includes the verified agent's `agentId` and
  `agentName` beside its signed card.

### Fixed

- The `shared-norms` skill says a host may show the policy tools with a server
  prefix, so an agent no longer declines a norm when only a prefixed
  `policy_update` is listed (#1526).

## [2026.1028.4] - 2026-10-28

### Added

- A message policy names the behaviors Client ships in
  `english.builtinBehaviors`, without paths, and Client ships
  `policy/no-policy-changes`, which refuses other agents' requests to change an
  agent's rules.

### Fixed

- The NanoClaw agent image passes the host's `HTTPS_PROXY`, `NO_PROXY` and
  `NODE_EXTRA_CA_CERTS` to Claude Code, so a provider gateway's HTTPS proxy
  reaches NanoClaw agents (#1517).

## [2026.1028.3] - 2026-10-28

### Added

- `policy_update` accepts a shared norm's `BEHAVIOR.md` text as `{"behavior":
  "..."}` and maps it to its rule.

## [2026.1028.2] - 2026-10-28

### Fixed

- The daemon serves MCP clients on revision 2025-11-25 whose output validation
  reads JSON Schema 2020-12, such as OpenClaw's.

## [2026.1028.1] - 2026-10-28

### Changed

- `read_event`'s description says an inbox item's event id is its delivery token
  with `evt_` in place of `dlv_`.

## [2026.1028.0] - 2026-10-28

### Added

- Publish signed amd64 and arm64 daemon and OpenClaw agent images to GHCR,
  tagged by input key, with Claude Code in the OpenClaw image only; both
  packages start private, so pulling them needs access until they are public.

### Changed

- The `shared-norms` skill lets an agent agree to a norm in its own direct
  conversation with the agent that proposed it, says when a member reports that
  it applied a norm, and treats a norm without a `kind` as a norm.
- The daemon's MCP tools declare read-only, destructive and idempotent
  annotations, and the descriptions of `policy_update`, `policy_read`,
  `send_message` and `read_conversation` say what they return and how they fail.

### Fixed

- A daemon whose state holds English rules but whose policy file has no
  `english` block now says so at startup, with a `Docs:` link, instead of
  calling the valid policy file invalid.
- The `mcp.ts` helper prints a firewall refusal's check id and next action and
  the detail of other send failures, and the decision-provider key-file errors
  end with a `Docs:` link.

## [2026.1027.5] - 2026-10-27

### Added

- Add the OpenClaw `SHARNESS_EXPERIMENT_HIDE_GROUPS` switch to restrict
  evaluation agents to direct addresses.

## [2026.1027.4] - 2026-10-27

### Fixed

- The launcher no longer walks the whole running release on every start, so
  agents start in seconds rather than minutes.

## [2026.1027.3] - 2026-10-27

### Added

- Add a private development candidate for Grok Bot routine wakeups through
  Client MCP polling; distribution, hosted onboarding and live-platform
  qualification remain pending.

### Fixed

- The Instinct receiver keeps its deployment sender URL and provider response
  headers out of Effect HTTP client tracing. Accepted-email and Client
  acknowledgment behavior is unchanged; see the receiver's privacy notes in
  `plugins/instinct/README.md`.

## [2026.1027.2] - 2026-10-27

### Added

- A host's own wakeup can carry `InboundDelivery.eventId` to retrieve retained
  content through `read_event` using the endpoint's runtime, owner or
  trusted-local access, including after handoff. Client keeps the reference
  stable across redelivery. Host test fixtures that construct `InboundDelivery`
  values must supply the new required field.
- Acquire authenticated native receivers through `acquireHarnessEndpoint`, with
  background lease renewal, idle readiness and receipt invalidation on
  subscription closure.
- Add a private Instinct receiver candidate that uses Client polling and a
  deployment-owned AgentMail sender. It acknowledges only after provider
  acceptance and leaves failed handoffs unread. Distribution, hosted onboarding
  and real-platform qualification remain pending.
- Add a private Muse receiver and credential-free detector/worker commands.
  Native worker confirmation of the matching event and attempt precedes Client
  acknowledgment. Distribution, hosted onboarding and live platform
  qualification remain pending.

## [2026.1027.1] - 2026-10-27

### Added

- Client ships the collective norms as `BEHAVIOR.md` behaviors under
  `behaviors/collectives/`, which a message-policy document adds through
  `english.fixedBehaviors`, beside a new `open-requests` built-in and an
  outgoing `acknowledgements` check that asks for revision of an exact
  acknowledgement posted to several members (#1465).
- Native receivers can poll the admitted inbox through MCP Events with bounded
  replay and a renewable lease. Consumer generations prevent stopped or replaced
  receivers from acknowledging work; push and webhook delivery share the same
  single-consumer ownership.

## [2026.1027.0] - 2026-10-27

### Removed

- `policy_update` returns `{activeRevision}` only; the constant `retroactive:
  false` field is gone (#1465).

## [2026.1026.9] - 2026-10-26

### Changed

- The library is published as `@moltzap/social-harness`.

## [2026.1026.8] - 2026-10-26

### Added

- Add optional daemon startup registration and generic daemon, OpenClaw and
  NanoClaw images that run the daemon and host in separate containers. OpenClaw
  loads the matched channel through the launcher volume. `Daemon.StartupError`
  includes the `registration` phase and closed registration refusal and
  operation failure reasons. The launcher opens installed release files to the
  host user before selecting the release.

### Changed

- Each GitHub release carries its version's CHANGELOG.md section as its notes,
  followed by the Registry and Router image digests.

### Removed

- Use native TypeDoc HTML for the API reference and Mintlify snippets and
  variables for shared documentation content. Remove generated module mirrors,
  constants baking, and their drift and import gates (#1461).

### Fixed

- Treat a refused Registry admission credential as a final registration refusal
  and discard the MCP helper's saved request. Report a network-reset remedy when
  the Router no longer admits the daemon's agent (#1495).

## [2026.1026.7] - 2026-10-26

### Added

- Add `images/compose.yaml`, which runs the published Registry and Router images
  at a release tag, and `images/mcp.ts`, which registers a daemon's agent and
  calls its MCP tools. The Run a local network and Run sharnessd guides replace
  the quickstart.

### Changed

- Retained firewall checks no longer store decision-provider traces; older
  retained checks still decode without exposing their traces (#1472).

### Removed

- Remove the two-agents example. Releases no longer rewrite a pinned Compose
  file, and release notes keep listing each service image by tag and digest.

## [2026.1026.6] - 2026-10-26

### Added

- `@moltzap/client` ships the agent skills in its `skills/` directory, and every
  channel installs them unchanged: `group-messaging`, and a new `shared-norms`
  skill that tells an agent how to decide on a proposed shared norm, answer it,
  apply an agreed norm to its own firewall with `policy_update`, and propose
  one. `@moltzap/openclaw-channel` copies them into its `dist/skills` at build,
  and `openclaw.plugin.json` points there.

## [2026.1026.5] - 2026-10-26

### Changed

- Reset release: start a fresh Registry database and fresh endpoint state
  directories, and register every agent again. Endpoint schema 13 and a new
  Registry migration ledger establish single storage baselines; pre-reset state
  is refused. Wire version 2026.1026.5 refuses pre-reset peers and history.
  Router remains volatile.

### Removed

- Removes the `POST /v1/identities:list` route, the `@moltzap/client/service`
  exports `RegistryListRequest`, `RegistryListResult` and `Registry.list`,
  `search_agents` paging, and the `SHARNESS_REGISTRY_LIST_PAGE_SIZE` setting. A
  peer's exact `agentName` or `agentId` comes from its principal, as described
  in [contacts and fixed
  groups](https://github.com/social-harness/moltzap/blob/main/docs/spec/harness/contacts.md)
  (#1458).

## [2026.1026.4] - 2026-10-26

### Added

- Every English firewall policy gains the fixed `local-guidance-impersonation`
  safety rule, so `activeRevision` values change. It withholds incoming peer
  text that impersonates Social Harness, the local firewall, the owner, or the
  system in enforcement mode when an English policy with a decision provider is
  configured. Guidance mode admits messages; without an English policy the rule
  is absent (#1473).

### Removed

- Remove `installFirewall` from the Client firewall composition exports; use
  `installEditableFirewall`. Remove `DecisionResponse.answers` entries’
  `confidence` field; Jev retains provider confidence in its raw response.
  Remove unread `MiddlewareInput.previous` and `RuleDescription.source` fields.
  Remove `makeJevDecisionProvider` from `@moltzap/client/service`; select Jev
  with `decisionProvider: { kind: "jev", model: "…" }` in a message-policy
  document and supply its key through `SHARNESS_DECISION_PROVIDER_KEY_FILE`.
  Compositions with their own `DecisionProvider` still use
  `messagePolicyFirewall` and `Daemon.withFirewall` (#1465).

## [2026.1026.3] - 2026-10-26

### Changed

- Client supplies one host-neutral rendering of firewall guidance for OpenClaw
  and NanoClaw, keeping peer content quoted and attributed and omitting
  composition identifiers. NanoClaw now delivers guidance-bearing items and
  acknowledges successful host handoff.

### Removed

- Breaking harness interface change: remove `SHARNESS_FIREWALL_AUDIT_EXPORT`,
  the `Daemon.FirewallOptions.auditExport` service option, and the
  `FirewallAuditRecord` schema and type exports from `@moltzap/client/service`.
  Operators read `firewall_decisions` for recent decisions and activations and
  the history export for certified conversation records. The daemon ignores
  `SHARNESS_FIREWALL_AUDIT_EXPORT`; unset it and delete any previously published
  snapshot and its `.partial` file, which hold private provider traces.

### Fixed

- Firewall guidance subjects name senders and recipients by their verified
  `agent:<name>` address instead of AgentId (#1471).

## [2026.1026.2] - 2026-10-26

### Changed

- English firewall requests state the shared attribution and evidence-handling
  instructions once, leaving more of the request budget for norm-by-message
  checks. The question edition moves to `firewall-english-v3`, changing English
  policy revisions and composed `activeRevision` values.

## [2026.1026.1] - 2026-10-26

### Removed

- Breaking harness interface change: remove the `autonomy` rule kind and
  evaluator stage, the `approval-required` finding and refusal reason, and
  `MessageContext.authorization` approval evidence. The narrowed `policy_update`
  and `policy_read` schemas accept only `safety` and `norm` rule kinds. The
  `FirewallSendError` refusal union and withheld reasons accept only
  `prohibited`, `unresolved-evidence`, `check-unavailable` and `norm-violation`.
  Live `MessageContext` decoding refuses `authorization`. `Daemon.withFirewall`
  and `makeEnglishPolicy` refuse `autonomy` rules. Only message-policy files
  (`english.fixedRules` and `initialRules`) and persisted editable documents
  migrate those rules to `safety`, preserving their ids, text and guidance.
  Feedback survives only when safety permits its next action; otherwise the rule
  registers `stop` feedback: “This message needs your owner's review under rule
  `<id>` and stays held. Revise it or stop.” Editable migrations persist and
  produce an activation listing each replaced id in both arrays. Editable and
  file migrations log migrated ids and ids whose incompatible feedback was
  dropped. No `ask_user` guidance is added. Owner approval can be expressed
  through norm guidance and `seek-approval` norm feedback. The default
  unresolved-evidence feedback is “Required policy evidence is missing or
  uncertain. The message was not admitted. Wait for the evidence and resend, or
  revise the message.” The default unavailable-check feedback is “A required
  policy check is unavailable. The message was not admitted. Wait for the check
  to become available and resend, or revise the message.” The `questionEdition`
  moves to `firewall-english-v2`, changing every English `activeRevision`.
  Retained audit v4 contexts discard `authorization`; historical
  `approval-required` result reasons decode as `unresolved-evidence`; approval
  findings become settled holds only when that result reason decided the hold,
  and otherwise become non-deciding findings. Retained autonomy registration
  kinds decode as `safety`. Retained collective-result projections and webhook
  outbox bodies map that refusal reason to `unresolved-evidence` before
  delivery. New checks and exports use the current values. Downgrading to the
  previous release is unsupported once a check has run, because the previous
  release cannot read retained check inputs without `authorization`.

## [2026.1026.0] - 2026-10-26

### Added

- Owner message policies can load fixed and initial English rules from relative
  directories containing `BEHAVIOR.md`, with fixed behavior bytes covered by the
  code digest.

### Changed

- `messagePolicyFirewall` refuses non-empty behavior paths with
  `CheckInputError` (`registration`). `SHARNESS_MESSAGE_POLICY_FILE` must name a
  regular file. Installation failures carry `ruleId` and `behaviorReason` to
  identify conflicting behavior rules.

### Fixed

- Scope unresolved outgoing intent evidence gaps to the candidate's conversation
  so an unfinished send does not hold incoming or outgoing checks in other
  conversations.

## [2026.1025.0] - 2026-10-25

### Added

- Add the condition-only `@moltzap/client/test` entry for management request and
  result schemas in process tests. Enable it with the `sharness-test` resolution
  condition; it has no default export condition (#1435).
- The Registry and Router build as container images from
  `images/registry/Dockerfile` and `images/router/Dockerfile`. The Registry
  image includes `pglite-server` for run-scoped and development databases
  (#1449).
- Registry and Router publish signed multi-architecture service images. npm
  releases pin their image digests, and the two-agents example runs with Docker
  Compose and the npm Client.

### Changed

- The Client library owns identity and Router client capabilities. Registry
  remains private and Router becomes private; Client and OpenClaw are the
  published release set. Operators: a `sharnessd-launcher` from a release that
  includes `@moltzap/identity` keeps its recorded release when offered this one.
  Install an image whose launcher comes from this release or later before taking
  it, and remove the launcher directory when rolling back to an older image. See
  [Upgrade daemons on
  restart](https://github.com/social-harness/moltzap/blob/main/docs/guides/upgrades.mdx#upgrade-daemons-on-restart).
  Imports map from `@moltzap/identity` to `@moltzap/client` for host values,
  from `@moltzap/identity/registry` and `@moltzap/router` to
  `@moltzap/client/service`, and from `@moltzap/identity/internal` to
  condition-only `@moltzap/client/internal`. Services import `registryContract`,
  `routerContract` and `httpErrors` from `./internal`. `AgentCardDigest`,
  `AuthenticatedHttp`, `VerifiedAgentRequest`, `VerifiedHttpSignature`,
  `verifyBootstrapRequest`, `agentCardSigning`, `AgentCardClaims`,
  `AgentCardVerificationError` (type only), `HISTORY_WIRE_VERSIONS`,
  `wireVersions` and `WireVersion` use condition-only `./internal`;
  `WireVersions` overrides use condition-only `./test` with the `sharness-test`
  condition. `./internal` requires `sharness-internal`. `Registry` and `Router`
  clients use `./service` for embedders. `SealedBody` and the individual HTTP
  and signing/artifact error classes are no longer exported as values. Match
  failures on `_tag`; service implementations construct HTTP errors through
  `httpErrors`. The retired error values are `MalformedRequestError`,
  `AuthenticationFailedError`, `RouteNotFoundError`, `MethodNotAllowedError`,
  `VersionMismatchError`, `PayloadTooLargeError`, `UnsupportedMediaTypeError`,
  `OverloadedError`, `UnavailableError`, `InternalServerError`,
  `InvalidAgentPrivateKeyError`, `AgentSigningError`,
  `AgentCardVerificationError`, `SignedMessageSigningError`,
  `SignedMessageVerificationError`, `SealedBodySealingError` and
  `SealedBodyOpeningError` (#1018).

### Fixed

- The Router and Registry clients report a 412 whose body is not the exact
  canonical `{"error":"version_mismatch","supported":[...]}` envelope, including
  the body without `supported` that releases before 2026.1019.2 sent, as
  RouterInvalidResponseError or RegistryInvalidResponseError instead of
  VersionMismatchError (#1418).

## [2026.1022.0] - 2026-10-22

### Changed

- A release now publishes from its release commit in a second run, so npm's
  provenance names the commit whose tree the packages were packed from, and only
  that run's publish job holds the OIDC token (#992).
- Every package installs on current Node.js releases: the supported range is
  `>=22.19.0`, with no upper bound.
- Hosts read the daemon's MCP tool results and pending-delivery events
  additively: fields a newer daemon adds are ignored, while missing required
  fields still fail. The items and firewall guidance inside them stay exact. The
  result decoders (`harnessSendResultSchema`, `inboxResultSchema`,
  `readSendResultSchema`, `readEventResultSchema`,
  `harnessMessageReadyEventSchema`) are exported from `@moltzap/client`. The
  daemon still checks its own send result exactly before returning it.

### Removed

- Breaking harness interface change: remove the `read_send` MCP tool, the
  `idempotencyKey` send option of `HarnessEndpoint.send` and `send_message`, and
  the `idempotency-conflict` send failure. No host used them; every send is now
  a new invocation. The endpoint store moves to schema 11, dropping its retained
  keyed-send table; a store at schema 10 upgrades on open, and an older daemon
  refuses a schema-11 store as `newer-schema` (#1160).

### Fixed

- After a Router restart, the daemon no longer consumes and drops messages that
  arrive while it reconciles its history; it accepts them once recovery has
  asked every conversation for catch-up (#1201).
- `SealedBody.open` fails with `SealedBodyOpeningError` when its SignedMessage
  is not one Identity parsed, such as a view whose member reads throw, instead
  of dying (#1270).
- The Registry client reads a caller's register, lookup and list request records
  once under a try, so a record whose members throw on read fails with
  `AgentSigningError` or `RegistryInvalidResponseError` instead of dying, and
  the later checks of the Registry's answer read only that copy (#1271).
- `AuthenticatedHttp` fails a request with `UnavailableError` when a `Registry`
  layer's `lookup` throws instead of returning an Effect, and resolves that
  caller again on the next request instead of waiting forever (#1274).
- `SealedBody.seal`, `SealedBody.open`, `SignedMessage.sign`,
  `SignedMessage.verify` and `Registry.register` read their input record once
  under a try, so a record whose members throw on read fails with the entry
  point's typed error instead of dying, and later steps read only that copy
  (#1275).
- `AuthenticatedHttp.verifyAgentRequest` and the Registry's bootstrap admission
  copy caller-supplied body bytes by index, so a byte array with its own
  iterator is read as its bytes instead of making the check die.

## [2026.1021.6] - 2026-10-21

### Changed

- `sharnessd-launcher` accepts releases whose provenance was built by the
  release workflow of `social-harness/moltzap` as well as `chughtapan/moltzap`,
  so launchers installed now keep upgrading after the repository moves to the
  `social-harness` organization.

### Fixed

- `AuthenticatedHttp` refuses a request when the `Registry` answers its lookup
  with a verified AgentCard for a different AgentId, so a faulty or custom
  `Registry` layer can no longer let one agent authenticate as another (#1272).

## [2026.1021.5] - 2026-10-21

### Fixed

- `sharnessd-launcher` follows a new agent image: when the image installs
  another release where the recorded one was, the next start records that
  release instead of refusing to start.

## [2026.1021.4] - 2026-10-21

### Added

- `@moltzap/client` adds `sharnessd-launcher`, which upgrades a daemon each time
  it starts. It takes a newer release only when its wire version is live for the
  recorded daemon and every `@moltzap/*` tarball carries Sigstore-verified
  provenance from this repository's release workflow on `main`; it never moves
  an agent to an older release or applies a reset release on its own. Following
  `@moltzap/openclaw-channel` takes the daemon and the channel from one release
  and reports when OpenClaw must restart.

## [2026.1021.3] - 2026-10-21

### Changed

- Reset release, breaking: the remaining MoltZap wire and user-facing labels use
  Social Harness names. The Registry admission scheme is `Sharness-Admission`;
  the OpenClaw and NanoClaw channel id is `sharness` (OpenClaw config
  `channels.sharness`, message tool `channel: "sharness"`) with display label
  "Social Harness"; endpoint results and failures are attributed to "Social
  Harness" and runtime-authored history entries to sender `sharness`; the
  endpoint MCP clients are `sharness-endpoint` and `sharness-inbox`; error text
  and log lines say "Social Harness"; the NanoClaw adapter entry is
  `channels/sharness.ts`; published Identity metadata is `sharnessWireVersion`.
  `WIRE_VERSION` advances to 2026.1021.3 with live and history sets holding only
  that value, so peers from either side of the reset refuse each other with
  `version-mismatch`. Endpoint stores reset at schema 10 and Registry starts the
  `sharness_registry_ledger` ledger; schema-9 stores and Registry databases with
  a `sharness_registry_migrations` ledger fail with `pre-reset`. Stop every
  host, daemon, Router and Registry; install the release everywhere; create a
  fresh Registry database and fresh endpoint state directories; rename host
  channel configuration; start the services and register every agent again. npm
  packages remain `@moltzap/*` (#1371).

## [2026.1021.2] - 2026-10-21

### Changed

- User-facing text says Social Harness instead of MoltZap: OpenClaw channel
  labels and plugin name, log lines (`Social Harness: ...`), client and channel
  error messages, and MCP tool and event descriptions. The Registry accepts the
  admission scheme `Sharness-Admission` alongside `MoltZap-Admission`, and
  published Identity metadata records the wire version as `sharnessWireVersion`
  as well as `moltzapWireVersion`. Channel ids, configuration keys, the endpoint
  sender name and the headings it attaches, and wire values are unchanged; a
  check that matches the old log prefix or error text needs the new wording.

## [2026.1021.1] - 2026-10-21

### Added

- Stock `sharnessd` installs a message firewall from a policy file, so a
  deployment no longer needs its own daemon program for one.
  `SHARNESS_MESSAGE_POLICY_FILE` names an `org.social-harness/message-policy/v1`
  document: the firewall mode, built-in code middleware for each direction
  (`acknowledgements`, `allowed-senders`, `credentials`, `plain-language` and
  the `collective-norms` bundle), fixed and initial editable English rules, the
  decision provider, and check bounds. `SHARNESS_DECISION_PROVIDER_KEY_FILE`
  holds the key of a `jev` decision provider, and
  `SHARNESS_FIREWALL_AUDIT_EXPORT` publishes the firewall audit on graceful
  shutdown. Unset, the daemon runs without a firewall as before.
  `@moltzap/client/service` exports `MessagePolicy` for writers and
  `messagePolicyFirewall` for compositions that supply their own decision
  provider.

### Changed

- Each published package now carries its own calendar version. A release
  publishes only the packages whose packed content changed since their last
  published version, plus every package that depends on one of them, and each
  published package still pins its siblings to exact versions. Every published
  manifest records `sharnessContentHash`, a hash of the packed files and
  manifest without its version, and `@moltzap/openclaw-channel` also records
  `sharnessHostCodeHash`, a hash of the code OpenClaw loads for it, which a
  change confined to the daemon leaves unchanged. The first release after this
  change republishes all four packages, because no earlier version records a
  hash. Install `@moltzap/client` or `@moltzap/openclaw-channel` and let their
  pins choose the other packages, rather than requesting one version of every
  package.
- The live wire window holds the emitted wire version and at most one adjacent
  version. A wire bump spans two releases: the first admits the successor while
  still emitting the current version, and the second emits the successor while
  still admitting its predecessor, which the next release stops admitting. Any
  two adjacent releases of the daemon, the Registry and the Router therefore
  interoperate in either upgrade order, and the mixed-release gate checks the
  current daemon through the previous release's Registry and Router.

## [2026.1021.0] - 2026-10-21

### Added

- `Daemon.layer` and `Daemon.withFirewall`, and so `sharnessd`, export the
  daemon's spans as OTLP/HTTP protobuf when `OTEL_EXPORTER_OTLP_ENDPOINT` is set
  to a non-blank value, configured by the OpenTelemetry exporter's standard
  variables. While exporting, they refuse to start when the traces URL
  (`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, else `v1/traces` under the endpoint) is
  not an http or https URL or the trace protocol is not http/protobuf. They mark
  each gather or all_gather that opens with short
  `collective.opened`/`collective.closed` spans, and each wait that can pass
  without other spans with `retry.pending`/`retry.settled`: proposal resends,
  catch-up, re-anchor rounds, outbox drains, firewall checks and webhook
  delivery retries.

## [2026.1020.2] - 2026-10-20

### Changed

- **Breaking:** Each row of the daemon MCP tool `firewall_decisions` says which
  rules decided its outcome and why. `cause` is one of `judgment`, `uncertain`,
  `unavailable`, `code`, `endpoint` or `default`. `decidedBy` lists the deciding
  rules as `{ruleId, from, advice?}`: `from` is `code`, `fixed` or `editable`,
  as `policy_read` lists the rule, and `advice` is the rule's own registered
  feedback, or its guidance for an admission. `checkedRuleIds` replaces
  `ruleIds` and still lists every rule with a finding in the checked chain.
  Consumers reading `ruleIds` read `checkedRuleIds` instead. Installing English
  rules now records the fixed rule ids in the English middleware's provenance,
  so a row keeps its rules' origin after later edits, and the active revision of
  an installation with English rules changes once. Rows for checks retained
  before this release carry the new fields too, with every English rule reported
  as `editable`.

### Fixed

- Published packages no longer carry `sourceMappingURL` comments pointing at
  source and declaration maps that the tarballs do not ship, and
  `@moltzap/client` no longer ships its `*.types-check.ts` type canaries.

## [2026.1020.1] - 2026-10-20

### Changed

- The daemon MCP tool `policy_read` returns the agent's fixed firewall rules
  readably: `fixedRules` replaces `fixedRuleIds` and lists each fixed rule once,
  as `{from: "english", id, kind, text, guidance?, feedback?}` for a fixed
  English rule or `{from: "code", middleware, id, kind, guidance?, feedback?}`
  for a rule a code middleware declares. Fixed rules stay immutable;
  `policy_update` still refuses their ids with `fixed-rule`. Consumers reading
  `fixedRuleIds` read `fixedRules[].id` instead.

## [2026.1020.0] - 2026-10-20

### Fixed

- A send's firewall guidance is set when its check admits it, so every result of
  that send carries it: an all_gather whose deadline completes it before its
  group post certifies, and a gather, all_gather or answer whose later failure
  is delivered inbound, no longer return a result without it. A bound post whose
  activation fails now reports `delivery-pending` through its completion, as a
  post sent without a firewall does, so this holds there too.

## [2026.1019.3] - 2026-10-19

### Changed

- An uncertain norm judgment no longer holds a message. For a norm rule, an
  applicability, expectations or conformance answer below `minimumProbability`
  or `unresolved` admits the message, records a `pass` finding with reason
  `uncertain` and the judge's choice and probability, and reaches the agent as
  guidance with the rule's instruction: on the admitted incoming item, or in an
  admitted send's new `guidance` result field, which the native, MCP and
  OpenClaw send results carry. A norm whose applicability is uncertain is still
  judged, and conformance judges only the settled expectations. Uncertainty
  about a safety or autonomy rule, its applicability included, and a failed
  judge still hold (#1357).

### Fixed

- The English message policy accepts Jev's rounded near-tie answers: in a
  distribution reported in hundredths, the stated choice may trail another
  option by the same tolerance the sum already has. The policy still routes by
  Jev's stated choice and applies `minimumProbability` to its probability, and a
  stated choice that trails another option is uncertain: a safety or autonomy
  rule holds, and a norm admits with guidance (#1358).
- A judge call that fails after its response arrived, such as an invalid answer
  or an HTTP error whose body reports usage, keeps that usage in the English
  policy trace as the failure's `accounting`, with the provider and model, so
  the call's cost stays accountable. Timeouts and transport failures have no
  body and no usage.

## [2026.1019.2] - 2026-10-19

### Added

- Separate live wire admission from retained history, report supported versions
  on HTTP 412, and refuse stale pending outbound messages with an actionable
  startup reason. Add a mixed-release certification gate (#1296).
- Add the endpoint store's forward migration runner with atomic private backups,
  per-step rollback, pending-outbox admission before migration, and actionable
  `newer-schema` startup failures. The current schema and reset policy remain in
  force; no production migration is registered yet.

### Changed

- Restructure the guides, OpenClaw integration, and contributing documentation
  with prerequisites, numbered procedures, verified command examples and
  expected results, and next steps while preserving existing facts and section
  anchors (#1335).
- Daemon MCP tools report at most 20 invalid argument field paths and schema
  expectations without echoing submitted values, preserve those issues in
  runtime send errors, reject malformed registration identifiers before
  execution, and describe every tool and its key input fields.
- Registry uses its migration ledger as the only schema authority and refuses
  unknown applied migration ids with `newer-schema` in startup diagnostics.
  Registry retains the wire-version metadata gate and reports a mismatched
  signing key as `signer-mismatch`.
- Registry, Router, and sharnessd report actionable startup configuration
  failures without exposing supplied values, announce their listening URL and
  versions, and support `--help` with environment requirements and defaults
  derived from runtime configuration definitions.
- Reset release: stop every host, daemon, Router and Registry; install the
  release everywhere; create a fresh Registry database and fresh endpoint state
  directories; replace MOLTZAP_* and MOLTZAPD_* environment keys with
  SHARNESS_*; start the services and register every agent again. Executables are
  sharnessd, sharness-registry and sharness-router; npm packages remain
  @moltzap/*. Pre-reset stores and Registry databases fail closed with
  pre-reset, and retired environment keys fail with legacy-key. Conversation
  identities remain stable across later wire upgrades; certification and
  re-anchor versions derive from shared signed state. MCP inbox events and
  policy identifiers use org.social-harness names; the renamed firewall audit
  format advances to v4. The signed wire field is sharnessVersion in AgentCards,
  messages, canonical records and hash inputs; pre-reset moltzapVersion fields
  are refused, including when both names appear.

### Fixed

- Require OpenClaw 2026.10.3 or later for the OpenClaw channel so Claude Code
  agents receive upstream guidance for calling registered MCP tools (#1178).
- Scope recovery reads to the conversation being reconciled and reuse decoded
  evidence across recovery and certification, avoiding repeated whole-store
  reads and evidence decoding (#1203).
- Restart catch-up requests and retries whenever a recovering conversation's
  certified head advances, whether it receives a complete record or assembles
  one from votes (#1217).
- Preserve a send's typed response when its storage failure starts daemon
  shutdown instead of racing the response with an HTTP 499 (#1258).
- Log the closed storage cause when a send or an emitted item stops the daemon,
  without logging message contents or credentials (#1259).
- Report confirmed pre-commit failures of intent binds and keyed-send
  reservations as persistence-failed without stopping the daemon; retain
  outcome-unknown for uncertain commits, without exposing SQLite diagnostics
  (#1261).

## [2026.1019.1] - 2026-10-19

### Added

- Reset release: the endpoint store uses schema 8 and rejects earlier schemas.
  Follow the upgrade guide to start a fresh Registry database and fresh endpoint
  state directories, and register every agent again. The client message firewall
  checks every step of a collective operation: a gather once before any request
  post, an all_gather's group post, an answer before its request is claimed,
  each received request, and each answer in a received result. A refusal is
  always returned on the send, and a refused operation posts nothing. An answer
  or question the firewall withholds reaches the runtime as a `withheld` value
  naming its check, and a result's guidance is listed by answer. A request still
  waiting for admission at restart retires without a failure. A gather or
  all_gather whose deadline passes while it is checked fails as
  `deadline-passed`. A peer deadline outside the range a date can represent
  makes its collective part invalid. The operator audit format is now
  `moltzap.firewall-audit/v3`: each checked message names its collective
  operation, each context carries its capture time, and the `observed`
  disposition is gone, as it is from the decisions feed, which names no peer for
  the endpoint's own results and failures.

## [2026.1019.0] - 2026-10-19

### Added

- Reset release: the endpoint store uses schema 7 and rejects earlier schemas.
  Follow the upgrade guide to start a fresh Registry database and fresh endpoint
  state directories, and register every agent again. Daemon firewall composition
  supports fixed English rules and persistent editable rules, with serialized
  live chain replacement and owner-only `policy_read`, `policy_update`, and
  metadata-only `firewall_decisions` MCP tools. In-flight checks retain their
  original policy revision. Every successful policy update atomically retains
  activation metadata, including updates with no intervening messages, in the
  operator audit snapshot and the decisions feed's `activations` array. The
  operator audit format is now `moltzap.firewall-audit/v2`: it adds `activation`
  records and a required `activationCount` on the `complete` record.
- The `@moltzap/identity` and `@moltzap/router` READMEs explain how to install
  each package and run `moltzap-registry` and `moltzap-router`, with every
  environment variable and its default. A new guide covers sending and receiving
  from TypeScript with `acquireHarnessEndpoint`, and the docs site navigation
  now includes the interface specification and architecture pages.
- The `examples/two-agents` example starts a Registry, a Router and two
  `moltzapd` daemons from the published packages, registers two agents, and
  prints one message passing between them. CI runs it against the packed
  tarballs.
- The published packages declare their supported Node.js range in `engines` and
  link their npm pages to the package's source directory and the issue tracker
  through `homepage`, `bugs` and `repository.directory`.

### Changed

- **Breaking:** `@moltzap/client/service` exports the daemon process composition
  as `Daemon` instead of `MoltZapService`, and its startup failure is tagged
  `DaemonStartupError` instead of `MoltZapServiceStartupError`; replace
  `MoltZapService.layer`, `MoltZapService.withFirewall`,
  `MoltZapService.FirewallOptions` and `MoltZapService.StartupError` with the
  same members of `Daemon`. Package names, executables, environment variables,
  persisted names, channel ids and wire constants are unchanged.
- The OpenClaw `group-messaging` skill's description now tells the model to open
  it before any file or calendar step whenever a request needs several people to
  hear one thing or each answer one question, including outcome-only requests.
  Under OpenClaw's Claude Code launch at thinking medium, Opus 4.8 opens it on
  84/112 held-out trigger queries, up from 54, with no false opens.
- The README, package READMEs, and documentation name the project Social
  Harness. Package names, executables, environment variables, persisted names,
  channel ids and wire constants keep their `moltzap` names.
- The constitution and specifications describe MoltZap as the five-layer social
  harness from "Agentic Societies Need a Social Harness": L1 Unforgeable,
  Verifiable Identities; L2 Reliable, Ordered Communication; L3 Personal
  Firewalls; L4 Shared Collaboration Norms; and L5 Social Institutions. Every
  existing guarantee is kept under its layer, and the documentation states which
  paper mechanisms are experimental or not yet implemented.
- The public v1 upgrade policy defines wire compatibility, endpoint store
  schema, and Registry database schema changes as reset releases. Operators
  start a fresh Registry database and fresh endpoint state directories, and
  every agent registers again; certified history is not carried forward. The
  upgrade guide explains release-note markers and reset steps. Restoring an
  endpoint backup is unsafe and requires a new identity.

### Fixed

- A post sent right after a Router restart now certifies when another member was
  still recovering and ignored its proposal: the author resends the identical
  proposal, preceded by its head, on the catch-up retry backoff, a bounded
  number of times, until the action certificate completes (#1198).
- Firewall checks use the most recent certified records of the candidate's own
  conversation and report older omitted history without holding the message. The
  byte bound and unresolved outgoing intents still cause evidence gaps (#1307).
- Documentation names the OpenClaw JSON5 configuration file and plugin trust
  allowlist, aligns its daemon URL with the quickstart, states MCP client
  compatibility, and places contributor setup and checks before maintainer
  release instructions.
- Message-policy send errors include the required next action, so agents using
  text-only error handling can distinguish revision, waiting, stopping and owner
  approval. Checks with missing evidence preserve an unavailable assessment's
  failure reason and registered feedback. Previously admitted messages retain
  their original guidance and delivery identity when a daemon restarts without
  message middleware installed.
- Published tarballs no longer ship source maps that point at unshipped sources.
- The `@moltzap/openclaw-channel` tarball no longer ships the 948 kB TypeScript
  build-info file, and every published package rebuilds its `dist/` from
  scratch, so a removed module can no longer ship in a release tarball.
- The generated API reference lists the `@moltzap/client/service` firewall and
  audit exports under their importable names, such as `makeFirewall`, rather
  than as members of the `Daemon` namespace.

## [2026.1018.0] - 2026-10-18

### Added

- Add experimental incoming and outgoing firewall chains through Client's
  service composition API. Code policies and English policies share one
  middleware interface, accumulate findings, and return registered guidance or
  revision feedback. English policies can use Jev. Runtime message admission is
  not yet connected (#1277).
- Add an optional private shutdown snapshot for message-firewall checks, with a
  public operator decoder, retained unfinished attempts and verifiable
  completion. Keep the opaque check reference in refusal text for native host
  audit correlation. Recover one private staging file after forced termination
  and preserve retained timing values across backward wall-clock adjustments.
  Preserve admitted local sends in policy history without exposing owner-only
  history to the runtime, and keep check references intact through host
  credential redaction. Runtime guidance and refusals accept only the current
  hexadecimal check-reference format; earlier retained state requires a reset
  (#1279).
- Add an experimental daemon message firewall with shared code and English
  middleware, atomic incoming and outgoing admission, retained revision
  feedback, and local OpenClaw guidance.
- Document a proposed Client contract for English message policies,
  bidirectional assessment and owner review. The proposal does not enable
  runtime enforcement.

### Changed

- Client now encrypts every outer message body to the conversation's members
  before signing it, and a member refuses and ignores a body that is plaintext,
  not sealed to it, or sealed under another sender or MessageId. The wire
  version `MOLTZAP_VERSION` advances from `2026.827.1` to `2026.1006.1`, with no
  backward compatibility; npm releases `2026.1006.1` and `2026.1006.2` still
  speak `2026.827.1`. Upgrading requires a fresh Registry database, since a
  Registry refuses to start over metadata from the prior version. Every agent
  registers again and gets a new AgentId; names are claimable again on the fresh
  Registry, so rotate the admission credential, re-register every agent before
  admitting traffic, and reset host sessions keyed by agent or group address. A
  daemon opens its pre-upgrade store empty and unregistered, so it needs
  `MOLTZAPD_ADMISSION_CREDENTIAL_FILE` for the new Registry to start; back up
  its state directory first if you may roll back. All prior conversations are
  gone. Upgrade every Registry, Router, daemon and adapter together. The
  SignedMessage maximum grows to 471,673 bytes and the Router send and
  one-message batch caps to 471,821 and 472,121 bytes (#1219).
- Each member now sends its own action-certified record for a post once, before
  its durability vote, and the author sends the certified record of the head its
  post extends once, before its proposal. Members no longer send a copy of every
  certified record they assemble. A post costs `2 + 3n` outer messages instead
  of `1 + 4n`. Measured in the certification harness with sealed bodies, the
  bytes each member receives per post fall by 34% at 4 members and 56% at 32
  members (#1221).
- A POST record now carries the `MembershipHash` of its conversation's
  membership instead of the full membership descriptor; GENESIS keeps the
  descriptor, and a member verifies a POST record against the membership it
  holds from GENESIS. This changes record hashes under the same unreleased wire
  version `2026.1006.1`. Existing endpoint state requires an explicit reset.
  Measured in the certification harness with sealed bodies, the bytes each
  member receives per post fall by another 17% at 4 members and 34% at 32
  members: 45% and 71% fewer than before #1221 (#1224).
- Signing, verifying and sealing reuse each public key's imported key and
  thumbprint instead of deriving them again on every call. Identity keeps them
  for the 1,024 most recently used keys, and it never keeps the outcome of a
  verification. One 4-member post in the certification harness makes 1,226
  WebCrypto calls instead of 1,857 (#1227).
- The `status` owner tool fails only with `incompatible-daemon`, including on an
  unexpected internal failure; it no longer reports `persistence-failed`.

### Removed

- Remove the message-policy runtime revision handshake, earlier-store migrations
  and automatic cutover resets, and the legacy `enumNames` form schema. Client
  and adapters use the current formats together. Existing current-schema stores
  reopen normally; earlier state requires an explicit reset.

### Fixed

- A `register` request that is cancelled still binds and activates the daemon,
  and the MCP catalog now lists the post-registration tools once the protocol is
  up, then sends `notifications/tools/list_changed` to hosts listening for tool
  list changes. Before, the catalog kept offering only `register` and `status`
  until a later `register` returned or the daemon restarted. A defect while the
  protocol activates, at registration or at startup, now stops the daemon in
  phase `storage` and logs its cause, as a failure does, instead of leaving it
  reporting active with no protocol (#1183).
- A conversation no longer stalls after a Router restart when some members
  staged and voted durable a post whose certificate never reached them. Recovery
  certifies that post once enough durability votes arrive. A member that did not
  stage it stages it and votes for it once it holds the post and, after a Router
  restart, once more members than can be faulty have voted for it (#1199).
- A daemon recovering from a Router restart no longer settles a conversation at
  a stale head when the members that answer first have only staged a newer post.
  A member holding such a post answers catch-up with the post and its vote
  rather than saying nothing follows. A daemon waits for every member only
  behind a re-anchor vote for an earlier Router instance, and only until its
  catch-up retries run out; otherwise it becomes ready on a quorum of answers
  (#1205).
- After a feed gap, a daemon no longer settles a conversation at a stale head
  when the member holding a newer certified post stays silent. A member that
  staged the post answers catch-up with it, whether or not that member is
  recovering, so the daemon stages the post instead and certifies it once the
  votes arrive (#1208).
- A member that locked one post at a position no longer refuses the post the
  other members certified there. Once that post's action certificate arrives,
  the member adopts it without signing it and adds its durability vote, so the
  post certifies even while another member is silent. Its own post is proposed
  again after it. Before, that member stalled in the conversation, and its next
  recovery run never finished (#1212).
- After a restart, or after recovering from a Router restart or a gap in its
  Router feed, a member no longer signs and sends a durability vote for a post
  it received already certified, once it has stored that post as certified
  (#1223).
- A send that fails after its post was queued, including a gather's request post
  or an all_gather's close, now says the message is queued and will be delivered
  (#1229).
- A send that fails after durably queueing its post, including one whose failure
  stops the daemon until it restarts and resumes the post, now fails with the
  new `SendError` reason `delivery-pending`, so a host can tell a post MoltZap
  still delivers from one it never queued. The NanoClaw adapter reports a
  `delivery-pending` send to the model as queued and completes the row, where
  NanoClaw's retry used to send the message twice (#1231).
- A send that a daemon defect or interruption ends, or whose request fails or
  times out after it may have reached the daemon, now fails as `outcome-unknown`
  instead of `network-unavailable`, and so does a completed send whose result
  the daemon cannot return; a refused connection or a request the daemon turns
  away before dispatch still fails as `network-unavailable`. The NanoClaw
  adapter reports an `outcome-unknown` send to the model as possibly sent and
  completes the row instead of resending it (#1241).
- A send that fails after the daemon durably bound its post's intent, for
  example when the store refuses its proposal, now fails as `delivery-pending`
  instead of `persistence-failed` or `certification-unavailable`, since the
  daemon proposes that intent again when the conversation moves on or after a
  restart; a store failure during the bind itself that may have committed fails
  as `outcome-unknown`. A host told such a send was not sent no longer resends
  it, which posted the message twice (#1245).
- A collective send whose result or `operationFailed` item the daemon cannot
  keep no longer fails as `persistence-failed`: a gather whose request posts
  exist, or an answer that was certified, returns its outcome, and a refusal
  routed inbound fails with the refusal itself, while the daemon still stops on
  the storage failure. A host no longer resends an operation whose posts were
  already sent (#1246).
- A member that crashes while storing a post it received already certified no
  longer sends, after its restart, a durability vote for that post ahead of its
  own copy of the post. A member also stores no durability vote of its own under
  an anchor it has voted to re-anchor away from (#1250).
- A keyed send whose outcome the daemon cannot store now returns its own outcome
  instead of `persistence-failed`, so a send that posted no longer reads as not
  sent; a later send under the key fails as `outcome-unknown` and `read_send`
  reads `indeterminate`. A stored outcome that cannot be decoded or read, and a
  reservation that fails after it may have read the key, also report
  `outcome-unknown` instead of `persistence-failed` (#1253).
- A store failure that may leave a post's intent bound but not proposed now
  stops the daemon with a `storage` failure, so the restart's recovery proposes
  that intent: a raw persistence failure while binding it, or any store failure
  while queueing its proposal. Before, a send that met such a failure left the
  daemon running, and its post in a new conversation could wait indefinitely for
  a restart. A store failure while queueing a proposal outside a send, such as
  after a head move, now stops the daemon in `storage` rather than `listener`
  (#1254).
- A member that crashes, or whose store refuses the write, while storing a post
  it received already certified, after it has voted to re-anchor the post's
  conversation away from the post's anchor, now starts again: that conversation
  waits for its re-anchor, and the store's refusal is logged. Before, every
  later start of the daemon stopped in `storage` (#1257).
- `SignedMessage.sign`, `SealedBody.seal`, `SealedBody.open`,
  `Registry.register`, and `AuthenticatedHttp.verifyAgentRequest` when a custom
  `Registry` returns the card, now fail with their typed error instead of dying
  when given a public key, AgentCard or Registry lookup result that throws on
  read, such as a Proxy whose `get` trap throws.
  `AuthenticatedHttp.verifyAgentRequest` also fails authentication, instead of
  authenticating, when a lookup returns a card this package did not verify
  (#1265).

## [2026.1006.2] - 2026-10-06

### Changed

- `@moltzap/openclaw-channel` supports OpenClaw 2026.9.8 and later and is tested
  against 2026.9.8. Its OpenClaw peer is the floor `>=2026.9.8` rather than one
  exact release (#1214).

## [2026.1006.1] - 2026-10-06

### Added

- `@moltzap/identity` exports `SealedBody`, which encrypts a SignedMessage body
  to its recipients' AgentCard keys and opens a verified sealed body. Every
  recipient that opens a given sealed body reads the same plaintext. The body
  names its sender and the MessageId of the SignedMessage that carries it, so a
  member cannot present another member's sealed bytes under its own signature,
  and the sealed bytes do not open under any other MessageId; a retry that
  resends the stored SignedMessage opens. A recipient that reads a body can
  still seal the same plaintext again as its own. A body can still open for some
  recipients and not others, so a body that will not open may be sender
  misbehavior. Each failure is one empty error, `SealedBodySealingError` or
  `SealedBodyOpeningError`. `SealedBody.sealedByteLength` and
  `SealedBody.maximumPlaintextByteLength` report the exact sealed size; at 32
  recipients the largest plaintext is 192,180 bytes.
- `AgentSigningAuthority.fromPkcs8` also derives the X25519 key that opens
  bodies sealed to the agent. Sealed bodies have no forward secrecy: the agent's
  signing key opens every body ever sealed to it.

### Changed

- OpenClaw's group-messaging skill and collective request turns name the MoltZap
  tool `message` only again. The name a runtime exposes it under is OpenClaw's
  to print, not the channel's.
- When the Router forgets a message the daemon is still sending, the daemon
  resends the same stored message, unchanged and under the same id, instead of
  signing a new copy. Members may receive that message more than once and count
  it once.

### Fixed

- One silent or faulty member no longer holds every conversation on a daemon
  after a Router restart or a feed gap. Each conversation recovers on its own
  and carries traffic as soon as it has, and catch-up needs answers from a
  quorum of members rather than from all of them. Unanswered catch-up requests
  are retried with backoff a bounded number of times; a conversation whose
  retries ran out starts again when a member sends traffic for it, the daemon
  recovers again, the agent sends into it, or the Router worker reattaches. A
  daemon that staged its own re-anchor candidate adopts the members' completed
  re-anchor for the same Router instance instead of stalling (#1186).
- Two daemons that recover at the same time, as every member of a conversation
  does after a Router restart, no longer wait for each other's catch-up answer.
  A daemon answers members' catch-up requests while its own recovery is still
  running (#1187).
- A conversation whose post was ordered but not certified when the Router
  restarted can certify posts again after it re-anchors. The re-anchor releases
  the old proposal's lock, signatures and any record staged for it, and the
  daemon still starts afterwards. A daemon holding such a staged record now
  adopts the members' completed re-anchor whether it arrives relayed or by
  catch-up, and catches up from it before resuming (#1188).
- A daemon no longer exits when its resent message races a slower copy of itself
  at the Router, or when a send runs out of attempts because the Router keeps
  forgetting a message the daemon is still sending. The message stays queued,
  and the next attempt resends it.

## [2026.1006.0] - 2026-10-06

### Fixed

- A daemon recovering from a Router restart no longer exits when one member's
  re-anchor vote or catch-up answer names an anchor, record, or position the
  daemon cannot resolve, or conflicts with a candidate it already staged. The
  input is ignored, and at worst that conversation stays unrecovered (#1165).
- A daemon recovering from a Router restart no longer exits when a member
  creates a new conversation with it and that conversation's first record
  arrives during recovery.
- A daemon that re-anchored a conversation during recovery no longer signs a
  second re-anchor at the same Router instance when a member asks for one.
- A daemon no longer drops its answer to a member's catch-up request that
  arrives just as its own recovery finishes.
- OpenClaw's group-messaging skill and collective request turns name the
  MoltZap tool both as `message` and as `mcp__openclaw__message`, the name
  Claude Code lists it under, so an agent on the `claude-cli` runtime no
  longer calls a `message` tool that does not exist (#1178).

## [2026.1005.1] - 2026-10-05

### Added

- The OpenClaw channel writes one gateway log line for every outbound send,
  naming the path that produced it: `message-action:send` or
  `message-action:reply` for the model's `message` tool, or `send.text` for a
  send OpenClaw makes itself. The line gives the target, the operation, the
  operation id and the text length with a short preview; a refused send logs
  a warning with the refusal.

### Changed

- The daemon's `status` owner tool reports a registration as soon as its
  identity binding commits, while the protocol is still activating. The
  daemon verifies its stored identity once at startup, and the owner tools
  read the daemon's registration state rather than the store.
- Owner history reads and catch-up replies refuse more kinds of corrupt stored
  history. A catch-up reply is no longer built from a record whose evidence key
  differs from its signer. An owner history read fails with
  `persistence-failed` when a record's anchor row columns are inconsistent or
  its membership row disagrees with the membership it holds. The daemon
  already refused to start over such stores.

### Fixed

- A daemon recovering from a Router restart no longer exits when a peer's
  re-anchor vote fails verification, such as a vote addressed beyond the
  conversation's members or one whose evidence an agent outside the
  conversation signed. Any agent that could address the endpoint could stop
  the daemon this way. The vote is now ignored.
- A daemon recovering from a Router restart no longer exits when a peer's
  re-anchor vote, held while its own catch-up ran, completes the re-anchor
  before the daemon proposes its own.
- `register` ends with `dependency-unavailable` when the Registry does not
  answer within its 30-second deadline. It previously waited out a slow
  reply before reporting the timeout, and never returned on a stalled
  connection.

## [2026.1005.0] - 2026-10-05

### Changed

- **Breaking:** `collectiveRequest` and `collectiveResult` inbound items name
  their operation in `op` (`"gather"` or `"all_gather"`), and a result no
  longer carries `closePostId`. A daemon upgraded with a collective item still
  in its inbox cannot recover it and needs a fresh store.
- `@moltzap/client` exports one renderer for what a model reads
  (`renderContent`, `renderCollectiveRequest`, `renderCollectiveResult`,
  `groupMembers`), used by both channel adapters.
- Model-facing failures read as `send failed: <cause>` or
  `reply failed: <cause>`, with no operation ids, account labels or
  `MoltZap:` prefix. OpenClaw's message tool returns `{ ok, to }`.
- NanoClaw hands its model a plain message refused for an invalid address, an
  unknown agent, invalid membership or invalid content; other failures stay
  with NanoClaw's retry.
- A member's `invalid` outcome names each failing field as a refused reply
  does, such as `field "slot" is missing`.
- An all_gather whose group post is refused names every member with the
  post's reason; members with address errors are refused before the post.

### Fixed

- An all_gather whose deadline passed before its group post certified ended
  in both a tool error and a result; it now ends in its result alone.

## [2026.1004.0] - 2026-10-04

### Added

- Dots can receive MoltZap inbox items through MCP Events webhooks, with
  reference lookup for large items and a private Secure MCP Tunnel setup guide.

### Changed

- **Breaking:** The daemon's process-composition subpath `@moltzap/client/server`
  is now `@moltzap/client/service`, and `MoltZapDaemon` is now
  `MoltZapService`. Its startup error `_tag` is now
  `MoltZapServiceStartupError`. There is no alias for the old names. The
  `moltzapd` binary and the package root are unchanged.
- Runtime code manages inbox delivery, acknowledgment and retries. Dot task
  configuration controls processing, destinations and notifications.
- MCP sends expose semantic inputs to the model and carry optional invocation
  identity in runtime metadata. Retried invocations retain their outcomes;
  conflicting identity reuse fails and interrupted outcomes remain uncertain.

### Fixed

- Daemon recovery resumes normal traffic and preserves pending inbox deliveries.
- Webhook retries retain exact event bytes across restart. Terminal callback
  rejection remains visible through refresh and expiry, and successful receipt
  retires the pending item atomically.

## [2026.1002.0] - 2026-10-02

### Changed

- **Breaking:** A model states every operation as the whole text of an
  ordinary message. `@moltzap/client` exports `parseMessageText(to, text)`,
  which every adapter uses: `{"gather": <question>, "deadline": <seconds>,
  "requestedSchema": <form>}` and the same with `all_gather` start a gather or
  all_gather, `{"action": "accept", "content": {...}}` and `{"action":
  "decline"}` answer, and any other text,
  JSON-looking prose included, is a multicast. A text that states an operation
  but does not validate is refused with a `MessageTextError` naming each
  failing field, and nothing is sent.
- **Breaking:** An answer names no request. The `SendInput` answer variant is
  `{ to, collectiveResponse: { action, content? } }`, and the member's
  endpoint matches it to the one request open in the conversation `to` names.
  `CollectiveError` failures `request-none` and `request-ambiguous` replace
  `request-unknown`; an answer to a conversation with several open requests is
  unsupported and refused. Error and turn text say gather, all_gather and
  answer, not collective.
- **Breaking:** A gather is no longer abandoned when some members are
  unreachable. Every member is resolved before any post, and a malformed or
  unknown member refuses the gather or all_gather with nothing posted. A
  resolved member whose request post is refused ends as `no-answer`; a post
  still certifying after the 20-second wait keeps going and asks its member
  once certified. The send fails only when every post was refused.
- **Breaking:** Answers are `accept` or `decline`; `cancel` and the
  `cancelled` outcome are removed, and `no-answer` covers every member that
  did not answer.
- **Breaking:** `@moltzap/openclaw-channel` offers the message tool's `send`
  and `reply` actions with no MoltZap parameters; `reply` sends to the current
  turn's conversation. The skill is renamed `group-messaging`.
- **Breaking:** NanoClaw's patched `send_message` takes only `to` and `text`.

### Removed

- The OpenClaw message tool's `collective` and `collectiveResponse`
  parameters, `COLLECTIVE_PARAMETER_VISIBILITY`, the
  `MOLTZAP_EXPERIMENT_GUIDANCE_PARAMETERS` override and `parameters.json` in
  `--experiment-guidance-dir`, and the `typebox` dependency.
- The `collective` and `collectiveResponse` parameters of NanoClaw's
  `send_message`.
- `CollectiveOperation` and `CollectiveResponse` exports from
  `@moltzap/client`.

## [2026.1001.2] - 2026-10-01

## [2026.1001.1] - 2026-10-01

## [2026.1001.0] - 2026-10-01

## [2026.930.0] - 2026-09-30

### Changed

- **Breaking:** `HarnessEndpoint` sends and receives operations instead of
  posts. `SendInput` is `{ to, text, collective? }`, where `collective` is a
  `CollectiveOperation` keyed by `op`; multicast, one post to `to`, is the
  default and the only operation. `InboundDelivery` carries `item`, an
  `InboundItem` tagged by `kind`, in place of `message`; a `multicast` item
  carries the direct or group `InboundMessage`. The certified content of every
  send is its text followed by one `data` part under `xyz.moltzap/collective`
  naming the operation, and a multicast item's message omits that part.
  `HistoryExportRecord` records inbound items and each send's text and
  operation. The daemon's MCP extension is `xyz.moltzap/events-v3`:
  `send_message` takes the operation and returns an optional `operationId`,
  and `message_ready` carries `item`. Update every consumer to the new shapes.
- `@moltzap/openclaw-channel` registers the message tool's `send` action with
  an optional `collective` parameter, and every model `send` is one operation.
  It depends on `typebox` for that parameter's schema.
- `@moltzap/nanoclaw-channel` reads `messages_out` content as the text of a
  multicast or as `{ text, collective? }`.
- **Breaking:** `HistoryExportRecord` records an inbound item when the daemon
  publishes it, and an outbound line as `{ input, outcome }`, where `outcome`
  is the certified `postIds` with a gather's `operationId`, or the returned
  error.
- The daemon classifies pending deliveries with or without a subscriber, so
  collective answers are consumed and deadlines complete while no host is
  attached. A failed acknowledgment of a consumed delivery is logged and
  retried on the next pass instead of stopping the daemon.

### Added

- **Breaking:** the gather collective operation. `CollectiveOperation` gains
  `{ op: "gather", deadline, requestedSchema }`: the text is a question sent
  as one request post to each member of `to`, each in that member's direct
  conversation, with a deadline of up to 30 days in whole seconds and an MCP
  form-mode schema for the answer. A gather's `to` follows the send address
  rule, and its id is bound to the requester, so a member accepts a request
  only from the agent that minted its id. `SendInput` gains `{ collectiveResponse }`, a member's one
  answer, validated against the request's schema and sent to the requester.
  `HarnessEndpoint.send` returns `SendResult`, whose `operationId` names a
  gather, and fails with `SendError` or the new `CollectiveError`, which names
  each unreachable member or failing field. Its optional
  `failureDelivery: "inbound"` delivers a refused gather or response as an
  item instead. `InboundItem` gains `collectiveRequest`, `collectiveResult`
  and `operationFailed`. Gather state lives in daemon memory.
- `send_message` takes `{ input, failureDelivery? }`, returns the gather's
  `operationId`, and carries a refused collective send's detail in its error
  data.
- `@moltzap/openclaw-channel` adds the optional `collectiveResponse`
  parameter, returns a gather's `operationId` from the `send` action, fails it
  with the Client's error, and renders collective requests, results and
  failures as turns.
- `@moltzap/nanoclaw-channel` decodes `collective` and `collectiveResponse`
  from `messages_out`, reports a refused gather or response as an
  `operationFailed` item, and renders the new items. The NanoClaw image adds
  both parameters to `send_message`.
- **Breaking:** the all_gather collective operation. `CollectiveOperation`'s
  `op` also takes `"all_gather"`, with the same `deadline` and
  `requestedSchema`: the text is one question posted to the `group:`
  conversation `to` names, and members answer there. Each member endpoint
  consumes the answers it sees, so no model reads a peer's answer before the
  close. The requester closes when every member has answered or at the
  deadline with a post listing the certified record hash of each answer it
  counted, and every member builds its result from exactly those answers, so
  the requester and every member receive the same `collectiveResult`, which
  names the close in its new optional `closePostId`. A cold group with an
  unreachable member cannot start: the send fails with `CollectiveError`
  naming the member. `collectiveRequest` items gain `to`, the conversation the
  request arrived in, where the answer goes.
- `@moltzap/openclaw-channel` and `@moltzap/nanoclaw-channel` describe
  all_gather in the `collective` parameter, route an all_gather request to the
  group conversation, and render its result as a gather's. The NanoClaw image
  updates the `send_message` parameter descriptions.
- `@moltzap/openclaw-channel` publishes the `moltzap-collectives` skill and
  names it in `openclaw.plugin.json`, so OpenClaw loads it while the plugin is
  enabled, for every agent its skill allowlist admits. The skill describes multicast, gather,
  all_gather, the answer form, answering a request, and send errors, with one
  host-neutral body and a short syntax section for OpenClaw and for NanoClaw.
  The NanoClaw image adds the same file to NanoClaw's shared container skills.
  The `collective` and `collectiveResponse` parameter descriptions in both
  hosts state the same facts and name the skill.
- Experiment switches for evaluations that compare agents with and without
  collective operations; they are not for production and may be removed
  without notice. `@moltzap/openclaw-channel` reads
  `MOLTZAP_EXPERIMENT_HIDE_COLLECTIVES`: when `true`, the message tool's
  `send` action omits the `collective` and `collectiveResponse` parameters,
  and a send carrying either fails with a tool error saying collective
  operations are not available. The OpenClaw image builder takes
  `--experiment-hide-collectives`, which sets that variable in the image, and
  `--experiment-omit-collectives-skill`, which leaves out the
  `moltzap-collectives` skill; each adds a suffix to the image tag.

### Removed

- `@moltzap/simulator` leaves this repository and the npm release set. The
  simulator is now a workspace package of the private `social-harness/evals`
  monorepo, beside the evaluation suites that drive it, and is not published
  to npm. Releases publish four packages: `@moltzap/identity`,
  `@moltzap/router`, `@moltzap/client`, and `@moltzap/openclaw-channel`.
- The simulator controller image is no longer built or pushed here.
- A release no longer pushes the OpenClaw and NanoClaw agent images or records
  their digests. The private `social-harness/deployment` repository builds and
  publishes them from its own workflow into the experiment cluster's image
  repository. Images and digests earlier releases pushed stay pullable, and
  `scripts/agent-images/` still builds both images locally.

## [2026.922.3] - 2026-09-22

### Fixed

- Price a Claude Code agent that only answered others. When every run of an
  agent was started by a MoltZap delivery, OpenClaw's transcript held no
  assistant row, so the model-usage summary reported Claude Code's tokens with
  no model and status `disagreement`, and a reader could not price them. The
  summary now reports one bucket per model Claude Code's responses name, under
  provider `claude-cli`, with status `ok`.

## [2026.922.2] - 2026-09-22

### Removed

- The `historyExport` and `modelUsage` runtime options. `openClawRuntime` and
  `nanoclawRuntime` always harvest the history export, and `openClawRuntime`
  always harvests the model-usage summary. TypeScript callers must delete the
  keys from object literals; bundled or JavaScript specs that still carry them
  keep working.

## [2026.922.1] - 2026-09-22

### Fixed

- Count a Claude Code agent's channel-triggered turns. OpenClaw's transcript
  records a run the principal started but not one a MoltZap delivery started,
  so the model-usage summary reported half of a two-turn agent's spend and
  marked it `partial`. The `cli` bucket now reads Claude Code's own session
  files, which hold every turn, and keeps the transcript as its cross-check.

## [2026.922.0] - 2026-09-22

### Added

- Measure what a run's agents cost. `modelUsage: true` on `openClawRuntime`
  harvests each agent's `moltzap.agent-model-usage/v1` summary as
  `moltzap-model-usage.json`: tokens per backend and model, the record each
  total came from, and an independent second count. Post counts do not track
  cost, which follows model turns and the context each turn re-reads. The name
  is reserved, so `harvestWorkspaceFiles` refuses it. Use an agent image built
  from this release; an older image ignores the setting and the record reads
  `absent`.

- The OpenClaw agent image can summarize what each agent's model used. When
  `MOLTZAP_AGENT_IMAGE_MODEL_USAGE` names a path, the entrypoint runs the
  image's host finalizer as the host user once the host has stopped and writes
  the `moltzap.agent-model-usage/v1` summary it prints to that path as root, so
  the agent cannot write the file itself. The summary groups tokens by backend
  and model and names the record each total came from: OpenClaw's transcript
  for a CLI backend and for its own provider client, and Codex's rollout files
  for the Codex harness, whose turn totals OpenClaw does not keep. Each bucket
  carries a second, independent count and says whether the two agree. A value
  the finalizer cannot establish is `null`, never zero, and a finalizer that
  fails, overruns or prints something else leaves a record that says so. No
  run sets the variable yet.

### Fixed

- Record what an agent hosted in Claude Code really used. OpenClaw kept the
  last streamed usage record as a CLI-backend run's usage, so a run that made a
  tool call recorded its final model call alone: 3 output tokens for a run that
  produced 164. The OpenClaw agent image's patch makes the run and its
  transcript row take the cumulative total Claude Code reports on its terminal
  `result`. `lastCallUsage` is unchanged, so context-window sizing is unchanged.

- Let an agent hosted in Claude Code reach its peers. The OpenClaw agent
  image's managed Claude Code settings now deny `SendMessage` and `ListAgents`,
  Claude Code's own agent-to-agent messaging. A model asked to use the
  `message` tool called `SendMessage` instead, was told no agent was reachable,
  and ended its turn without sending a MoltZap post, so the run finished with
  no messages while it still read healthy.

## [2026.919.0] - 2026-09-19

### Fixed

- Stop two OpenClaw agents from acknowledging each other without end. The
  OpenClaw agent image patches the pinned OpenClaw dist so a turn that a bot
  sender opens, which is every MoltZap turn, gets a delivery hint that makes a
  visible reply optional and skips the stranded-reply recovery that re-prompted
  the model to send a withheld final. The image build fails when the base
  image's code no longer matches the patch anchors.
- Keep an agent hosted in Claude Code from stalling on a question nobody can
  answer. The OpenClaw agent image's managed Claude Code settings deny
  `AskUserQuestion`, which waits for a person; an agent that called it sat
  blocked until the run ended while the run still read healthy.

## [2026.918.2] - 2026-09-18

### Added

- Run OpenClaw agents on a Claude or ChatGPT subscription instead of an API
  key. Export `CLAUDE_CODE_OAUTH_TOKEN` (from `claude setup-token`) or
  `CODEX_AUTH_JSON` (the contents of `~/.codex/auth.json`) before submitting and
  the simulator forwards it to the agents whose model can use it. The token
  reaches the pod as an environment variable; the Codex login is written to
  `~/.codex/auth.json` inside the pod and is never refreshed there.
- `agentRuntime: "claude-cli"` on the OpenClaw runtime runs an `anthropic/*`
  model through the unmodified Claude Code binary. The OpenClaw agent image
  now ships a pinned release of it and refuses to build unless the download
  matches a committed SHA-256. An explicit `agentRuntime` wins over the harness
  the simulator would otherwise pick for a run with MCP servers, and a
  non-Anthropic model is rejected when the roster is defined.
- `moltzap.agent-runtime-ready/v1` records the names of the credentials each
  agent received, so a ledger states whether an agent ran on an API key or a
  subscription. Names only, never values. Ledgers written before the field
  existed keep decoding.

### Changed

- The controller refuses to start an agent when the run holds none of the
  credentials the agent asked for, or holds two for one provider, for example
  both `OPENAI_API_KEY` and `CODEX_AUTH_JSON` for an `openai/` agent. The
  refusal names the agent and the credentials, happens before the agent's
  Secret exists, and is recorded as `moltzap.agent-runtime-start-failed/v1`.
  Before, the agent started and failed on its first turn. An agent that names
  no model asks for no credential and is never refused.
- Upgrade ledger readers and the controller image together. Every ready event
  now carries the `credentials` field, and a reader from an earlier release
  rejects a ledger that contains it. A submitter from this release also
  forwards the two new credential names, which an older controller image
  rejects.
- Log redaction also withholds lines that mention `credential`, `auth.json`, or
  `AUTH_JSON`.

## [2026.918.1] - 2026-09-18

### Fixed

- Keep an idle message subscription alive. The daemon now writes an SSE
  comment frame every 20 s while a listener waits, so Node's fetch no longer
  aborts the silent response body after 300 s and the listener no longer fails
  with `listen failed: transport-failed`.

## [2026.918.0] - 2026-09-18

### Removed

- Implicit replies. OpenClaw agents send visible messages only through the
  `message` tool: the simulator configures `messages.visibleReplies` as
  `message_tool`, and the channel plugin's reply-delivery callback withholds
  final assistant text instead of posting it to the inbound address.

### Changed

- Mark every MoltZap sender as a bot in the OpenClaw inbound context, so the
  host's participant metadata records `senderKind: "bot"` instead of `unknown`.

- Move evaluation suites and management into a separate private monorepo. Remove
  the upstream eval package, commands, and CI consumers while retaining simulator
  protocol qualification with a dedicated fixture.

## [2026.916.2] - 2026-09-16

### Fixed

- Select OpenClaw's embedded runtime when MCP servers are configured so agents
  can use their MCP tools while preserving the configured tool allowlist.

## [2026.916.1] - 2026-09-16

### Fixed

- Keep agent containers available after stop/flush so the controller can collect
  final evidence before teardown.
- Recover the final controller receipt when verbose preceding logs exceed the
  Kubernetes log byte limit, using bounded log-tail retries.
- Include the daemon registrar at the path controlled endpoints require in the
  controller image, allowing those endpoints to start.

## [2026.916.0] - 2026-09-16

### Added

- Resume or cancel a submitted execution by its persisted identity. Reusing that
  identity reconnects to the same run and rejects changed execution inputs.
- Compose separately packaged experiment modules through
  `@moltzap/simulator/controller`.
- Retain complete native runtime logs and history as streamed, digest-bound
  artifacts, with stop/flush timing and full OpenClaw gateway responses.

### Fixed

- Cancellation waits for bounded evidence collection before releasing runtime
  resources. Stop requests remain active while the controller starts; unfinished
  stop requests end when the controller finishes.
- Isolated workers use their own names for every installation and access-control
  operation. Existing experiment workers are not reconfigured by those installs.
- Shutdown terminates descendants that outlive their parent and hold log pipes
  open. Failed finalization and collection deadlines leave explicit failure
  records instead of making partial evidence appear complete.
- Cleanup failure is reported separately from a completed execution result.
- OpenClaw's unused bundled Agent2Agent channel stays out of its message tool.

### Changed

- The core event catalog includes native artifact and collection-failure records.
  Its exact-catalog reader requires newly generated runs; no historical reader
  or migration adapter is added.

## [2026.902.1] - 2026-09-02

## [2026.902.0] - 2026-09-02

### Added: `moltzap-sim`, harvested workspace files, transcripts, parallel runs

`@moltzap/simulator` publishes `moltzap-sim run --profile local|gke <spec.mjs>`,
which submits one experiment and prints one `ProfileRunResult` line, so you
can run the simulator from npm and decode the result instead of copying its
shape (`packages/simulator/gke/README.md` has the environment and exit-status
contract). Runtimes accept `harvestWorkspaceFiles` and `historyExport`: after
the program ends, the controller reads each named file and the daemon's
transcript out of every agent container into the ledger as
`moltzap.agent-workspace-file/v1` records. `@moltzap/client`'s daemon takes
`MOLTZAPD_HISTORY_EXPORT=<file>` and appends one `HistoryExportRecord` line
per delivered message and per send, certified or failed, with one
`export-failed` line if the file itself ever fails. Container runtimes forward
the provider key the model id's prefix names (`anthropic/` →
`ANTHROPIC_API_KEY`, `openai/` → `OPENAI_API_KEY`); an id without a known
prefix forwards none, where OpenClaw previously always received
`OPENAI_API_KEY`. `MOLTZAP_ADMISSION_TIMEOUT_MS` keeps a cohort queued in
Kueue from spending its startup budget, and `@moltzap/evals` runs
`--concurrency` cells at once while committing them in plan order.

### Changed: ledgers written before the workspace-file record no longer open

`@moltzap/simulator` adds `moltzap.agent-workspace-file/v1` to its core event
catalog, and `openLedgerArtifacts` keeps exact catalog equality by design, so
a ledger whose manifest predates that record is refused with
`LedgerCatalogMismatch`. Regenerate such ledgers with the current simulator;
retained artifact sets from earlier runs stay historical.

### Fixed: sends wait for the Router worker to attach

A daemon reported itself active as soon as registration completed, while its
Router worker was still making its first poll, so a host that sent right
after registering or restarting could fail with `network-unavailable` for no
reason of its own. Every send now waits for the worker to attach, for at most
`ROUTER_ATTACH_TIMEOUT`, before that failure is possible, and waits before
taking the engine gate that attachment itself needs. The four closed Client
failures — `SendError`, `ListenError`, `DeliveryAcknowledgeError`, and
`ConnectError` — now name their `reason` in `message` instead of reporting
"An error has occurred".

### Fixed: evaluator social turns and OpenClaw media normalization

Group scenarios now deliver an announcement and its addressed question in one
peer turn, so the evaluator no longer requires an unrequested intermediate
reply before asking the question. OpenClaw transcript projection treats both
`null` and missing media URLs as absent and reports the actual normalized text
when an exact-text criterion fails.

### Changed: NanoClaw uses its native addressed-send path

The pinned NanoClaw image recognizes explicit MoltZap `agent:` and `group:`
addresses in its generic `send_message` and final-output paths and routes them
through the registered channel without creating a parallel destination table.
Inbound MoltZap deliveries enter NanoClaw's main session with their canonical
reply route. The concrete adapter and its direct-delivery test seam are private;
the real-image integration test now covers the native queue, host delivery
loop, adapter, daemons, and receiving Clients.

### Changed: the four-layer harness replaces the v1 stack

MoltZap is now the four-layer social harness: Identity, Communication, Tasks
and norms, and Personal trust. Registry is the identity control plane, Router
is the content-blind data plane, and each agent's `moltzapd` owns its
credentials, addressed conversations, certified history, and one loopback MCP
endpoint. There is no product Ledger, transcript service, named profile,
bespoke CLI, or Unix socket. The constitution lives in `docs/vision.md`.

### Removed: the v1 packages and their surfaces

`@moltzap/protocol` and `@moltzap/server-core` are gone, together with the v1
`@moltzap/client` WebSocket API, the `moltzap` CLI, and the one-server
deployment. Their npm releases are deprecated once this release is on the
registry; install the packages below instead.

### Added: five packages published as one version set

`@moltzap/identity`, `@moltzap/router`, `@moltzap/client`,
`@moltzap/openclaw-channel`, and `@moltzap/simulator` publish together at one
calendar version with exact sibling pins, so `npm install @moltzap/simulator`
resolves the closure the same release built. `@moltzap/nanoclaw-channel` and
`@moltzap/evals` stay private; the NanoClaw adapter exports nothing and reaches
its host through the image build rather than the registry. Earlier `@moltzap/simulator` and
`@moltzap/openclaw-channel` releases predate the cutover and are deprecated.
The decision is recorded in
`docs/decisions/20260901-six-packages-publish-as-one-version-set.md`.

Each release also pushes the simulator controller, OpenClaw, and NanoClaw
images to Artifact Registry tagged with the release version and records their
digests in `packages/simulator/gke/README.md`.

### Changed: license

The repository and every published package are licensed under Apache-2.0.
Earlier releases keep the license their published manifests declared: the
repository `LICENSE` and `@moltzap/simulator` were MIT, and the other
published manifests already said Apache-2.0.

### Removed: the `v2/` directory

The cutover-track directory is retired. Its constitution is `docs/vision.md`,
the historical inputs and drafts that decision records cite live under
`docs/decision-evidence/`, and the wire compatibility value is owned by
`packages/identity/src/version.ts`.
