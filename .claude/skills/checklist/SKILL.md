---
name: checklist
description: The project checklist for Social Harness and MoltZap. Read it before you plan, design, write, review or land any change in moltzap, deployment or evals.
---

# Project checklist

Every change in moltzap, deployment and evals meets these rules. A
repository's `AGENTS.md` adds only what is specific to that repository; when
the two disagree, stop and report the conflict. The project lead's reasons and
words behind each rule are in docs-internal's
[decisions audit](https://github.com/social-harness/docs-internal/blob/main/workflow/evidence/2026-10-26-owner-architecture-decisions.md).

**Terms.** Social Harness is a stack of layers, as the paper *Agentic
Societies Need a Social Harness* describes them: identities (L1), reliable
ordered communication (L2), each agent's personal firewall (L3), norms that
agents agree to (L4), and institutions and applications built on them (L5).
An agent runs inside a *host*, a program such as OpenClaw or NanoClaw, which
reaches the stack through a local *daemon*. The *firewall* checks every message
an agent sends or receives against its policies. An agent's *owner* is the
person it acts for.

## Where the system is heading

The project lead's design doc, "System Architecture: Social Harness Frontdoor
for the Internet", is the goal plans and docs serve (audit rule 11.5):

- An application defines which agents may join, the norms for each role, the
  actions an agent may take without asking its owner, and how a member is
  removed.
- A user's agent joins an application from one advertised prompt, after the
  owner signs in to prove who they are.
- Every agent's daemon checks each message, inbound and outbound. It approves
  the message, approves it with guidance for the agent, holds it for the
  owner, or blocks it.

## Before you design

- [ ] Find what already does the job: a dependency, the agent host, the
  platform, another layer or a sibling repository. Use or configure it. Do not
  re-implement another system or wrap it in waits, retries, preflights or
  gates, and name in the PR the existing options you rejected for any new
  machinery.
- [ ] One owner per value, one path per problem. When two systems set the
  same value, or two mechanisms solve the same problem, keep the one that fits
  CUPID best and delete the other. Never add a third to reconcile them.
- [ ] Check each property once, at the boundary that owns it. A check that
  repeats another layer's is deleted, not kept as defense in depth.
- [ ] Every plan has an Architecture section: the constraints and the one that
  decides the design, the full target file tree, exact placement and names,
  the direction of dependencies, the contracts and entry points, and one line
  per module checking it against
  [CUPID](https://dannorth.net/cupid-for-joyful-coding/). Review it with
  gstack's `/plan-eng-review` before building.
- [ ] Design the interfaces application builders use from several real
  applications: office hours, GPU sharing, a social deduction game and a
  buy-nothing marketplace. Build each application's own logic as another agent
  with its norms and firewall.

## Where code goes

- [ ] Code lives with its user. Code that one repository uses lives in that
  repository; code that several use lives with the team that owns it. Delete
  code that nothing uses.
- [ ] Public never depends on private. A public repository clones, builds,
  tests and runs without access to private ones. Teammates gain capability
  through credentials and cluster access, never through a build dependency.
- [ ] Keep few top-level folders, each named for its responsibility. Add a
  layer's folder with its first code. Use the `software-architecture` skill
  to organize modules within it. A package's folder carries the package's
  name, and a rename moves both.
- [ ] Dependencies point down. A lower layer never imports a higher one; when
  it needs something from above, it declares an interface and the higher layer
  implements it. Parts are wired together only where a process starts.
- [ ] Depend only on what a lower layer's public specification states. If you
  need more, change that specification first. A new layer on top adds its own
  small interface and leaves the ones below it unchanged.
- [ ] A network service's API and client live with the layer whose job the
  service does, and its server is a separate package. Shared code gives the
  server whole operations, such as "verify this request", rather than steps.
- [ ] Each daemon holds its agent's state and keys. A relay sees only what
  routing needs, and no central service holds members' conversations or
  secrets.

## Interfaces

Read the [safer-by-default principles](https://github.com/chughtapan/safer-by-default/blob/5b86e278e87fa3515266f1bc819828c42b21ed59/PRINCIPLES.md)
and the architecture checker's
[CUPID enforcement](https://github.com/chughtapan/safer-architecture-lsp/blob/f784083f2297ebe107d644a0b6d842040ab70e0a/docs/cupid.md)
and [rules](https://github.com/chughtapan/safer-architecture-lsp/blob/f784083f2297ebe107d644a0b6d842040ab70e0a/docs/rules.md)
directly; these items do not restate them.

- [ ] Close only what you own. A closed union and an exhaustive switch fit a
  set the system owns: agent states, errors, lifecycle phases, the objects it
  creates. A set that grows with the outside world (provider names, credential
  names, model ids, harness runtimes, a third party's configuration) is a
  string or a record from the caller.
- [ ] Validate a value where your code consumes it, against the consuming
  system's rule (a Kubernetes environment-variable name, a DNS label), and pass
  everything else through to its owner, surfacing the owner's error. Our usage
  policy for another system, such as which provider to use, goes in docs,
  never in code that narrows that system's interface.
- [ ] Put each setting with the team that routinely changes it: a routine
  change owned by one team lands in one repository with one review.
- [ ] Publish as little as possible. Before publishing code for another
  repository, have each side build its own artifact and connect them by a
  stable reference, such as an image digest. New packages start private.
- [ ] Name the reader that acts on each new field, state, limit or tool; with
  none, leave it out. Terms only an agreement's parties read stay in its
  plain-English text.
- [ ] One owning module and one parser per wire contract, called by both
  sides; readers reject fields they do not know. Only interfaces designed to
  grow, such as the results an agent reads from the daemon's tools, take
  additions at any time. Every other change an existing reader would reject,
  and any change to signed data, waits for the planned breaking release.
  Encode an invariant in a type only the right parser produces. Never parse
  another system's configuration strictly.
- [ ] Before defining a contract between repositories, look for the design
  that needs none. A container image shares only its documented settings.
- [ ] Design for intended use. A consumer that imports something internal
  moves to the public API; delete unused public interfaces before publishing
  them; make what a sender produces correct rather than the receiver lenient;
  word tools and instructions so they work on any chat channel.
- [ ] Name interfaces, records, branches and files for what they hold or
  deliver, never for a phase. Use the short everyday word, the ecosystem's own
  field names, and error names that state what the caller observes. New
  things use the Social Harness name.

## Firewall, norms and applications

- [ ] A rule an agent must follow is a firewall policy, checked outside the
  agent on every message by fixed code, by the firewall's decision service
  reading a plain-English rule, or by the owner. Agent instructions carry only
  what the agent needs to do its work. When a firewall lacks a rule, add a
  policy.
- [ ] The firewall checks a request before the agent acts on it and a reply
  before it leaves. It does not inspect tool calls or change the host.
- [ ] Who is asking comes from the message's verified signature, never from
  what the message claims.
- [ ] When the owner must approve something, the firewall holds it and the
  agent asks through the host's own ask-the-user feature. There is no second
  approval mechanism.
- [ ] Stop a leak with a policy on the messages that leave. Keep the
  capability legitimate use needs.
- [ ] Agreements between agents and services are ordinary signed messages
  plus firewall policies. A norm is a plain-English rule the parties agree to,
  and each adds it to its own firewall.
- [ ] A service agent, such as the membership concierge, is defined by its
  instructions, files and tools. Specify what the system provides, and leave
  how builders structure their agents to them. A workflow that changes often,
  such as sign-up, is agent instructions plus tools.

## Hosts and other systems

- [ ] Use only a host's documented configuration and extension points, never
  its internal behaviour or a patch; existing patches are debt to remove. Use
  the host's own features and default names first.
- [ ] Agent skills, the instruction files an agent reads, ship with the
  library (`@moltzap/social-harness`). Each channel plugin, the package that
  connects one host to the stack, takes them from there.
- [ ] Let the container orchestrator provide isolation, start order and
  helper processes, and the runtime library retries and in-flight work. Read a
  host's usage from its standard telemetry export. Use a standard tool (a
  dependency bot, a documentation generator, a host's own installer) instead
  of keeping our own copy of one.

## One source per fact

- [ ] Keep one copy of each rule, fact, setting, version and parser, and one
  builder per artifact; derive the rest. When a change meets a second copy,
  remove it in that change instead of editing both. A check whose only job is
  to keep two of our own copies in step goes with one of them.
- [ ] Each documented fact has one home, written for that document's reader;
  other documents link to it.
- [ ] Identifiers the system issues stay out of the repository. Member ids,
  agent keys, AgentIds, and invite and operation ids never appear in
  manifests, settings, docs or tests, because they change on every reset or
  re-enrolment. Configuration names a role, such as the demo's owner, and the
  running system looks up who holds it. Names we choose, such as a platform
  agent's name, are configuration.

## Release and rollout

- [ ] Each package and image is versioned and published only when its own
  contents change. Publish to a package registry only what users run next to
  their own agent, and ship services as signed container images.
- [ ] Our agents and outside users upgrade the same way, through the same
  public updater. Evaluation runs pin exact versions.
- [ ] Collect incompatible changes to the wire format, stored data and names
  into one planned breaking release, and never loosen the compatibility rules
  to ship one early. Until the first public release, keep no
  backward-compatibility code, and delete the old path when the new one lands.
- [ ] A rollout applies the change and waits only for that change to become
  ready, with no preflight checks, no restore to a previous version, and no
  waits or retries for other systems. Automate every rollout that can be.
  Deploy the images built from the same commit as the configuration; record
  versions in git only for artifacts built elsewhere, and let a bot move them.
- [ ] Public end-to-end tests run on the CI provider's standard runners
  without secrets, on merges to main, on a schedule or by hand; private
  end-to-end suites run by hand. No nightly suites and no runner fleets of our
  own. Anything that accumulates has a retention policy.

## Tests and evaluations

- [ ] Before touching a test file, load `google-swe-testing` and apply it to
  every test in that file.
- [ ] A test checks behaviour through public interfaces and breaks only when
  behaviour changes. A test that pins implementation (a dependency's private
  modules or dist layout, exact manifest, Dockerfile or prompt text, version
  numbers, call sequences) is deleted, not fixed.
- [ ] A test asserts what its own repository owns: another repository's
  defaults, rendered output or wording only where they are part of this
  repository's public contract. When a dependency or vendor bump forces a test
  edit, the test is wrong.
- [ ] Integration tests use real systems, and fakes pass the same contract
  tests as the real thing.
- [ ] Evaluations state what should happen and keep safety cases in their own
  group. Every setup in a comparison gets the same model and tools, and attacks
  target the property claimed to be better, including availability and cost.
- [ ] Every build, type-check, lint and test run goes through the build
  system's targets (Nx), in your own commands and in the steps you hand to
  another agent; a test set with no target gets one.

## Docs and records

- [ ] A decision record (ADR) states a few stable rules and why. Layouts,
  lists, versions and mechanisms live in plans, code and public
  specifications; product flows get plans. Replace a record by superseding it.
- [ ] Specify interfaces before implementation plans. Vision documents state
  goals, required properties and the pass/fail checks that show them; the
  plan names the mechanisms.
- [ ] Agree with the project lead on what each reader must come away knowing
  before outlining pages, and do not batch docs by release. Organize public
  docs by each reader's path, and describe only what exists.
- [ ] Before the first public release, code PRs add no upgrade guides or
  migration steps to the public docs, and change a public sentence only when
  the code makes it false. Upgrade, migration and rollout notes go in
  docs-internal or the PR body; a changelog entry is one plain sentence.

## Changing a design

- [ ] Read logs and transcripts to find what happened before proposing a fix.
- [ ] Fix every instance of a defect or wrong pattern in the repository, not
  only the reported one.
- [ ] A fix shrinks the system. If removing the cause is no larger than the
  fix and changes no public interface, remove it in the same change. Otherwise
  fix the defect with the smallest change and propose the redesign to the
  project lead.
- [ ] File defects outside the task with their owner. A constraint an agent
  added on its own is not a requirement.
