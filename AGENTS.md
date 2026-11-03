# Social Harness — agent instructions

## Start every task

Load the applicable skills before planning, lookup or editing. This is the
agent's responsibility; an ordinary user request is sufficient and need not
name a skill. Invoke the host's Skill tool when available; otherwise read the
entire `SKILL.md` at the path below. Listing a skill, remembering a previous
session or checking its hash does not load its instructions.

| Task | Load |
| --- | --- |
| Plan, design, write, review or land any change | `.agents/skills/checklist/SKILL.md` |
| Plan a change or prepare a handoff | `.agents/skills/shared-sdlc/SKILL.md` |
| Design, implement or review software | `.agents/skills/software-architecture/SKILL.md` |
| Verify, merge or release a change, or brief another agent to | `.agents/skills/delivery/SKILL.md` |
| Find, file or review an internal record | `.agents/skills/records/SKILL.md` |
| Write or edit public documentation | `.agents/skills/docs/SKILL.md` |

Load only applicable skills, then follow their targeted references.
Do not ask the user to invoke skills for you.

Every `CLAUDE.md` is a symlink to the `AGENTS.md` beside it.
`packages/*/AGENTS.md` and `plugins/*/AGENTS.md` add package specifics. Scoped
instructions refine this file and the constitution; they never override them.
A conflict is an authority defect, so work in that scope stops until the
instructions agree.

State only what a check cannot. If `pnpm lint` fails on it, name the check
rather than repeating the rule — prose that duplicates a linter costs context
every turn and drifts from the behavior that is actually enforced.

## Project

Social Harness is the layered infrastructure through which autonomous agents
representing different principals message, coordinate, and collaborate despite
faulty or malicious peers. npm packages keep the `@moltzap` scope until the
npm cutover; everything else uses Social Harness names. Identifiers in stored
data or on the wire keep their names until the planned reset release.
Executables use `sharness*`, environment variables use `SHARNESS_*`, and
protocol namespaces use `org.social-harness/*`.

Generic OpenClaw and NanoClaw images and their stand-in live tests live under
`images/`; private product overlays stay in deployment. Image publication follows
[Publication and versions](docs/reference/spec/releases.md#publication-and-versions).

`main` is the only track; npm publication membership and versioning follow
[Publication and versions](docs/reference/spec/releases.md#publication-and-versions).
Releases follow the same section: a merge releases after CI passes, and resets
release by hand. A user-visible change adds a fragment under `changelog.d/`
(see `towncrier.toml`).

**The constitution is `docs/reference/constitution.md` → The constitution.** It is canonical
there and paraphrased nowhere, this file included: two copies at the top of the
same authority order drift, and the drift is invisible.

## Workflow

Understand the task → make a design decision if needed → implement and test →
independent review → ship. An existing request or issue authorizes bounded work;
do not ask for a separate plan approval unless the scope changes.

- A routine fix needs an issue/PR description and appropriate checks.
- A coordinated feature needs one plan.
- A durable architecture, public interface, security, persistence, or package
  ownership choice needs one complete source-backed ADR and independent review.
  An agent cannot invent the owner's approval or treat a reviewer PASS as
  decision authority.
- Update the existing PR/plan for a handoff. Add a separate handoff only when
  those records cannot carry what the next agent needs.

Maintainer records and the pinned internal skills follow
[CONTRIBUTING.md → Maintainer records](CONTRIBUTING.md#maintainer-records).
Public specifications remain the implementation baseline until a change is
admitted. Agree on a complete decision candidate before starting its admission
review; do not automatically review intermediate drafts. Without maintainer
records, maintain public code using these specifications, normal planning,
independent review, and Git/PR commands.

Use an isolated worktree for concurrent changes, preserve other people's work,
and confirm branch, commit, uncommitted changes, existing checks, and next action
before resuming a handoff. Keep useful current evidence; do not commit failed-run
exports, old attempt logs, or duplicate summaries.

## Toolchain and skills

Use Node from `.node-version` and the pnpm version in `package.json`. Run build,
test, lint, and typecheck through `pnpm nx` or the package scripts that wrap it.
Effect and `@effect/*` are the runtime idiom. Tooling is TypeScript;
`scripts/tsconfig.json` enforces erasable syntax for the scripts Node runs
directly. Write `.mjs` or `.js` only for a file a tool cannot load as
TypeScript.

Load the Google guide that matches the work: `google-typescript-style` for
`.ts`, `google-javascript-style` for JavaScript a tool requires,
`google-shell-style` for `.sh`, `google-swe-testing`, `google-documentation-guide`,
`google-swe-change-management`, `google-swe-builds-dependencies-and-ci` for
build, dependency, and CI policy, `google-swe-engineering-standards` for the
lint and architecture gates themselves, and the applicable Google code-review
author or reviewer guide. New code meets its guide in the commit that
introduces it; existing code is brought along by the change that touches it.
Repository law, Effect conventions, Nx, ESLint, oxfmt, and scoped package
instructions take precedence. Link to the guide rather than copying it into the
repository, and keep tests and documentation in the change they explain.

gstack reviews compose with the Google guides where they apply.

| Guide | Loaded by |
|---|---|
| `google-code-review-reviewer` | `/ship`'s structured review |
| `google-swe-testing` | `/ship`'s testing specialist; `/plan-eng-review`'s test review |
| the same language guides | `/ship`'s maintainability specialist; `/plan-eng-review`'s code-quality review |
| `google-swe-engineering-standards` | the same two, for changes to the lint or architecture gates |
| `google-swe-builds-dependencies-and-ci` | `/plan-eng-review`'s architecture review; `/ship` for workflow, Nx, manifest, or release-script changes |
| `google-swe-compute-platforms` | either, for cluster execution: profiles, admission, run namespaces, images, Temporal |
| `google-swe-change-management` | `/ship`'s data-migration and api-contract specialists; `/plan-eng-review`'s scope challenge |
| `google-swe-code-review-systems` | either, for changes to a gate, a review regime, or this file |
| `google-documentation-guide` | either, for documentation claims |

State which checks and reviews actually ran.

## Dependency versions

Each dependency version or digest is set in one source for its kind. A copy
exists only where a tool needs a literal value, such as a lockfile, a
Dockerfile `FROM`, or the manifest of something built outside the workspace,
and a check fails when the copy disagrees with its source.

| Kind | Source |
| --- | --- |
| npm dependency range | `catalog` in `pnpm-workspace.yaml` |
| Forced npm resolution | `overrides` in `pnpm-workspace.yaml` |
| Supported Node.js range | `engines.node` in the root `package.json` |
| Node.js for development and CI | `.node-version` |
| pnpm | `packageManager` in the root `package.json` |
| OpenClaw support floor | `peerDependencies.openclaw` in `plugins/openclaw/package.json` |
| GitHub Action | its one `uses:` ref; pnpm and Node setup live in `.github/actions/setup-toolchain` |

## Code

- Symbol questions (where is X defined, who calls it, what type is
  this) → LSP. Grep/Explore only for breadth or text search.
- Cite by symbol name (`file.ts → handleFrame`), never by line;
  `file.ts:NNN` only in PRs, reviews, and issues.
- Rationale goes in JSDoc on the symbol it explains, in the commit
  message, or in a docs file — never scattered inline through a body. A
  `//` line inside a body moves to that symbol's JSDoc or goes; if it
  will not fit there, the function is doing too much. A line directly
  above a named thing sits on that symbol, not in the body: an
  object-literal property, a `const`, an `it`. Lint-disable
  justifications are the exception.
  A shell script's header block is its JSDoc. Write for a cold reader
  in present tense: why the code is shaped this way, non-obvious
  invariants, surprising constants. If deleting a comment loses nothing
  the name and signature already give, delete it, including any doc line
  that repeats the symbol it sits on. `@param` and `@returns` go as a
  whole set or not at all, since a partial set fails
  `jsdoc/check-param-names`; fold what a parameter carries beyond its type
  into the description prose. Lint requires a block on every export, so
  rewrite an export block to say what the name cannot rather than
  deleting it.
  JavaScript carries the JavaScript guide's typed annotations, which no
  signature gives. Never issue/spec/phase numbers, change narration
  (formerly, no longer, renamed from), or design alternatives. Rewrite
  touched comments in the same PR; history lives in git, not code.
- A new or touched user-facing error with a known fix, and a new or
  touched CLI `--help`, ends with a `Docs:` URL to the heading that
  matches its error text or command.

Effect idiom, typed errors, exhaustive switches, and cast bans are enforced by
the `agent-code-guard` eslint rules. Read their output rather than a summary
of it here.

Package-local `safer-architecture.config.json` files are hand-maintained.
`pnpm arch:check` runs safer-architecture-lsp against them.

## Tests

- `*.types-check.ts` canaries pin current type-level invariants; the
  header states the invariant and why it matters. Pin what exists —
  never a negative canary for something deleted.

## Verify

`pnpm lint` needs a prior `pnpm build` — typed linting resolves against built
`.d.ts` outputs, and CI builds first.

## Docs

Read this file and `docs/reference/constitution.md`, then the applicable public specification
under `docs/reference/spec/` and package instructions. Reconcile a boundary change with its public
specification; do not leave a binding interface specification only in chat,
agent state, or records outside this repository.

Writing or regenerating public docs uses the `docs` skill at
`.agents/skills/docs/SKILL.md`; `.claude/skills/docs` links to the same source.
Keep repository-owned skills under `.agents/skills/` with a matching Claude
link.
Generated files come from their source and must not be edited by hand.
The API reference uses native TypeDoc output (`pnpm docs:api`); shared site
content uses Mintlify snippets and `docs.json` variables.

<!-- nx configuration start-->
<!-- Leave the start & end comments to automatically receive updates. -->

## General Guidelines for working with Nx

- For navigating/exploring the workspace, use the `nx-workspace` skill when available - it has patterns for querying projects, targets, and dependencies
- When running tasks (for example build, lint, test, e2e, etc.), always prefer running the task through `nx` (i.e. `nx run`, `nx run-many`, `nx affected`) instead of using the underlying tooling directly
- Prefix nx commands with the workspace's package manager (e.g., `pnpm nx build`, `npm exec nx test`) - avoids using globally installed CLI
- You have access to the Nx MCP server and its tools, use them to help the user
- For Nx plugin best practices, check `node_modules/@nx/<plugin>/PLUGIN.md`. Not all plugins have this file - proceed without it if unavailable.
- NEVER guess CLI flags - always check nx_docs or `--help` first when unsure

## Scaffolding & Generators

- For scaffolding tasks (creating apps, libs, project structure, setup), use the `nx-generate` skill when available; otherwise inspect the generator help before running it

## When to use nx_docs

- USE for: advanced config options, unfamiliar flags, migration guides, plugin configuration, edge cases
- DON'T USE for: basic generator syntax (`nx g @nx/react:app`), standard commands, things you already know
- The `nx-generate` skill handles generator discovery internally - don't call nx_docs just to look up generator syntax


<!-- nx configuration end-->
