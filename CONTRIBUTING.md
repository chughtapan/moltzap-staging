# Contributing to Social Harness

Set up a Social Harness checkout, verify your change, and submit it for review.

## Prerequisites

- A local checkout of [the repository](https://github.com/social-harness/social-harness).
- The Node.js version in `.node-version` and the pnpm version in
  `package.json`. With Corepack, `corepack enable` selects the package manager.
- The root `AGENTS.md` and instructions for the package you are changing.
- A PostgreSQL server for the tests, with `SHARNESS_PG_TEST_URL` set to a
  login on it that can create schemas and roles. Each test creates a schema
  of its own and drops it afterward. For example:

  ```bash
  docker run --detach --publish 127.0.0.1:5432:5432 --env POSTGRES_HOST_AUTH_METHOD=trust postgres:17
  export SHARNESS_PG_TEST_URL=postgres://postgres@127.0.0.1:5432/postgres
  ```

## Local setup

Run these commands from the repository root:

```bash
pnpm install --frozen-lockfile
```

```bash
pnpm build
```

pnpm installs the locked dependencies, then Nx builds the packages in dependency
order. Both commands should exit with status 0.

## Make the change

Branch from `main` and keep one coherent change in the pull request, including
its tests and documentation. Follow the repository's
[conventions](AGENTS.md).

The [constitution](docs/reference/constitution.md) and
[specifications](docs/reference/spec/index.md) define the public contracts. Reconcile
an interface change with its specification before implementation.

## Before submitting a PR

Build, run the affected tests, and check lint and formatting:

```bash
pnpm build
```

```bash
pnpm test:affected
```

```bash
pnpm check
```

Each command should exit with status 0. Build before lint so typed linting can
resolve package declarations. Choose additional tests by the callers your
change affects. CI runs the full gate on the pull request.

For a documentation change, also check site links:

```bash
pnpm docs:check
```

The result should report no broken internal links.

## Integration tests

The tests under `tests/integration` run a real `sharnessd` process and sign
several owners' agents in to it. Use this workspace only for tests that start a daemon process. Keep
every other test beside the code it exercises.

Run commands from the repository root. Nx builds the packages each target
needs. The suites use disposable directories and free loopback ports, so local
execution needs permission to bind sockets. Image scenarios require Docker.

| Command | Runs |
| --- | --- |
| `pnpm nx run @social-harness/integration-tests:test:integration` | Daemon acceptance suites |
| `pnpm nx run @social-harness/integration-tests:test:live` | Docker image scenarios |

Every daemon keeps its agents on the PostgreSQL server that
`SHARNESS_PG_TEST_URL` names. Each scenario also creates a restricted login
for its daemon, and removes it afterward.

## Architecture checks

`pnpm lint` includes the package architecture checks. To run them separately:

```bash
pnpm arch:check
```

Analysis exits with status 0 when it succeeds, 1 for findings, or 2 when it
cannot analyze a project. The root package's hand-maintained
`safer-architecture.config.json` owns its budgets, domains, and entrypoints.
Explain changes to those constraints or `safer-arch-ignore` directives in the
PR description so reviewers can assess them.

## Container images

CI builds every image a release publishes. See the
[image contract](docs/reference/spec/releases.md#service-images) for published names,
architectures, and signature requirements.

## Documentation

Choose a document's home and review its prose with the
[writing guide](.agents/skills/docs/SKILL.md).
Edit hand-written pages at their source. Shared site values live in
`docs/docs.json`.

| Command | Result |
| --- | --- |
| `pnpm docs:dev` | Preview the Mintlify site locally. |
| `pnpm docs:open` | Preview the site and open it in a browser. |
| `pnpm docs:check` | Check internal site links. |

## Open the pull request

Open against `main`. Explain the problem, the resulting behavior, and which
checks ran. Link the issue when one exists. Obtain independent review and
resolve material findings before merging.

For a vulnerability, use the
[security reporting procedure](SECURITY.md).
Maintainer release behavior is defined in
[Publication and versions](docs/reference/spec/releases.md#publication-and-versions).
