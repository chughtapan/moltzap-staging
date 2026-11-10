---
name: delivery
description: How changes are verified, merged and released across social-harness, deployment and evals. Read it before you verify, merge or release a change, or brief another agent to.
---

# Delivering changes quickly

A change often crosses three repositories before it reaches the people who need
it:

| Repository | Role |
| --- | --- |
| `social-harness/social-harness` | The core Social Harness library, released to npm |
| `social-harness/deployment` | Hosting: the container images agents run in, built on the library |
| `social-harness/evals` | Evaluation tools that run those images |

Most of the time a change takes is waiting, not working: re-running checks CI
will run anyway, untangling conflicts, and handing off between steps. This
guide gives the principles that keep each step short. Each repository's
`AGENTS.md` adds only its exact commands and files.

All three repositories use [Nx](https://nx.dev) to run builds and checks. Each
build, typecheck, test or lint target of one package (an Nx *project*) is a
task.

## Check locally what your change can break; let CI check the rest

CI runs the complete suite on every pull request. Before pushing, run only the
tasks your change can affect. Nx works these out from your diff against main:
`pnpm nx affected -t build typecheck test lint`. Running everything locally
repeats CI's work, usually on a machine other people are also using, and costs
more than it saves.

- Leave expensive checks that your change cannot reach to CI, such as building
  container images when no image source changed.
- If a test file fails by timing out while the machine is busy, rerun that one
  file once. If it passes, push and let CI decide. Do not rerun the whole suite.
- When you hand a task to another agent, give it the same scope. Do not ask for
  "full verification" in a brief; it overrides these rules and brings the cost
  back. If a person asks you for a full local run, do it for that task only.

## Merge only on green

These repositories do not mark any check as *required*, so GitHub lets a pull
request merge even when its checks failed, and GitHub's auto-merge merges as
soon as there is no conflict. Do not enable auto-merge on a pull request. Merge
through a gate instead: wait until the pull
request's checks finish, and merge only if each one passed or was skipped.

```sh
gh pr checks <number> --watch && gh pr merge <number> --merge
```

`gh pr checks` exits non-zero when a check fails, so the merge does not run.
Gate each pull request separately; a loop that prints several results and then
merges them all is how a failed pull request gets merged.

## Hand mechanical conflicts to Copilot

A conflict in a file that is generated or only appended to, such as a
changelog, a test snapshot or generated documentation, needs no judgment. A
lockfile is regenerated, not merged line by line: after resolving its manifest,
run the package manager's install and commit the result. Comment on the pull request:

```text
@copilot Resolve the merge conflicts with main.
```

The GitHub Copilot coding agent pushes a commit that resolves them. Review that
it touched only those files and kept the entries from both sides, then merge
through the gate. If its commit changes anything else, or none arrives within an
hour, resolve the conflict yourself. A conflict in code the pull request itself
changes is resolved by the pull request's author.

## Release in order

A repository can adopt a library version only after it is published, so a
change that spans the three repositories moves one step at a time:

Each repository's `AGENTS.md` names the workflows for its step.

1. **Release the library.** The library's release workflow publishes the
   packages whose content changed, at one new calendar version. Start it from the latest commit on main. If
   main moves while it runs, the run stops itself; start a new one from the new
   latest commit.
2. **Move deployment to that version.** A *pin* is an exact version recorded in
   a manifest or lockfile. Change every pin of the library's packages in one
   pull request; deployment's own checks fail if any pin disagrees. Then publish
   images, but only from a main commit whose checks passed; the image workflow
   refuses any other.
3. **Move evals to a commit on deployment's main** and to the same library version,
   in one pull request. Evals' checks fail if the deployment commit is not on
   deployment's main, or if the two repositories pin different versions.

Each step waits for the previous step to be published: the library's packages on
npm, or deployment's change merged to main with its images built. Release as soon as a library change is
merged. If you know another library change will merge the same day, wait for it
and release both together, so the chain runs once; if you don't know of one,
release now.

## Make the cache do the work

Nx records each task's result under a hash of the task's inputs, and shares the
results through a remote cache, one per repository. A task whose inputs match a
stored result is restored, not run.

- Only commits that CI built on main are in the remote cache. Merge main into
  your branch before building, so packages you did not change hash the same as
  main's and are restored.
- Each repository's cache has its own address and access token, read from
  environment variables (`NX_SELF_HOSTED_REMOTE_CACHE_SERVER` and
  `NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN`); ask the repository's owner for
  them. The variables must match the repository the command runs in. A command
  that carries another repository's variables finds nothing and runs every task.
  Nx marks each restored task `[remote cache]` or `[local cache]` in its output;
  if a task you did not change runs instead, check the address in
  `NX_SELF_HOSTED_REMOTE_CACHE_SERVER`.
- A target marked as not cacheable, such as a long integration suite, runs on
  every change. Make it cacheable by listing everything it reads as its inputs
  in its Nx configuration, and run its independent parts in parallel. Do this in
  its own pull request, not inside an unrelated change.

## Background: which tools use the slow compiler

Compiling and typechecking use the native TypeScript compiler (TypeScript 7),
several times faster than the JavaScript implementation (TypeScript 6). Tools
that call TypeScript as a library, such as typed linting with typescript-eslint,
must still use TypeScript 6 until they support TypeScript 7. Lint gets faster
when that support arrives; upgrading the lint tool before then does not change
its speed.
