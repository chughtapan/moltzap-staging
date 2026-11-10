# Contributing to Social Harness

Follow the [contribution guide](https://docs.socialharness.org/guides/contributing) to set up a
checkout, check your change, and open a pull request. Repository and package
instructions live in [AGENTS.md](AGENTS.md).

Report vulnerabilities through [SECURITY.md](SECURITY.md).

## Maintainer records

Maintainers set `DOCS_INTERNAL_ROOT` to a checkout of the internal records at
the revision pinned in `.agents/internal-skills.json` and start with its
README. That revision pins the workflow; it does not approve proposed
decisions. The pinned skills under `.agents/skills/` and `.claude/skills/` are
updated and verified from that checkout, with `DOCS_COMMIT` set to the pinned
revision:

```shell
"$DOCS_INTERNAL_ROOT/bin/records" skills sync --target . --revision "$DOCS_COMMIT"
```

```shell
"$DOCS_INTERNAL_ROOT/bin/records" skills check --target . --revision "$DOCS_COMMIT"
```
