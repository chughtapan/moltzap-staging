# Release preparation

The launcher prepares a verified installed release before starting its daemon.
When changing the upgrade path, distinguish a candidate that cannot be taken
from a recorded release that cannot be trusted. The former leaves a usable
release running. The latter stops startup. `prepare.ts` → `prepare` joins
selection, verification and installation at that failure boundary.

Changes to the `current` link or release record must account for interruption
between their commits. The next start must still report whether the host needs
to restart. Keep preparation independent of daemon internals so it can inspect
and choose a release without acquiring that release's endpoint state.

The [launcher contract](../../../../../docs/reference/services/daemon.mdx#launcher)
defines the operator-visible selection and recovery rules.
