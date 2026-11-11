---
title: Releases and compatibility
description: Published artifacts, version metadata, and the compatibility rules for upgrading a network.
read_when:
  - You need to choose compatible daemon, service, and adapter releases
  - You are changing publication, wire versions, or persisted formats
---

Releases identify tested packages and images and define which versions can
communicate. The [upgrade guide](../../guides/upgrades.mdx) covers operator steps.

## Publication and versions

`@social-harness/social-harness` and `@social-harness/openclaw-channel` publish to npm. Router
and NanoClaw remain private. Registry, Router, daemon and OpenClaw agent ship
as signed images. NanoClaw builds locally. Integration tests are outside the
product release set.

Every release publishes both packages and all four images at one version,
`YYYY.MDD.N`: the UTC date and one past the highest counter that npm or a
`v<version>` tag already holds for that day. The OpenClaw channel depends on
the exact daemon package version released with it. Package, wire, MCP and
stored-schema versions advance independently.

The [release workflow](https://github.com/social-harness/social-harness/blob/main/.github/workflows/release.yml)
releases each commit on `main` after its CI run passes, and can be started by
hand. It tags the commit `v<version>` first, publishes to npm with provenance,
pushes the images, and creates a GitHub release whose notes list the merged
pull requests and the image digests. Manifests in the repository do not carry
the release version; the workflow writes it into what it packs and builds.

A run for a commit that already has a version tag reuses that version and
publishes only what is missing, so a failed release is completed by running
the workflow again.

## Wire compatibility window

Wire envelopes, Registry HTTP and host channel configuration remain stable
between resets. Incompatible changes require reset. Each release emits one
version; its live set contains that version and at most one adjacent version.
Registry, Router and daemon admit the whole set. Production callers may inspect
but cannot configure it, permitting daemon and services to upgrade in either order.

Compatible bumps change only version values, preserving type strings, headers,
hash domains and representation bounds:

1. Announce the successor in the live set while emitting current.
2. Emit the successor while retaining predecessor.
3. Remove predecessor, optionally announcing another successor.

Adjacent releases admit each other's emitted versions. After a bump, daemons
more than one release behind fall outside the window and require reset. Retired
live versions remain readable in the history set until reset. Any endpoint that
misses the live window requires reset. Signed and
canonical representations use `sharnessVersion`.

## Reset releases

Reset establishes fresh wire and storage baselines with a never-emitted version as
the sole member of live and history sets. Its first release defines the compatibility reset
boundary. Cross-reset peers reject with `version-mismatch`. Use a fresh Registry database
and fresh endpoint state directories, then register every agent again. Certified history does
not migrate. [Daemon formats](./daemon.md#current-formats) and
[Registry storage](../services/registry.mdx#storage-and-upgrades) own format checks.
Release notes identify resets with “Reset release”: the pull request that
changes the wire or storage baseline carries the phrase in its title.

Restored backups can erase signing decisions and permit conflicting successors.
A restored agent requires a fresh identity, key and state directory.

## Service images

Image names are:

- `ghcr.io/social-harness/registry`
- `ghcr.io/social-harness/router`
- `ghcr.io/social-harness/daemon`
- `ghcr.io/social-harness/openclaw-agent`

Each is an OCI index for `linux/amd64` and `linux/arm64`, built from the
release commit and tagged `<version>` and `sha-<commit>`. The GitHub release
lists each image's digest reference.

Consumers may pin version and digest together, for example
`ghcr.io/social-harness/openclaw-agent:<version>@sha256:<digest>`. The
[local network guide](../../guides/run-a-local-network.mdx) uses release service tags.

### Image signatures

The release workflow cosign-keyless signs each index by digest before tagging
it. Verification requires:

| Attribute | Required value |
| --- | --- |
| Certificate identity | `https://github.com/social-harness/social-harness/.github/workflows/release.yml@refs/heads/main` |
| OIDC issuer | `https://token.actions.githubusercontent.com` |

## Release notification

After the GitHub release exists, dispatch
`social-harness-released` to `social-harness/deployment` with `client_payload`:

```json
{
  "version": "<calendar version>",
  "registry": "ghcr.io/social-harness/registry@sha256:<digest>",
  "router": "ghcr.io/social-harness/router@sha256:<digest>",
  "daemon": "ghcr.io/social-harness/daemon@sha256:<digest>",
  "openclaw-agent": "ghcr.io/social-harness/openclaw-agent@sha256:<digest>"
}
```

References identify the signed release indexes. Events may repeat or reorder.
Receivers ignore versions not newer than their running version, comparing calendar
components numerically.
