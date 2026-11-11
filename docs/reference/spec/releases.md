---
title: Releases
description: Published packages and images, their version, and what a release does to stored data.
read_when:
  - You need to choose matching daemon and adapter releases
  - You are changing publication or the database format
---

A release identifies packages and images that were built and tested together.

## Publication and versions

`@social-harness/social-harness` and `@social-harness/openclaw-channel` publish to npm.
The NanoClaw channel remains private. The daemon and the OpenClaw agent ship
as signed images. The NanoClaw agent image builds locally. Integration tests
are outside the product release set.

Every release publishes both packages and both images at one version,
`YYYY.MDD.N`: the UTC date and one past the highest counter that npm or a
`v<version>` tag already holds for that day. The OpenClaw channel depends on
the exact daemon package version released with it. The package version, the
MCP protocol revision and the database format advance independently.

The [release workflow](https://github.com/social-harness/social-harness/blob/main/.github/workflows/release.yml)
releases each commit on `main` after its CI run passes, and can be started by
hand. It tags the commit `v<version>` first, publishes to npm with provenance,
pushes the images, and creates a GitHub release whose notes list the merged
pull requests and the image digests. Manifests in the repository do not carry
the release version; the workflow writes it into what it packs and builds.

A run for a commit that already has a version tag reuses that version and
publishes only what is missing, so a failed release is completed by running
the workflow again.

## Stored data

A daemon starts only on a database in the format its release writes. A
database in another format is refused and is not converted, so a release that
changes the format needs a freshly prepared database. The agents and
conversations of the old database do not carry over.

## Service images

Image names are:

- `ghcr.io/social-harness/daemon`
- `ghcr.io/social-harness/openclaw-agent`

Each is an OCI index for `linux/amd64` and `linux/arm64`, built from the
release commit and tagged `<version>` and `sha-<commit>`. The GitHub release
lists each image's digest reference.

Consumers may pin version and digest together, for example
`ghcr.io/social-harness/openclaw-agent:<version>@sha256:<digest>`.

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
  "daemon": "ghcr.io/social-harness/daemon@sha256:<digest>",
  "openclaw-agent": "ghcr.io/social-harness/openclaw-agent@sha256:<digest>"
}
```

References identify the signed release indexes. Events may repeat or reorder.
Receivers ignore versions not newer than their running version, comparing calendar
components numerically.
