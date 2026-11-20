---
title: Releases
description: Published packages and images, and their version.
read_when:
  - You need to choose a daemon release
  - You are changing publication
---

A release identifies packages and images that were built and tested together.

## Publication and versions

`@social-harness/social-harness` publishes to npm.
The daemon ships
as a signed image.

Every release publishes the package and the image at one version,
`YYYY.MDD.N`: the UTC date and one past the highest counter that npm or a
`v<version>` tag already holds for that day. The package version
and the MCP protocol revision advance independently.

The [release workflow](https://github.com/social-harness/social-harness/blob/main/.github/workflows/release.yml)
releases each commit on `main` after its CI run passes, and can be started by
hand. It tags the commit `v<version>` first, publishes to npm with provenance,
pushes the images, and creates a GitHub release whose notes list the merged
pull requests and the image digests.

## Service images

Image names are:

- `ghcr.io/social-harness/daemon`

Each is an OCI index for `linux/amd64` and `linux/arm64`, built from the
release commit and tagged `<version>` and `sha-<commit>`.

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
  "daemon": "ghcr.io/social-harness/daemon@sha256:<digest>"
}
```

References identify the signed release indexes. Events may repeat or reorder.
Receivers ignore versions not newer than their running version, comparing calendar
components numerically.
