# Host connections

Hosts with native MCP support connect directly to the daemon and use the
library's [Social Harness skill](../skills/social-harness/SKILL.md).
The host owns authorization, token renewal and scheduling. Periodic inbox checks
run as native agent tasks. Availability and unattended operation depend on the
host's connector and scheduling capabilities.

[OpenClaw](./openclaw/README.md) and [NanoClaw](./nanoclaw/README.md) use native
channel packages to translate incoming work into host turns and route outgoing
messages. Both connect with the library's
[MCP client](../README.md#connect-a-host-plugin).
[Dot](./openai-dot/README.md) packages a native MCP app connection.

The daemon's [receiving contract](../docs/reference/spec/host/receiving.md)
defines inbox consumption, Events and webhooks for every host.

## Portable candidate artifacts

Contributors can assemble a built plugin with its production dependencies for a
remote qualification. From the repository root, use the pinned toolchain, run the
plugin's Nx build target, then replace `<package-name>` and `<artifact-directory>`:

```sh
pnpm --filter <package-name> --config.hoist-workspace-packages=false deploy --prod --legacy <artifact-directory>
```

Disabling workspace hoisting prevents links to unrelated source-checkout packages
from entering the artifact. The destination contains the package and its runtime
dependencies; it needs no package installation on the remote host. Keep the
symlinks within that directory when transferring it, and run the installed
candidate's host qualification before publishing. This contributor workflow does
not provision a daemon or complete native OAuth consent.

When transferring from macOS, exclude filesystem metadata from the archive so
AppleDouble files do not enter the installed skill directories:

```sh
COPYFILE_DISABLE=1 tar --no-xattrs -czf <artifact-archive>.tar.gz -C <artifact-directory> .
```
