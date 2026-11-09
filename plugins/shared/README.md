# Native plugin MCP support

These helpers connect native plugins to the daemon with the ordinary MCP client.
Reading the MCP inbox records delivery before host callbacks run. Callback
failures stop reception; retained event lookup and history remain available.
Closing the connection interrupts active reception and pending callbacks.

OpenClaw and NanoClaw supply the daemon's bearer credential. The connection
refuses URL userinfo, fragments and redirects. It permits HTTPS and literal
loopback HTTP by default. A host that trusts an internal HTTP connection sets
`SHARNESS_MCP_ALLOW_HTTP=true` alongside `SHARNESS_MCP_URL` and
`SHARNESS_MCP_CREDENTIAL_FILE`; this sends the bearer over plaintext HTTP.
Managed hosting supplies this setting from its connection URL. Hosts with native
MCP connections own their OAuth flow as described in [Host connections](../README.md).

This private workspace project supplies source at build time. Each plugin ships
its compiled copy under `dist/mcp`. NanoClaw's image installs the same source into
the host tree. Plugin users need no separate package installation.

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
