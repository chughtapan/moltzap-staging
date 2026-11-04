# Native plugin MCP support

These helpers connect native plugins to the daemon with the ordinary MCP client.
Reading the MCP inbox records delivery before host callbacks run. Callback
failures stop reception; retained event lookup and history remain available.
Closing the connection interrupts active reception and pending callbacks.

Connections accept either a fixed bearer or the stock MCP SDK's authentication
provider. A renewable connection lets that provider own token storage, refresh
and interactive authorization. Shared code serializes the SDK's OAuth refreshes
to protect rotating grants when requests overlap. The same provider supplies tool calls and Events;
plugins do not copy its tokens into a second receiver credential. Polling uses
fresh authorization on each request. Events stream reconnection after access-token
expiry remains unqualified.
The connection uses the daemon's fixed `use:mcp` permission. A `403`
insufficient-scope response fails; this receiver does not perform interactive
scope elevation. Request new permissions through the native client's authorization
flow.

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
