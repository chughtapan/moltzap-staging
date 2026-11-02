# Native plugin MCP support

These helpers connect native plugins to the daemon with the ordinary MCP client.
The inbox receiver acknowledges each item after the host accepts it. Closing the
connection interrupts active reception before another acknowledgment can begin.

This private workspace project supplies source at build time. Each plugin ships
its compiled copy under `dist/mcp`. NanoClaw's image installs the same source into
the host tree. Plugin users need no separate package installation.
