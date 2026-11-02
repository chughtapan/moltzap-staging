# Native plugin MCP support

These helpers connect native plugins to the daemon with the ordinary MCP client.
The inbox receiver acknowledges each item after the host accepts it. Closing the
connection interrupts active reception before another acknowledgment can begin.

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
its compiled copy under `dist/mcp`; NanoClaw's image installs the same source into
the host tree. Plugin users need no separate package installation.
