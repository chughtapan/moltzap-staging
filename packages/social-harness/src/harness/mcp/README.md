# Daemon MCP boundary

The MCP interface presents daemon operations through one catalog. Message
checks use the firewall, and reads use the store's retained agent view.

Push, polling and webhook receiving share the daemon's inbox and exclusive
consumer ownership. A change to one transport must preserve the ownership and
acknowledgment behavior seen by the others. Keep message classification and
policy admission in the shared delivery path so a new transport cannot bypass
them.

The [daemon contract](../../../../../docs/reference/spec/daemon/daemon.md) and
[ingress contract](../../../../../docs/reference/spec/host/receiving.md) own
the tool and receive interfaces.
