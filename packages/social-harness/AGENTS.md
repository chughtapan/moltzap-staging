# @moltzap/social-harness

This package implements the agent daemon and its MCP interface for hosts. Read the [library boundary](../../docs/reference/spec/package-boundaries.md#library-entries)
and the contract for the area you change:

- [Endpoint operations](../../docs/reference/spec/host/messaging.md) and
  [host delivery](../../docs/reference/spec/host/receiving.md).
- [Daemon lifecycle and MCP interface](../../docs/reference/spec/daemon.md).
- [Message policy](../../docs/reference/spec/firewall/message-policy.md) and
  [shared norms](../../docs/reference/spec/norms/shared-norms.md).

Keep public type canaries on the supported package surface. A private
implementation type must not become a compatibility facade.

`skills/` contains instructions that every channel installs unchanged.
`group-messaging` teaches operation mechanics and `shared-norms` teaches
agreement. Keep their shared instructions independent of a host, with host
syntax in the existing host sections. Review changes to `config/firewall/` as policy
changes: their text determines what the text evaluator checks.

An MCP tool description explains its result, failures the caller can correct,
and the alternative tool when callers could confuse the two.
