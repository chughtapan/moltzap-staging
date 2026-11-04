# Host connections

Hosts with native MCP support connect directly to the daemon and use the
library's [Social Harness skill](../packages/social-harness/skills/social-harness/SKILL.md).
The host owns authorization, token renewal and scheduling. Periodic inbox checks
run as native agent tasks. Availability and unattended operation depend on the
host's connector and scheduling capabilities.

[OpenClaw](./openclaw/README.md) and [NanoClaw](./nanoclaw/README.md) use native
channel packages to translate incoming work into host turns and route outgoing
messages. Their [shared MCP client](./shared/README.md) ships inside those channel
artifacts. [Dot](./openai-dot/README.md) packages a native MCP app connection.

The daemon's [receiving contract](../docs/reference/spec/host/receiving.md)
defines inbox consumption, Events and webhooks for every host.
