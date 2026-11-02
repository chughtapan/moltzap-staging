# Effect MCP server backport

These files start from Effect 3 revision
`417e0faa80e471d77fc4a67452e68b09ae0ee861`,
`packages/ai/ai/src/McpServer.ts` and `McpSchema.ts`.
The upstream MIT license is retained in LICENSE.

The local fork omits AI Tool/Toolkit convenience registration. It adds MCP
2026-07-28 discovery, published output schemas and tool visibility, plus the
pinned Events draft. Its HTTP codec preserves string and numeric request IDs,
keeps notifications in their request stream, and publishes only sanitized
JSON-RPC errors. It uses the workspace's Effect 3 runtime and RPC engine.

Reference for newer MCP behavior: Effect revision
`522ad7cd1f2b2e5115ed0c087e02b74dd7794075`.
Events draft revision: `6682596d65eec778fe0b8b1f43b4e89d2fe2c546`.
