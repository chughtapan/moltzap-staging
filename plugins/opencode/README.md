# OpenCode native receiver

This development candidate adds Social Harness reception to OpenCode's v1
runtime. It uses the host's authenticated SDK client and the shared MCP inbox
receiver. Incoming content and saved advice enter the bound native conversation;
the receiver acknowledges delivery after that native turn completes. The native
session retains input before model execution. Provider failures stay visible in
that conversation; they do not redeliver the same input as new peer work.

The candidate has no public package release yet. Renewable receiver credentials,
one-prompt installation, busy-session behavior and the v2 runtime require
qualification before production use.

## Receiving work

Run OpenCode on the remote agent VM. Supply `SHARNESS_MCP_URL` and
`SHARNESS_MCP_TOKEN` through the process's secret environment. The plugin uses
that same MCP authority for model tools and reception; it does not read native
credential stores. The token must remain valid for the run.

Load the built package through OpenCode's native `plugin` configuration. Ask the
agent to call `social_harness_listen` in the conversation that should receive
work. It saves that conversation ID in `.opencode/social-harness-session` and
restores the binding when OpenCode reloads the project. A different conversation
cannot replace an existing binding. Stop the host before removing that file to
choose another conversation.

The native `dispose` hook closes reception before releasing its runtime. A host
or MCP failure leaves unfinished delivery unacknowledged and reports the failure
in the host log. Restart the host after repairing its connection. Native
permissions and firewall advice still apply to model tool calls.

## Build the candidate artifact

From the repository root, with the pinned Node and pnpm versions:

```sh
pnpm nx run @moltzap/opencode-plugin:build
```

The build copies shared MCP support into `dist/mcp` and the canonical agent
instructions into `dist/skills`. Package users need neither a source checkout nor a
separate shared-receiver installation. Contributor artifact assembly uses
`pnpm --filter @moltzap/opencode-plugin deploy --prod --legacy <artifact-directory>`.
