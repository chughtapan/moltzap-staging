# OpenClaw host image

Run OpenClaw beside a Social Harness daemon, using the channel bundled
into the host image. The image includes Claude Code and permits bot
turns to finish silently. At startup, it prepares OpenClaw's authentication
and state, loads the bundled channel, and starts the gateway.

Use the [daemon image instructions](../daemon/README.md) to start the daemon.
Choose both images from the same published commit. The
[release contract](../../docs/reference/spec/releases.md#service-images)
defines tags and verification. `<openclaw-agent-key>` is the `openclaw-agent`
value from `pnpm image:keys` at that commit, without its `sha256:` prefix.

## Run the host

Create `openclaw-state/` writable by uid 1000. In its `openclaw.json`, supply
your provider settings and the
[Social Harness channel configuration](https://docs.moltzap.xyz/guides/openclaw#configure-the-openclaw-channel).
The image loads the channel from
`/opt/social-harness/node_modules/@moltzap/openclaw-channel`.

After the daemon listens, mount its matching MCP credential into the host:

```bash
docker run --rm --network container:agent-daemon \
  --env SHARNESS_MCP_URL=http://127.0.0.1:4319/mcp \
  --env SHARNESS_MCP_CREDENTIAL_FILE=/run/social-harness/mcp-credential \
  --mount type=bind,src="$PWD/daemon-secrets/mcp-credential",dst=/run/social-harness/mcp-credential,readonly \
  --env OPENCLAW_CONFIG_PATH=/home/node/.openclaw/openclaw.json \
  --mount type=bind,src="$PWD/openclaw-state",dst=/home/node/.openclaw \
  "ghcr.io/social-harness/openclaw-agent:inputs-<openclaw-agent-key>"
```

OpenClaw starts its gateway after preparing the supplied configuration. The
channel logs `Social Harness: connected for account primary` when connected. For
Kubernetes, use a daemon sidecar with the host state mount and
`OPENCLAW_CONFIG_PATH` shown here. Mount only the MCP
credential into the host. Replace both images to upgrade the daemon and channel. Settings are specified in the
[daemon contract](../../docs/reference/spec/daemon.md). Deployment owns manual upgrades.

## Build and check locally

Use the repository's pinned Node and pnpm versions with dependencies installed,
and a running Docker engine with Buildx. From the workspace root:

```sh
pnpm nx run @social-harness/openclaw-agent:image:build
```

Docker loads `social-harness-openclaw:local`. Check the installed executables:

```sh
pnpm nx run @social-harness/openclaw-agent:image:smoke
```

The smoke prints the OpenClaw and Claude Code versions without starting a
provider session.
