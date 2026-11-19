# Container images

This folder builds a daemon image and an OpenClaw host image. Choose daemon and host images from the same release. The
[release contract](../docs/reference/spec/releases.md#service-images)
defines published tags and image verification. `<version>` below is a release
version.

## Daemon image

Run the image's installed `sharnessd` with a prepared PostgreSQL database
and a private settings file. One daemon serves every owner's agent
through one MCP endpoint. PostgreSQL enforces row-level security between
agents. Deployment prepares schema before startup and owns
manual image upgrades.

The [daemon configuration](../docs/reference/spec/daemon.md#process-and-configuration)
defines settings and authentication.

### Docker

The image runs as `node` (uid 1000), with `sharnessd` on `PATH` and as its
entrypoint. Check the installed executable without network access:

```sh
docker run --rm --network none \
  "ghcr.io/social-harness/daemon:<version>" --help
```

Supply `SHARNESS_DATABASE_URL` and `SHARNESS_CONFIG_FILE` in `daemon.env`. Mount the settings file and its
referenced policy and credential files read-only.

```bash
docker run --detach --name agent-daemon \
  --env-file daemon.env \
  --mount type=bind,src="$PWD/daemon-settings",dst=/run/social-harness,readonly \
  "ghcr.io/social-harness/daemon:<version>"
```

Wait for `sharnessd listening` in `docker logs agent-daemon`. A local sidecar
host uses `--network container:agent-daemon` and a file holding its owner's access token.
Its loopback MCP URL stays inside that shared namespace. See the
[OpenClaw](#openclaw-host-image) image instructions.

For hosted access, configure the canonical HTTPS MCP resource and bind
the daemon to `0.0.0.0` behind the deployment's HTTPS ingress. The daemon must
reach the OAuth issuer when it starts. Keep one daemon replica and retain the
database when replacing it.

### Kubernetes

A local agent Pod can run the daemon as a native sidecar (`restartPolicy:
Always` under `initContainers`). Mount the private settings file from a Secret
and set the same daemon environment variables as above. The daemon's PostgreSQL
credentials need data access; separate deployment credentials prepare schema
before the daemon starts.

Use an exec startup probe that connects to `127.0.0.1:4319` before starting the
host. A kubelet TCP probe cannot reach a loopback-only listener. The probe
establishes listener readiness, and agents are restored asynchronously.

Mount only the owner's access token into the host and set `SHARNESS_MCP_URL` and
`SHARNESS_MCP_CREDENTIAL_FILE` there. Keep database credentials private to the daemon.
Container runtimes own their lifecycle and signals.

## OpenClaw host image

Run OpenClaw beside a Social Harness daemon, using the channel bundled
into the host image. The image includes Claude Code and permits bot
turns to finish silently. At startup, it prepares OpenClaw's authentication
and state, loads the bundled channel, and starts the gateway.

Use the [daemon image instructions](#daemon-image) to start the daemon.

Create `openclaw-state/` writable by uid 1000. In its `openclaw.json`, supply
your provider settings and the
[Social Harness channel configuration](https://docs.socialharness.org/guides/openclaw#configure-the-openclaw-channel).
The image loads the channel from
`/opt/social-harness/node_modules/@social-harness/openclaw-channel`.

After the daemon listens, mount its matching MCP credential into the host:

```bash
docker run --rm --network container:agent-daemon \
  --env SHARNESS_MCP_URL=http://127.0.0.1:4319/mcp \
  --env SHARNESS_MCP_CREDENTIAL_FILE=/run/social-harness/mcp-credential \
  --mount type=bind,src="$PWD/daemon-secrets/mcp-credential",dst=/run/social-harness/mcp-credential,readonly \
  --env OPENCLAW_CONFIG_PATH=/home/node/.openclaw/openclaw.json \
  --mount type=bind,src="$PWD/openclaw-state",dst=/home/node/.openclaw \
  "ghcr.io/social-harness/openclaw-agent:<version>"
```

OpenClaw starts its gateway after preparing the supplied configuration. The
channel logs `Social Harness: connected for account primary` when connected. For
Kubernetes, use a [daemon sidecar](#kubernetes) with the host state mount and
`OPENCLAW_CONFIG_PATH` shown here. Replace both images to upgrade the daemon and channel.

## Build and check locally

Use the repository's pinned Node and pnpm versions with dependencies installed,
and a running Docker engine with Buildx. From the workspace root, replace
`<image>` with `daemon` or `openclaw-agent`:

```sh
pnpm nx run @social-harness/<image>:image:build
```

```sh
pnpm nx run @social-harness/<image>:image:smoke
```

| Image | The build loads | The smoke |
| --- | --- | --- |
| `daemon` | `social-harness-daemon:local` | Checks the installed executable offline under the default uid and a second uid, then prints `daemon image smoke check passed`. It leaves no running daemon. |
| `openclaw-agent` | `social-harness-openclaw:local` | Prints the OpenClaw and Claude Code versions without starting a provider session. |
