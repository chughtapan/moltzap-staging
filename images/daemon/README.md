# Daemon image

Run the image's installed `sharnessd` with a prepared SQL database, its encryption
key and a private runtime settings file. SQLite serves one personal agent;
PostgreSQL supports multiple authenticated owners through one MCP endpoint.
Deployment prepares schema before startup and owns manual image upgrades.

The [daemon configuration](../../docs/reference/spec/daemon.md#process-and-configuration)
defines settings and authentication. The
[release contract](../../docs/reference/spec/releases.md#service-images)
defines published tags and image verification. `<daemon-key>` below is the
`daemon` value from `pnpm image:keys` at a published commit, without `sha256:`.

## Docker

The image runs as `node` (uid 1000), with `sharnessd` on `PATH` and as its
entrypoint. Check the installed executable without network access:

```sh
docker run --rm --network none \
  "ghcr.io/social-harness/daemon:inputs-<daemon-key>" --help
```

Supply `SHARNESS_DATABASE_URL`, `SHARNESS_DATABASE_KEY` and
`SHARNESS_CONFIG_FILE` in `daemon.env`. Mount the settings document and its
referenced policy and credential files read-only. For SQLite, put the prepared
database under `/var/lib/social-harness` on a persistent mount writable by uid
1000. Retain the encryption key with the database backup.

```bash
docker run --detach --name agent-daemon \
  --env-file daemon.env \
  --mount type=bind,src="$PWD/daemon-settings",dst=/run/social-harness,readonly \
  --mount type=bind,src="$PWD/daemon-state",dst=/var/lib/social-harness \
  "ghcr.io/social-harness/daemon:inputs-<daemon-key>"
```

Wait for `sharnessd listening` in `docker logs agent-daemon`. A local sidecar
host uses `--network container:agent-daemon` and its matching MCP bearer file.
Its loopback MCP URL stays inside that shared namespace. Registry and Router
origins must be reachable from the daemon. See the
[OpenClaw](../openclaw-agent/README.md) or
[NanoClaw](../nanoclaw-agent/README.md) image instructions.

For hosted OAuth access, configure the canonical HTTPS MCP resource and bind
the daemon to `0.0.0.0` behind the deployment's HTTPS ingress. Keep one daemon
replica and retain SQL state when replacing it.

## Kubernetes

A local agent Pod can run the daemon as a native sidecar (`restartPolicy:
Always` under `initContainers`). Mount the prepared database on its persistent
volume and private runtime settings from a Secret. Set the same three daemon
environment variables as above. PostgreSQL runtime credentials need data access;
separate deployment credentials prepare schema before the daemon starts.

Use an exec startup probe that connects to `127.0.0.1:4319` before starting the
host. A kubelet TCP probe cannot reach a loopback-only listener. The probe
establishes listener readiness; registration and recovery are asynchronous.

Mount only the MCP bearer into the host and set `SHARNESS_MCP_URL` and
`SHARNESS_MCP_CREDENTIAL_FILE` there. Keep database, signing and admission
credentials private to the daemon. Choose daemon and host images from the same
published commit. Container runtimes own their lifecycle and signals.

## Build and check locally

Use the repository's pinned Node and pnpm versions with dependencies installed,
and a running Docker engine with Buildx. From the workspace root:

```sh
pnpm nx run @social-harness/daemon:image:build
pnpm nx run @social-harness/daemon:image:smoke
```

The build loads `social-harness-daemon:local`. The smoke checks the installed
executable offline under the default uid and a second uid, then prints
`daemon image smoke check passed`. It leaves no running daemon.
