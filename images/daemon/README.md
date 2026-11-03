# Daemon image

Run `sharnessd` with its configured SQL database and encryption key available
to the container. SQLite serves one personal agent; PostgreSQL supports multiple
authenticated owners through one MCP endpoint. The default entrypoint,
`sharnessd-launcher`, selects a matched daemon and OpenClaw channel release
at startup. The image's bundled release supplies the fallback.

Use the [daemon configuration](../../docs/reference/spec/daemon.md#process-and-configuration)
for settings and [Upgrades](https://docs.moltzap.xyz/guides/upgrades) for release
selection. The [release contract](../../docs/reference/spec/releases.md#service-images)
defines published tags and image verification. In the examples below,
`<daemon-key>` is the `daemon` value from `pnpm image:keys` at a published
commit, without its `sha256:` prefix.

## Run sharnessd directly

The image also puts `sharnessd` on `PATH` and runs it as the `node` user
(uid 1000). Use it directly when you need the bundled daemon without launcher
release selection, such as a Pod whose host does not load a channel from the
launcher volume. It uses the same daemon settings and persistent state.

In Docker, override the entrypoint with `--entrypoint sharnessd`. For example,
this prints the daemon's help and exits without starting a service:

```sh
docker run --rm --network none --entrypoint sharnessd \
  "ghcr.io/social-harness/daemon:inputs-<daemon-key>" --help
```

In Kubernetes, set `command: ["sharnessd"]` on the daemon container. The
following deployment examples use the default launcher entrypoint.

## Docker

Configure the database once using `sharnessd configure <settings.json>` and the
[daemon configuration](../../docs/reference/spec/daemon.md#process-and-configuration).
For SQLite, place it under `/var/lib/social-harness` on a persistent mount
writable by uid 1000. `daemon.env` supplies `SHARNESS_DATABASE_URL` and
`SHARNESS_DATABASE_KEY`; keep the key with your backup. A host's bearer file
must match the local credential saved in SQL. The daemon reads settings and
signing keys from the database at startup.

Keep a fresh anonymous launcher volume per daemon container. Docker copies the
image seed only when creating that volume. Keep SQL state on its persistent
mount and retain it when replacing containers.
```bash
docker run --detach --name agent-daemon \
  --env-file daemon.env \
  --env SHARNESS_LAUNCHER_UPGRADE=off \
  --mount type=bind,src="$PWD/daemon-state",dst=/var/lib/social-harness \
  --mount type=volume,dst=/opt/social-harness/launcher \
  "ghcr.io/social-harness/daemon:inputs-<daemon-key>"
```

Wait for `sharnessd listening` in `docker logs agent-daemon`, then start the host
with `--network container:agent-daemon` and the MCP credential file. See the
[OpenClaw](../openclaw-agent/README.md) or [NanoClaw](../nanoclaw-agent/README.md)
image instructions. Port `4319` is on loopback in this shared namespace. Registry and Router origins must be reachable
from the daemon. This example runs the image release with upgrades off. To
upgrade, replace both containers with the newer image and a fresh launcher volume.

## Kubernetes

Use the daemon as a native sidecar (`restartPolicy: Always` under
`initContainers`). An exec startup probe connects to `127.0.0.1:4319`. A TCP probe
from the kubelet cannot reach a loopback-only listener. Kubernetes waits for the
startup probe before starting the host.

The following Pod template excerpt assumes a `daemon-settings` ConfigMap, a
`daemon-secrets` Secret and a `daemon-state` PersistentVolumeClaim. The ConfigMap
sets launcher options; the Secret supplies `SHARNESS_DATABASE_URL` and
`SHARNESS_DATABASE_KEY`. Configure the
database before starting this Pod. `fsGroup` lets the daemon
create its state directory on a new volume, whose root it does not own. Take
`<daemon-key>` and `<openclaw-agent-key>` from `pnpm image:keys` at one release
commit, or at one commit whose `images.yml` run published. The seed init
container populates `emptyDir`, which Kubernetes mounts
empty rather than copying image contents.

```yaml
spec:
  securityContext:
    fsGroup: 1000
  initContainers:
    - name: launcher-seed
      image: ghcr.io/social-harness/daemon:inputs-<daemon-key>
      command: ["cp", "-R", "/opt/social-harness/launcher/seed", "/launcher/seed"]
      volumeMounts:
        - { name: launcher, mountPath: /launcher }
    - name: launcher-prepare
      image: ghcr.io/social-harness/daemon:inputs-<daemon-key>
      args: ["prepare"]
      envFrom:
        - configMapRef: { name: daemon-settings }
      env:
        - { name: SHARNESS_LAUNCHER_UPGRADE, value: "on" }
      volumeMounts:
        - { name: launcher, mountPath: /opt/social-harness/launcher }
    - name: daemon
      image: ghcr.io/social-harness/daemon:inputs-<daemon-key>
      restartPolicy: Always
      env:
        - { name: SHARNESS_LAUNCHER_UPGRADE, value: "off" }
      envFrom:
        - configMapRef: { name: daemon-settings }
        - secretRef: { name: daemon-secrets }
      startupProbe:
        exec:
          command:
            - node
            - --input-type=module
            - --eval
            - 'import { connect } from "node:net"; const s = connect(4319, "127.0.0.1", () => s.end()); s.on("error", () => process.exit(1));'
        periodSeconds: 2
        failureThreshold: 450
      volumeMounts:
        - { name: launcher, mountPath: /opt/social-harness/launcher }
        - { name: state, mountPath: /var/lib/social-harness }
  containers:
    - name: host
      image: ghcr.io/social-harness/openclaw-agent:inputs-<openclaw-agent-key>
      env:
        - { name: SHARNESS_MCP_URL, value: "http://127.0.0.1:4319/mcp" }
        - { name: SHARNESS_MCP_CREDENTIAL_FILE, value: /run/social-harness/mcp-credential }
      volumeMounts:
        - { name: launcher, mountPath: /opt/social-harness/launcher, readOnly: true }
        - name: daemon-secrets
          mountPath: /run/social-harness/mcp-credential
          subPath: mcp-credential
          readOnly: true
  volumes:
    - { name: launcher, emptyDir: {} }
    - name: state
      persistentVolumeClaim: { claimName: daemon-state }
    - name: daemon-secrets
      secret: { secretName: daemon-secrets }
```

The regular `launcher-prepare` init container upgrades once per Pod, before the
host loads its channel. The daemon sidecar keeps that release on restarts.
With startup registration configured, the probe passes after registration and
activation. With no registration settings, it can pass before registration.
Persist state for the lifetime of its signing key. Add the host's own writable
state and provider configuration.
The host mounts the public launcher tree and only the MCP credential from the
daemon's secrets.

## Build and check locally

Use the repository's pinned Node and pnpm versions with dependencies installed,
and a running Docker engine with Buildx. From the workspace root:

```sh
pnpm nx run @social-harness/daemon:image:build
```

Docker loads `social-harness-daemon:local`. Check the launcher's offline fallback
and the channel files it exposes to a host:

```sh
pnpm nx run @social-harness/daemon:image:smoke
```

The smoke prints `daemon image smoke check passed` on success. It leaves no
running daemon.
