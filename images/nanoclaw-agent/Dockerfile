# Stock NanoClaw plus the workspace's runtime closure, channel and skills.
# build.ts supplies the verified upstream source and builds its stock base.
ARG NANOCLAW_AGENT_IMAGE=nanoclaw-agent:unconfigured
FROM ${NANOCLAW_AGENT_IMAGE} AS builder

USER root
# better-sqlite3's native build needs a compiler, make and Python. They are
# pinned to the versions Debian bookworm, the pinned base's release, ships, so
# a changed toolchain fails here instead of compiling the addon differently.
RUN apt-get update \
  && apt-get install --yes --no-install-recommends \
    g++=4:12.2.0-3 make=4.3-4.1 python3=3.11.2-1+b1 \
  && rm -rf /var/lib/apt/lists/*

COPY --from=nanoclaw-source . /opt/social-harness/nanoclaw/app
COPY --from=social-harness-channel . /opt/social-harness/nanoclaw/channel/
COPY nanoclaw-v2.3.0.patch install-channel.ts pnpm-lock.yaml /opt/social-harness/nanoclaw/

# The install takes every dependency, with its integrity hash, from the
# committed pnpm-lock.yaml that `build.ts lock` resolved for this patched
# and prepared tree, and fails if the tree no longer matches it.
COPY --from=social-harness-workspace . /opt/social-harness/workspace/
WORKDIR /opt/social-harness/nanoclaw/app
RUN git apply --check /opt/social-harness/nanoclaw/nanoclaw-v2.3.0.patch \
  && git apply /opt/social-harness/nanoclaw/nanoclaw-v2.3.0.patch \
  && node /opt/social-harness/nanoclaw/install-channel.ts \
    /opt/social-harness/nanoclaw/app \
    /opt/social-harness/nanoclaw/channel/sharness.ts \
  && cp /opt/social-harness/nanoclaw/pnpm-lock.yaml pnpm-lock.yaml \
  && HUSKY=0 pnpm install --frozen-lockfile \
  && pnpm run build \
  && mkdir -p /app/node_modules/@moltzap \
  && ln -s /opt/social-harness/nanoclaw/app/node_modules/@moltzap/social-harness /app/node_modules/@moltzap/social-harness \
  && ln -s /opt/social-harness/nanoclaw/app/node_modules/effect /app/node_modules/effect \
  && ln -s /app/node_modules container/agent-runner/node_modules \
  && /app/node_modules/.bin/tsc --noEmit -p container/agent-runner/tsconfig.json \
  && node --input-type=module --eval \
    'const { default: Database } = await import("better-sqlite3"); const database = new Database(":memory:"); database.close()' \
  && rm -rf /root/.cache /root/.local/share/pnpm/store

FROM ${NANOCLAW_AGENT_IMAGE}

USER root
ARG NANOCLAW_SOURCE_REVISION
ARG NANOCLAW_SOURCE_ARCHIVE_SHA256
LABEL org.social-harness.nanoclaw-source-revision="${NANOCLAW_SOURCE_REVISION}"
LABEL org.social-harness.nanoclaw-source-archive-sha256="${NANOCLAW_SOURCE_ARCHIVE_SHA256}"

COPY --from=builder /opt/social-harness/workspace /opt/social-harness/workspace
COPY --from=builder /opt/social-harness/nanoclaw/app /opt/social-harness/nanoclaw/app
# NanoClaw links every directory under container/skills into each agent
# group's ~/.claude/skills, so this adds the skills the Client ships without
# changing NanoClaw's source.
COPY --from=social-harness-workspace node_modules/@moltzap/social-harness/skills/ /opt/social-harness/nanoclaw/app/container/skills/
COPY entrypoint.ts /opt/social-harness/nanoclaw/entrypoint.ts
COPY cli-gateway.ts preload.ts session-processes.ts initialize-agent-group.ts /opt/social-harness/nanoclaw/

# Bun resolves the mounted runner from /app, while Node resolves the host from
# its extracted tree. The links give both processes the same patched packages.
RUN mkdir -p /var/lib/social-harness/nanoclaw \
  && mkdir -p /app/node_modules/@moltzap \
  && ln -s /opt/social-harness/nanoclaw/app/node_modules/@moltzap/social-harness /app/node_modules/@moltzap/social-harness \
  && ln -s /opt/social-harness/nanoclaw/app/node_modules/effect /app/node_modules/effect \
  && ln -s /opt/social-harness/nanoclaw/app/node_modules /opt/social-harness/node_modules \
  && ln -s /opt/social-harness/nanoclaw/app/container/agent-runner/src /app/src \
  && ln -s /opt/social-harness/nanoclaw/app/container/skills /app/skills \
  && ln -s /opt/social-harness/nanoclaw/app/container/CLAUDE.md /app/CLAUDE.md \
  && ln -s /var/lib/social-harness/nanoclaw/current-session-context /app/.nanoclaw-session.json \
  && rmdir /workspace/group /workspace/extra /workspace \
  && ln -s /var/lib/social-harness/nanoclaw/current-workspace /workspace \
  && chown -R node:node /var/lib/social-harness/nanoclaw

ENV NODE_ENV=production
WORKDIR /var/lib/social-harness/nanoclaw
USER node
ENTRYPOINT ["/usr/bin/tini", "--", "node", "/opt/social-harness/nanoclaw/entrypoint.ts"]
