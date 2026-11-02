# Runs sharnessd-launcher over a seed of this commit's Client and OpenClaw channel.
# Explicit context paths keep builds accountable to the image input key.
# Legacy deploy packages workspace dependencies without injected lockfile entries.
# The seed lives on the launcher volume so its current link resolves in both containers.
FROM node:24.18.0-bookworm-slim@sha256:6f7b03f7c2c8e2e784dcf9295400527b9b1270fd37b7e9a7285cf83b6951452d AS build
WORKDIR /build
ENV HUSKY=0 NX_DAEMON=false NX_NO_CLOUD=true
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc .node-version nx.json tsconfig.base.json ./
COPY scripts/build/clean-dist.ts scripts/build/clean-dist.ts
COPY scripts/build/copy-social-harness-skills.ts scripts/build/copy-social-harness-skills.ts
COPY scripts/build/copy-plugin-mcp.ts scripts/build/copy-plugin-mcp.ts
COPY packages/social-harness/ packages/social-harness/
COPY plugins/openclaw/ plugins/openclaw/
COPY plugins/shared/ plugins/shared/
COPY images/daemon/package.json images/daemon/package.json
RUN corepack enable && corepack prepare pnpm@10.6.2 --activate
RUN pnpm install --frozen-lockfile --filter @social-harness/daemon...
RUN pnpm nx run @social-harness/daemon:build
RUN pnpm deploy --filter @social-harness/daemon --prod --legacy /deploy

FROM node:24.18.0-bookworm-slim@sha256:6f7b03f7c2c8e2e784dcf9295400527b9b1270fd37b7e9a7285cf83b6951452d
LABEL org.opencontainers.image.source="https://github.com/social-harness/moltzap"
LABEL org.opencontainers.image.description="Social Harness daemon launcher"
COPY --from=build --chown=node:node /deploy/ /opt/social-harness/launcher/seed/
ENV PATH="/opt/social-harness/launcher/seed/node_modules/.bin:${PATH}"
ENV SHARNESS_LAUNCHER_DIRECTORY=/opt/social-harness/launcher
ENV SHARNESS_LAUNCHER_SEED=/opt/social-harness/launcher/seed
ENV SHARNESS_LAUNCHER_PACKAGE=@moltzap/openclaw-channel
RUN chown node:node /opt/social-harness/launcher && chmod -R a+rX /opt/social-harness/launcher/seed
VOLUME ["/opt/social-harness/launcher"]
USER node
ENTRYPOINT ["sharnessd-launcher"]
