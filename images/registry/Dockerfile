# Runs the Registry or the PGlite CLI that the local network starts separately.
# Explicit context paths keep builds accountable to the image input key.
# Legacy deploy packages workspace dependencies without injected lockfile entries.
FROM node:24.18.0-bookworm-slim@sha256:6f7b03f7c2c8e2e784dcf9295400527b9b1270fd37b7e9a7285cf83b6951452d AS build
WORKDIR /build
ENV HUSKY=0 NX_DAEMON=false NX_NO_CLOUD=true
COPY LICENSE NOTICE ./
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc .node-version nx.json tsconfig.base.json ./
COPY scripts/build/clean-dist.ts scripts/build/clean-dist.ts
COPY packages/social-harness/ packages/social-harness/
COPY packages/registry/ packages/registry/
RUN corepack enable && corepack prepare pnpm@10.6.2 --activate
RUN pnpm install --frozen-lockfile --filter @moltzap/registry...
RUN pnpm nx run @moltzap/registry:build
RUN pnpm deploy --filter @moltzap/registry --prod --legacy /deploy

FROM node:24.18.0-bookworm-slim@sha256:6f7b03f7c2c8e2e784dcf9295400527b9b1270fd37b7e9a7285cf83b6951452d
LABEL org.opencontainers.image.source="https://github.com/social-harness/moltzap"
LABEL org.opencontainers.image.description="Social Harness Registry process"
WORKDIR /app
COPY --from=build /deploy/ /app/
COPY --from=build /build/LICENSE /build/NOTICE /app/
ENV PATH="/app/node_modules/.bin:${PATH}"
ENV SHARNESS_REGISTRY_HOST=0.0.0.0 SHARNESS_REGISTRY_PORT=3001
USER node
EXPOSE 3001
ENTRYPOINT ["/app/bin/sharness-registry"]
