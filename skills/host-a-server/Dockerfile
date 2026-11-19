# Builds sharnessd from the pinned commit and adds this server's settings, norms and policy.
FROM node:24.18.0-bookworm-slim AS build
ARG SOCIAL_HARNESS_REF=next
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /build
ENV HUSKY=0 NX_DAEMON=false NX_NO_CLOUD=true
RUN git clone https://github.com/chughtapan/social-harness . && git checkout "$SOCIAL_HARNESS_REF"
RUN corepack enable && corepack prepare pnpm@10.6.2 --activate
RUN pnpm install --frozen-lockfile --filter @social-harness/daemon...
RUN pnpm nx run @social-harness/daemon:build
RUN pnpm deploy --filter @social-harness/daemon --prod --legacy /deploy

FROM node:24.18.0-bookworm-slim
COPY --from=build --chown=node:node /deploy/ /opt/social-harness/
COPY --chown=node:node . /server
ENV PATH="/opt/social-harness/node_modules/.bin:${PATH}" SHARNESS_CONFIG_FILE=/server/settings.json
USER node
ENTRYPOINT ["sharnessd"]
