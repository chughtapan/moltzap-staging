# OpenClaw host loads the matched channel from the daemon launcher volume.
ARG OPENCLAW_BASE_IMAGE=ghcr.io/openclaw/openclaw@sha256:70b8dbd23f8d27a206f43f9828fdebc87289e50d56dc55177354c9270d764026
FROM ${OPENCLAW_BASE_IMAGE} AS agent

USER root
ARG OPENCLAW_BASE_IMAGE
ARG TARGETARCH
ARG CLAUDE_CODE_VERSION=2.1.292
ARG CLAUDE_CODE_SHA256_AMD64=a967e7b1d8b4e47ee421d5433027880347952b0c0857abf880e2c942a4ec93b3
ARG CLAUDE_CODE_SHA256_ARM64=24caa9e6ff13bf227049a2626f1c816fc895023050f0ec3b12dbf14d897367e0
LABEL org.social-harness.openclaw-base-image="${OPENCLAW_BASE_IMAGE}"
LABEL org.social-harness.claude-code-version="${CLAUDE_CODE_VERSION}"

# The claude-cli agent runtime drives the unmodified Claude Code binary, which
# the base image does not ship, and that binary is handed the owner's
# subscription token. The npm package is only a launcher and Anthropic's
# install.sh runs whatever release is latest, so the pinned release binary is
# fetched directly and nothing runs until it matches the digest pinned above.
# The verified binary then installs itself under the host user's
# ~/.local/bin, which the base image already keeps on PATH, and the installed
# copy is checked against the same digest. The version probe runs under a
# foreign HOME because the agent host's HOME is the bootstrap state
# directory, not /home/node. The install leaves its own state under
# /home/node (an install identity, session and backup directories, caches,
# locks and npm's log), which the same layer removes so the image ships no
# Claude Code state.
USER node
RUN case "${TARGETARCH}" in \
    amd64) platform=linux-x64; digest="${CLAUDE_CODE_SHA256_AMD64}" ;; \
    arm64) platform=linux-arm64; digest="${CLAUDE_CODE_SHA256_ARM64}" ;; \
    *) echo "no pinned Claude Code binary for ${TARGETARCH}" >&2; exit 1 ;; \
  esac \
  && curl -fsSL "https://downloads.claude.ai/claude-code-releases/${CLAUDE_CODE_VERSION}/${platform}/claude" -o /tmp/claude \
  && echo "${digest}  /tmp/claude" | sha256sum -c - \
  && chmod +x /tmp/claude \
  && /tmp/claude install "${CLAUDE_CODE_VERSION}" \
  && rm -f /tmp/claude \
  && echo "${digest}  $(readlink -f /home/node/.local/bin/claude)" | sha256sum -c - \
  && HOME=/tmp/claude-version-probe /home/node/.local/bin/claude --version \
  && rm -rf /tmp/claude-version-probe /home/node/.claude /home/node/.claude.json \
    /home/node/.cache/claude /home/node/.local/state/claude /home/node/.npm
USER root


COPY patch-openclaw.ts /opt/social-harness/agent/
RUN node /opt/social-harness/agent/patch-openclaw.ts /app/dist \
    --base-image "${OPENCLAW_BASE_IMAGE}" \
    --marker /opt/social-harness/agent/openclaw-patch.json
RUN mkdir -p /app/node_modules/@openai \
  && ln -s /app/dist/extensions/codex/node_modules/@openai/codex /app/node_modules/@openai/codex
COPY entrypoint.sh configure-channel.ts /opt/social-harness/agent/
ENV NODE_ENV=production
WORKDIR /app
USER node
ENTRYPOINT ["bash", "/opt/social-harness/agent/entrypoint.sh"]
