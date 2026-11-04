#!/usr/bin/env bash
# Exercise the installed host executables without starting a provider session.
set -euo pipefail
readonly IMAGE="${1:?local OpenClaw image required}"
docker run --rm --network none --entrypoint node "${IMAGE}" /app/openclaw.mjs --version
docker run --rm --network none --entrypoint /home/node/.local/bin/claude "${IMAGE}" --version
docker run --rm --network none --entrypoint node "${IMAGE}" --input-type=module --eval '
  const { createRequire } = await import("node:module");
  const root = "/opt/social-harness/node_modules/@moltzap/openclaw-channel";
  const sdk = createRequire(`${root}/dist/plugin.js`)
    .resolve("openclaw/plugin-sdk/channel-core");
  if (!sdk.startsWith("/app/")) {
    throw new Error("channel did not resolve the host SDK");
  }
  const { default: plugin } = await import(`${root}/dist/index.js`);
  if (plugin.id !== "social-harness") throw new Error("installed channel did not load");
'
