#!/usr/bin/env bash
# Prepares OpenClaw authentication and state, then becomes the gateway.
# doctor imports the bootstrap's paired devices into SQLite without --fix,
# preserving openclaw.json. Channel selection runs before that import.
#
# OPENCLAW_SECRETS_PLAN binds environment-backed credentials before startup.
# The plan names variables and carries no values. A delivered Codex login is
# imported once without a backup archive. Reimporting it would overwrite tokens
# OpenClaw has since refreshed.
#
# A failed preparation step exits with its status. exec gives the gateway the
# entrypoint's signals and exit status. OPENCLAW_ENTRY selects its executable.
set -euo pipefail

readonly ENTRY="${OPENCLAW_ENTRY:-/app/openclaw.mjs}"
readonly CODEX_LOGIN_HOME="${CODEX_HOME:-${HOME}/.codex}"

if ! node "$(dirname "${BASH_SOURCE[0]}")/configure-channel.ts"; then
  echo "[social harness openclaw host] channel configuration failed; not starting OpenClaw" >&2
  exit 1
fi

#######################################
# Run one OpenClaw command before the gateway, ending the host if it fails.
# Arguments:
#   The step's name for the error message, then OpenClaw's arguments.
# Outputs:
#   Writes a failed step's exit status to STDERR.
#######################################
prepare() {
  local -r step="$1"
  shift
  local status=0
  node "${ENTRY}" "$@" || status=$?
  if ((status != 0)); then
    echo "[social harness openclaw host] ${step} exited ${status}; not starting the gateway" >&2
    exit "${status}"
  fi
}

#######################################
# Whether the agent's auth store already holds an `openai` profile.
# Globals:
#   ENTRY
# Returns:
#   0 when it does, 1 when it does not. A failed listing ends the host.
#######################################
has_openai_profile() {
  local profiles
  profiles="$(node "${ENTRY}" models auth list --provider openai --json)" || {
    echo "[social harness openclaw host] auth profile listing failed; not starting the gateway. Docs: https://github.com/social-harness/moltzap/tree/main/images/openclaw-agent#openclaw-host-image" >&2
    exit 1
  }
  local has_profile
  has_profile="$(node --input-type=module --eval \
      'const { profiles } = JSON.parse(await new Response(process.stdin).text());
       console.log(profiles.length > 0 ? "yes" : "no");' <<<"${profiles}")" || exit 1
  [[ "${has_profile}" == "yes" ]]
}

prepare doctor doctor --non-interactive
if [[ -f "${CODEX_LOGIN_HOME}/auth.json" ]] && ! has_openai_profile; then
  prepare "codex login import" migrate apply codex \
    --from "${CODEX_LOGIN_HOME}" --item auth:openai --include-secrets \
    --no-backup --force --yes
fi
if [[ -n "${OPENCLAW_SECRETS_PLAN:-}" ]]; then
  prepare "secrets apply" secrets apply --from "${OPENCLAW_SECRETS_PLAN}"
fi

exec node "${ENTRY}" gateway run --allow-unconfigured --port 18789
