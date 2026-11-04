#!/usr/bin/env bash
# Exercise the installed daemon offline as its default user and another user.
set -euo pipefail
readonly IMAGE="${1:?local daemon image required}"
docker run --rm --network none "${IMAGE}" --help
docker run --rm --network none --user 1001:1001 "${IMAGE}" --help
printf '%s\n' 'daemon image smoke check passed'
