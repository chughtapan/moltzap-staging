#!/bin/bash
# Decide whether Social Harness needs a release from GATE_COMMIT's tree and history
# (GITHUB_SHA when GATE_COMMIT is unset).
# Output: release or skip on stdout, the reason on stderr. Unreadable metadata
# fails instead of silently suppressing a release. Resumption and retries belong
# to the ordinary manual release dispatch. Private manifests never publish, so
# the revert check compares only the versions of the others.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
commit="${GATE_COMMIT:-${GITHUB_SHA:?GATE_COMMIT or GITHUB_SHA must name the commit to gate}}"

if [[ "$(git rev-parse --is-shallow-repository)" == true ]]; then
  echo 'ERROR: The release gate requires full history; fetch with depth 0. Docs: https://docs.moltzap.xyz/reference/spec/releases#publication-and-versions' >&2
  exit 1
fi

subject=$(git log -1 --format=%s "$commit")
if [[ "$subject" == 'chore(release): moltzap@'* ]]; then
  echo '::notice::Release commits do not start a release.' >&2
  echo skip
  exit 0
fi
fragments=$(git ls-tree -r --name-only "$commit" -- changelog.d/)
fragments=$(grep -vFx 'changelog.d/template.md' <<<"$fragments" || true)
if [[ -z "$fragments" ]]; then
  echo '::notice::No changelog fragments need a release.' >&2
  echo skip
  exit 0
fi
release_line=$(git log "$commit" --format='%H %s' |
  grep -m1 '^[0-9a-f]* chore(release): moltzap@' || true)
release_commit=${release_line%% *}
if [[ -z "$release_commit" ]]; then
  echo '::notice::No earlier release exists; starting a Social Harness release.' >&2
  echo release
  exit 0
fi
wire_manifest=$(git grep -l -E '"name"[[:space:]]*:[[:space:]]*"@moltzap/social-harness"' "$release_commit" -- ':(glob)packages/*/package.json' || true)
wire_path=${wire_manifest#*:}
source_path=packages/social-harness/src/version.ts
if ! head_wire=$(git show "$commit:$source_path" |
  node --input-type=module -e '
    import { readFileSync } from "node:fs";
    const { parseCompatibilityVersion } = await import(process.argv[1]);
    const result = parseCompatibilityVersion(readFileSync(0, "utf8"));
    if ("error" in result) { console.error(result.error); process.exit(1); }
    console.log(result.value);
  ' "$SCRIPT_DIR/read-wire-version.ts") ||
  ! release_wire=$(git show "$release_commit:$wire_path" |
    jq -er '.sharnessWireVersion | strings | select(length > 0)'); then
  echo "ERROR: Cannot read WIRE_VERSION from $source_path or sharnessWireVersion from $wire_path. Docs: https://docs.moltzap.xyz/reference/spec/releases#publication-and-versions" >&2
  exit 1
fi
if [[ "$head_wire" != "$release_wire" ]]; then
  echo '::notice::WIRE_VERSION differs from the last release; reset releases by hand through workflow_dispatch.' >&2
  echo skip
  exit 0
fi
manifests=$(git ls-tree -r --name-only "$commit" -- 'packages/' 'plugins/')
while IFS= read -r manifest; do
  [[ "$manifest" =~ ^(packages|plugins)/[^/]+/package\.json$ ]] || continue
  [[ "$(git show "$commit:$manifest" | jq -r .private)" != true ]] || continue
  release_manifest=$manifest
  if [[ "$manifest" == packages/social-harness/package.json ]]; then
    release_manifest=$wire_path
  fi
  git cat-file -e "$release_commit:$release_manifest" 2>/dev/null || continue
  if ! head_version=$(git show "$commit:$manifest" | jq -er '.version | strings | split(".") | map(tonumber)') ||
    ! release_version=$(git show "$release_commit:$release_manifest" | jq -er '.version | strings | split(".") | map(tonumber)'); then
    echo "ERROR: Cannot read version from $manifest. Docs: https://docs.moltzap.xyz/reference/spec/releases#publication-and-versions" >&2
    exit 1
  fi
  if jq -en --argjson head "$head_version" \
    --argjson release "$release_version" '$head < $release' >/dev/null; then
    echo "::notice::$manifest is older than the last release; skipping a reverted release." >&2
    echo skip
    exit 0
  fi
done <<<"$manifests"
echo '::notice::Changelog fragments need a Social Harness release.' >&2
echo release
