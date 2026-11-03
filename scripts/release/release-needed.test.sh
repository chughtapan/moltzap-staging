#!/bin/bash
# Exercise Social Harness push admission using real committed trees and refs.
# Every case clones a local fixture; no network or workspace git writes occur.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/social-harness-release-needed.XXXXXX")
trap 'rm -rf "$TEMP_DIR"' EXIT
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
export GIT_AUTHOR_NAME='Social Harness test' GIT_AUTHOR_EMAIL=test@example.invalid
export GIT_COMMITTER_NAME="$GIT_AUTHOR_NAME" GIT_COMMITTER_EMAIL="$GIT_AUTHOR_EMAIL"
PASS=0

# Clone a fresh release fixture for one independent behavior.
new_case() {
  cd "$TEMP_DIR"
  git clone --quiet --no-hardlinks fixture "case-$PASS"
  cd "case-$PASS"
}

# Record the tree as the commit being gated.
commit_tip() {
  git add .
  git -c core.hooksPath=/dev/null commit --quiet --allow-empty -m "$1"
  GITHUB_SHA=$(git rev-parse HEAD)
  export GITHUB_SHA
}

# Assert the decision, exit status and reason exposed to the workflow.
# Arguments: behavior, decision, status (default 0), reason substring.
check() {
  local name="$1" expected="$2" status="${3:-0}" reason="${4:-}"
  local actual result=0
  actual=$(bash "$SCRIPT_DIR/release-needed.sh" 2>"$TEMP_DIR/notice") || result=$?
  if [[ "$result" != "$status" || "$actual" != "$expected" ]] ||
    ! grep -qF "$reason" "$TEMP_DIR/notice"; then
    echo "FAIL: $name: expected $status '$expected' and '$reason'; got $result '$actual'"
    cat "$TEMP_DIR/notice"
    exit 1
  fi
  echo "PASS: $name"
  PASS=$((PASS + 1))
}

main() {
  local queued_sha
  mkdir "$TEMP_DIR/fixture"
  cd "$TEMP_DIR/fixture"
  git init --quiet --initial-branch=main
  mkdir -p packages/social-harness/src plugins/openclaw changelog.d
  printf '%s\n' 'export const WIRE_VERSION = "2026.1021.3";' > packages/social-harness/src/version.ts
  printf '%s\n' '{"name":"@moltzap/social-harness","version":"2026.1024.1","sharnessWireVersion":"2026.1021.3"}' > packages/social-harness/package.json
  sed 's|@moltzap/social-harness|@moltzap/openclaw-channel|' packages/social-harness/package.json > plugins/openclaw/package.json
  touch changelog.d/template.md changelog.d/consumed.Fixed.md
  commit_tip 'fix: previous change'
  rm changelog.d/consumed.Fixed.md
  printf '%s\n' '{"name":"@moltzap/social-harness","version":"2026.1025.1","sharnessWireVersion":"2026.1021.3"}' > packages/social-harness/package.json
  sed 's|@moltzap/social-harness|@moltzap/openclaw-channel|' packages/social-harness/package.json > plugins/openclaw/package.json
  commit_tip 'chore(release): moltzap@2026.1025.1'

  new_case
  touch changelog.d/new.Fixed.md
  printf '%s\n' '{"name":"@moltzap/social-harness","version":"2026.1025.1"}' > packages/social-harness/package.json
  commit_tip 'fix: new change'
  check 'fragments present' release 0 'need a Social Harness release'
  rm changelog.d/new.Fixed.md
  check 'fragments come from GITHUB_SHA, despite working tree deletion' release

  new_case
  mv packages/social-harness packages/previous-library
  commit_tip 'chore(release): moltzap@2026.1025.2'
  mv packages/previous-library packages/social-harness
  touch changelog.d/new.Fixed.md
  commit_tip 'chore: align library directory'
  check 'library directory rename preserves release admission' release
  printf '%s\n' '{"name":"@moltzap/social-harness","version":"2026.1024.1"}' > packages/social-harness/package.json
  commit_tip 'fix: revert library version after directory rename'
  check 'renamed library still detects reverted release versions' skip 0 'older than the last release'

  new_case
  commit_tip 'chore: template only'
  check 'template only' skip 0 'No changelog fragments'
  touch changelog.d/untracked.Fixed.md
  check 'working tree fragments do not trigger publication' skip

  new_case
  commit_tip 'chore(release): moltzap@2026.1025.1'
  check 'release-commit head' skip 0 'Release commits do not start a release'

  new_case
  touch changelog.d/new.Fixed.md
  printf '%s\n' 'export const WIRE_VERSION = "2026.1025.0";' > packages/social-harness/src/version.ts
  commit_tip 'feat: reset wire'
  check 'readable wire change requires manual reset' skip 0 workflow_dispatch
  printf '%s\n' 'export const WIRE_VERSION = "2026.1021.3";' > packages/social-harness/src/version.ts
  check 'wire version comes from the gated commit despite working tree edits' skip 0 workflow_dispatch

  new_case
  touch changelog.d/new.Fixed.md
  printf '%s\n' 'export const OTHER = "2026.1021.3";' > packages/social-harness/src/version.ts
  commit_tip 'fix: missing wire source'
  check 'unreadable head wire fails' '' 1 packages/social-harness/src/version.ts

  new_case
  printf '%s\n' '{"name":"@moltzap/social-harness"}' > packages/social-harness/package.json
  commit_tip 'chore(release): moltzap@2026.1025.2'
  printf '%s\n' '{"name":"@moltzap/social-harness","version":"2026.1025.2","sharnessWireVersion":"2026.1021.3"}' > packages/social-harness/package.json
  touch changelog.d/new.Fixed.md
  commit_tip 'fix: restore readable head'
  check 'unreadable release wire fails' '' 1 packages/social-harness/package.json

  new_case
  git -c core.hooksPath=/dev/null revert --no-edit HEAD >/dev/null
  commit_tip 'fix: revert release'
  check 'reverted release lowers package versions' skip 0 'older than the last release'

  new_case
  touch changelog.d/new.Fixed.md
  printf '%s\n' '{"name":"@moltzap/openclaw-channel","version":"2026.1024.1"}' > plugins/openclaw/package.json
  commit_tip 'fix: revert package version with new notes'
  check 'lower package version skips without restored fragments' skip 0 'older than the last release'

  new_case
  touch changelog.d/new.Fixed.md
  commit_tip 'fix: queued change'
  queued_sha=$GITHUB_SHA
  rm changelog.d/new.Fixed.md
  commit_tip 'chore: remove fragments at newer tip'
  check 'newer tip without fragments skips' skip 0 'No changelog fragments'
  GITHUB_SHA=$queued_sha check 'gated commit releases despite newer tip deletion' release

  new_case
  touch changelog.d/new.Fixed.md
  printf '%s\n' '{"name":"@moltzap/openclaw-channel","version":"next"}' > plugins/openclaw/package.json
  commit_tip 'fix: unreadable manifest version'
  check 'unreadable manifest version fails' '' 1 'Cannot read version'

  new_case
  mkdir -p packages/new-service
  printf '%s\n' '{"name":"@moltzap/new-service","version":"next"}' > packages/new-service/package.json
  touch changelog.d/new.Fixed.md
  commit_tip 'feat: add Social Harness service'
  check 'manifest absent at last release is ignored by revert check' release

  new_case
  mkdir -p plugins/receiver
  printf '%s\n' '{"name":"@moltzap/receiver-plugin","private":true}' > plugins/receiver/package.json
  commit_tip 'chore(release): moltzap@2026.1025.2'
  touch changelog.d/new.Fixed.md
  commit_tip 'fix: change beside a private connector'
  check 'private manifest without a version is ignored by revert check' release

  new_case
  mkdir -p packages/social-harness/fixture
  printf '%s\n' '{"name":"nested-fixture","version":"2026.1025.1"}' > packages/social-harness/fixture/package.json
  commit_tip 'chore(release): moltzap@2026.1025.2'
  printf '%s\n' '{"name":"nested-fixture","version":"2026.1024.1"}' > packages/social-harness/fixture/package.json
  touch changelog.d/new.Fixed.md
  commit_tip 'test: change nested fixture version'
  check 'nested manifests are ignored by revert check' release

  cd "$TEMP_DIR"
  git clone --quiet --depth=1 "file://$TEMP_DIR/fixture" shallow
  cd shallow
  GITHUB_SHA=$(git rev-parse HEAD)
  check 'shallow history fails even at a release head' '' 1 'requires full history'

  new_case
  touch changelog.d/consumed.Fixed.md
  commit_tip 'fix: reuse an issue fragment name'
  check 'new fragment reusing a released name' release

  mkdir "$TEMP_DIR/first-release"
  cd "$TEMP_DIR/first-release"
  git init --quiet --initial-branch=main
  mkdir -p packages/social-harness changelog.d
  printf '%s\n' '{"name":"@moltzap/social-harness","version":"2026.1025.1","sharnessWireVersion":"2026.1021.3"}' > packages/social-harness/package.json
  touch changelog.d/first.Fixed.md
  commit_tip 'fix: first change'
  check 'no earlier release' release 0 'No earlier release'
  echo "$PASS passed, 0 failed"
}

main "$@"
