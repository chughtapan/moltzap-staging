#!/usr/bin/env bash
# Print the next calendar version for one release of every named package.
#
# Usage: compute-next-version.sh <package-dir>...
# Output: YYYY.MDD.N — today's UTC date (month without a leading zero) and one
# past the highest build counter any of the named packages has published for
# that day. The release writes that one string into the manifest of every
# package it selects, so the counter is taken over the union of the packages'
# npm histories rather than any single one. TAKEN_VERSIONS, whitespace-separated,
# adds versions npm has never seen but that are claimed anyway: the pushed
# release tags, so a release whose commit landed but whose publish never
# completed keeps its number.
#
# A package that has never been published answers 404 and contributes nothing
# to the union. A bootstrap-only package without a latest tag can return
# empty output successfully and also contributes nothing. Any other npm failure
# aborts: a registry outage read as "never published" would reuse a counter that
# is already taken.
set -euo pipefail

[ "$#" -ge 1 ] || { echo "usage: $0 <package-dir>..." >&2; exit 2; }

PREFIX="$(date -u +%Y.%-m%d)."

RESPONSES=("$(node -e 'console.log(JSON.stringify(process.argv[1].split(/\s+/u).filter(Boolean)))' "${TAKEN_VERSIONS:-}")")
for PKG in "$@"; do
  NPM_PACKAGE=$(node -p "require('./$PKG/package.json').name")
  if VERSIONS=$(npm view "$NPM_PACKAGE" versions --json 2>/dev/null); then
    RESPONSES+=("${VERSIONS:-[]}")
  elif node -e 'process.exit(JSON.parse(process.argv[1])?.error?.code === "E404" ? 0 : 1)' "$VERSIONS" 2>/dev/null; then
    RESPONSES+=("[]")
  else
    echo "npm view $NPM_PACKAGE failed without a 404: ${VERSIONS:-no output}" >&2
    exit 1
  fi
done

node -e '
  const [prefix, ...responses] = process.argv.slice(1);
  // A package with exactly one published version comes back as a bare string.
  const counters = responses
    .flatMap((text) => [JSON.parse(text)].flat())
    .filter((version) => version.startsWith(prefix))
    .map((version) => Number.parseInt(version.slice(prefix.length), 10))
    .filter(Number.isInteger);
  console.log(`${prefix}${counters.length === 0 ? 0 : Math.max(...counters) + 1}`);
' "$PREFIX" "${RESPONSES[@]}"
