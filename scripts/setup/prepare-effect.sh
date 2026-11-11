#!/usr/bin/env sh
#
# Clones the Effect source at the version the workspace resolves, for reading.
# The tag copies the pnpm-workspace.yaml `effect` override and `expected` is
# that tag's commit.

set -eu

dir=".repos/effect"
url="https://github.com/Effect-TS/effect.git"
tag="effect@3.22.1"
expected="417e0faa80e471d77fc4a67452e68b09ae0ee861"

if [ -e "$dir" ]; then
  actual=$(git -C "$dir" rev-parse HEAD 2>/dev/null || true)
  if [ "$actual" != "$expected" ]; then
    echo "Expected Effect $tag at $expected; found ${actual:-an invalid checkout} in $dir." >&2
    exit 1
  fi
  exit 0
fi

mkdir -p ".repos"
git clone --depth 1 --branch "$tag" --single-branch -- "$url" "$dir"

actual=$(git -C "$dir" rev-parse HEAD)
if [ "$actual" != "$expected" ]; then
  echo "Effect $tag resolved to unexpected commit $actual." >&2
  exit 1
fi
