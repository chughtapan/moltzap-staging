#!/usr/bin/env bash
# Tests for npm-version-exists.sh against a mock npm on PATH.
#
# The mock answers `npm view <name>@<version> version --json` from
# NPM_MOCK_RESPONSES, a JSON object keyed by the exact `<name>@<version>`
# argument whose values are either the version string npm would print or
# `{ "exit": N, "body": "..." }` for a failing lookup. An absent key fails the
# way npm fails for an unpublished name or version.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PASS=0
FAIL=0

assert_exit() {
  local test_name="$1" expected="$2"
  shift 2
  local actual=0
  "$@" >/dev/null 2>&1 || actual=$?
  if [ "$expected" = "$actual" ]; then
    echo "PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $test_name — expected exit $expected, got $actual"
    FAIL=$((FAIL + 1))
  fi
}

MOCK_DIR=$(mktemp -d)
cat > "$MOCK_DIR/npm" <<'MOCK'
#!/usr/bin/env bash
node -e '
  const entry = JSON.parse(process.env.NPM_MOCK_RESPONSES)[process.argv[1]] ?? {
    exit: 1,
    body: JSON.stringify({ error: { code: "E404" } }),
  };
  if (typeof entry === "object" && "exit" in entry) {
    console.log(entry.body ?? "");
    process.exit(entry.exit);
  }
  console.log(JSON.stringify(entry));
' "$2"
MOCK
chmod +x "$MOCK_DIR/npm"
export PATH="$MOCK_DIR:$PATH"

cleanup() { rm -rf "$MOCK_DIR"; }
trap cleanup EXIT

SCRIPT="$SCRIPT_DIR/npm-version-exists.sh"

export NPM_MOCK_RESPONSES='{"@social-harness/social-harness@2026.901.0":"2026.901.0"}'
assert_exit "published version exits 0" 0 "$SCRIPT" @social-harness/social-harness@2026.901.0
assert_exit "missing version exits 1" 1 "$SCRIPT" @social-harness/social-harness@2026.901.1
assert_exit "never-published name exits 1" 1 "$SCRIPT" @social-harness/unpublished-fixture@2026.901.0

export NPM_MOCK_RESPONSES='{"@social-harness/social-harness@2026.901.0":{"exit":1,"body":"{\"error\":{\"code\":\"E503\"}}"}}'
assert_exit "registry failure exits 2" 2 "$SCRIPT" @social-harness/social-harness@2026.901.0

export NPM_MOCK_RESPONSES='{"@social-harness/social-harness@2026.901.0":{"exit":1,"body":""}}'
assert_exit "failure without a body exits 2" 2 "$SCRIPT" @social-harness/social-harness@2026.901.0

export NPM_MOCK_RESPONSES='{"@social-harness/social-harness@2026.901.0":{"exit":0,"body":""}}'
assert_exit "empty success output exits 2" 2 "$SCRIPT" @social-harness/social-harness@2026.901.0

assert_exit "no argument exits 2" 2 "$SCRIPT"

echo "$PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
