#!/usr/bin/env bash
# Tests for compute-next-version.sh against a mock npm on PATH.
#
# The mock answers `npm view <name> versions --json` from NPM_MOCK_RESPONSES, a
# JSON object keyed by package name whose values are either the versions npm
# would print or `{ "exit": N, "body": "..." }` for a failing lookup. A package
# absent from the object fails the way npm fails for an unpublished name.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PASS=0
FAIL=0

assert_eq() {
  local test_name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    echo "PASS: $test_name"
    PASS=$((PASS + 1))
  else
    echo "FAIL: $test_name — expected '$expected', got '$actual'"
    FAIL=$((FAIL + 1))
  fi
}

YEAR=$(date -u +%Y)
MDD=$(date -u +%-m%d)
TODAY="${YEAR}.${MDD}"

MOCK_DIR=$(mktemp -d)
cat > "$MOCK_DIR/npm" <<'MOCK'
#!/usr/bin/env bash
node -e '
  const entry = JSON.parse(process.env.NPM_MOCK_RESPONSES)[process.argv[1]] ?? {
    exit: 1,
    body: JSON.stringify({ error: { code: "E404" } }),
  };
  if (!Array.isArray(entry) && typeof entry === "object" && "exit" in entry) {
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

# Package names supplied to the release version planner.
RELEASED=". plugins/openclaw"

echo "--- Test 1: No existing versions ---"
export NPM_MOCK_RESPONSES='{"@social-harness/social-harness":[]}'
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "no existing versions" "${TODAY}.0" "$RESULT"

echo "--- Test 1b: TAKEN_VERSIONS claims counters npm has never seen ---"
export NPM_MOCK_RESPONSES='{"@social-harness/social-harness":[]}'
RESULT=$(TAKEN_VERSIONS="${TODAY}.0
${TODAY}.3 2025.101.9" "$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "taken versions join the union" "${TODAY}.4" "$RESULT"
RESULT=$(TAKEN_VERSIONS="" "$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "empty taken versions change nothing" "${TODAY}.0" "$RESULT"

export NPM_MOCK_RESPONSES='{"@social-harness/social-harness":"0.0.0-bootstrap.0"}'
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "bootstrap starts a calendar history" "${TODAY}.0" "$RESULT"
export NPM_MOCK_RESPONSES='{"@social-harness/social-harness":{"exit":0,"body":""}}'
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "bootstrap without latest starts a calendar history" "${TODAY}.0" "$RESULT"
export NPM_MOCK_RESPONSES="{\"@social-harness/social-harness\":{\"exit\":0,\"body\":\"\"},\"@social-harness/openclaw-channel\":[\"${TODAY}.2\"]}"
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" . plugins/openclaw)
assert_eq "bootstrap preserves the sibling counter" "${TODAY}.3" "$RESULT"

echo "--- Test 2: One existing version today ---"
export NPM_MOCK_RESPONSES="{\"@social-harness/social-harness\":[\"${TODAY}.0\"]}"
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "one existing version" "${TODAY}.1" "$RESULT"

echo "--- Test 3: Multiple versions today ---"
export NPM_MOCK_RESPONSES="{\"@social-harness/social-harness\":[\"${TODAY}.0\", \"${TODAY}.1\", \"${TODAY}.2\"]}"
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "multiple versions" "${TODAY}.3" "$RESULT"

echo "--- Test 4: Mixed old and today versions ---"
export NPM_MOCK_RESPONSES="{\"@social-harness/social-harness\":[\"2026.101.0\", \"2026.101.1\", \"${TODAY}.0\"]}"
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "mixed versions" "${TODAY}.1" "$RESULT"

echo "--- Test 5: npm returns single string ---"
export NPM_MOCK_RESPONSES="{\"@social-harness/social-harness\":\"${TODAY}.0\"}"
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "single string response" "${TODAY}.1" "$RESULT"

echo "--- Test 6: Only old versions ---"
export NPM_MOCK_RESPONSES='{"@social-harness/social-harness":["2026.101.0", "2026.102.0"]}'
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "only old versions" "${TODAY}.0" "$RESULT"

echo "--- Test 7: npm 404 (deleted package) ---"
export NPM_MOCK_RESPONSES='{}'
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" .)
assert_eq "npm 404" "${TODAY}.0" "$RESULT"

echo "--- Test 7b: npm failure that is not a 404 aborts ---"
export NPM_MOCK_RESPONSES='{"@social-harness/social-harness":{"exit":1,"body":"{\"error\":{\"code\":\"E503\"}}"}}'
set +e
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" . 2>/dev/null)
STATUS=$?
set -e
assert_eq "registry error exits 1" "1" "$STATUS"
assert_eq "registry error prints no version" "" "$RESULT"

echo "--- Test 7c: npm failure with no JSON body aborts ---"
export NPM_MOCK_RESPONSES='{"@social-harness/social-harness":{"exit":1,"body":""}}'
set +e
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" . 2>/dev/null)
STATUS=$?
set -e
assert_eq "empty-body failure exits 1" "1" "$STATUS"

echo "--- Test 8: One version over the union of the released histories ---"
export NPM_MOCK_RESPONSES="{\"@social-harness/social-harness\":[\"${TODAY}.0\",\"${TODAY}.2\"],\"@social-harness/openclaw-channel\":[\"${TODAY}.1\"]}"
# shellcheck disable=SC2086 -- the package list is deliberately word-split.
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" $RELEASED)
assert_eq "union takes the highest counter across packages" "${TODAY}.3" "$RESULT"

echo "--- Test 9: Never-published packages (mock 404) alongside published ones ---"
export NPM_MOCK_RESPONSES="{\"@social-harness/social-harness\":[\"${TODAY}.0\"]}"
# shellcheck disable=SC2086
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" $RELEASED)
assert_eq "404 packages do not block a published sibling's counter" "${TODAY}.1" "$RESULT"

echo "--- Test 10: Every package unpublished ---"
export NPM_MOCK_RESPONSES='{}'
# shellcheck disable=SC2086
RESULT=$("$SCRIPT_DIR/compute-next-version.sh" $RELEASED)
assert_eq "all-404 union starts at build 0" "${TODAY}.0" "$RESULT"

echo "--- Test 11: No package argument is an error ---"
set +e
"$SCRIPT_DIR/compute-next-version.sh" >/dev/null 2>&1
STATUS=$?
set -e
assert_eq "missing arguments exit 2" "2" "$STATUS"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
