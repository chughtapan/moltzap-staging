/** @file The NanoClaw build downloads only what its pins name and refuses bytes whose sha256 differs. */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import {
  buildxDriver,
  downloadVerified,
  NANOCLAW_BASE_IMAGE,
  parseBuildArguments,
  requirePinnedBase,
} from "./build.ts";

const BODY = Buffer.from("export {};\n");
const BODY_SHA256 = createHash("sha256").update(BODY).digest("hex");

test("a download whose sha256 matches is returned whole", async () => {
  const body = await downloadVerified(
    "https://example.test/f",
    BODY_SHA256,
    fetchAnswering(200),
  );
  assert.deepEqual(body, BODY);
});

test("a download whose sha256 differs is refused", async () => {
  await assert.rejects(
    downloadVerified(
      "https://example.test/f",
      "0".repeat(64),
      fetchAnswering(200),
    ),
    new RegExp(
      `sha256 of https://example\\.test/f is ${BODY_SHA256}, expected 0{64}`,
      "u",
    ),
  );
});

test("a failed download is refused before hashing", async () => {
  await assert.rejects(
    downloadVerified(
      "https://example.test/f",
      BODY_SHA256,
      fetchAnswering(404),
    ),
    /download of https:\/\/example\.test\/f failed: 404/u,
  );
});

test("a tag naming an image reference builds that image", () => {
  assert.deepEqual(parseBuildArguments(["--tag", "nanoclaw-agent:build"]), {
    mode: "build",
    tag: "nanoclaw-agent:build",
  });
});

test("stage with a directory stages the verified inputs there", () => {
  assert.deepEqual(parseBuildArguments(["stage", "/inputs"]), {
    mode: "stage",
    directory: "/inputs",
  });
});

test("lock alone resolves the lockfile", () => {
  assert.deepEqual(parseBuildArguments(["lock"]), { mode: "lock" });
});

test("any other arguments are refused", () => {
  assert.throws(() => parseBuildArguments([]), TypeError);
  assert.throws(() => parseBuildArguments(["--tag", "untagged"]), TypeError);
  assert.throws(
    () => parseBuildArguments(["--tag", "a:b", "--push"]),
    TypeError,
  );
  assert.throws(() => parseBuildArguments(["stage"]), TypeError);
  assert.throws(() => parseBuildArguments(["lock", "extra"]), TypeError);
  assert.throws(
    () => parseBuildArguments(["stage", "/inputs", "--tag", "a:b"]),
    TypeError,
  );
});

test("the builder's driver is read from buildx inspect", () => {
  assert.equal(buildxDriver("Name: default\nDriver: docker\n"), "docker");
  assert.throws(() => buildxDriver("Name: default\n"), /reported no driver/u);
});

test("a stock Dockerfile that builds only from the pinned base is accepted", () => {
  assert.doesNotThrow(() => {
    requirePinnedBase(`FROM ${NANOCLAW_BASE_IMAGE.name}\nRUN true\n`);
  });
});

test("a stock Dockerfile that builds from an unpinned image is refused", () => {
  assert.throws(() => {
    requirePinnedBase("FROM unpinned:latest\n");
  }, /builds FROM unpinned:latest/u);
  assert.throws(() => {
    requirePinnedBase(
      `FROM ${NANOCLAW_BASE_IMAGE.name}\nFROM unpinned:latest AS tools\n`,
    );
  }, /unpinned:latest/u);
  assert.throws(() => {
    requirePinnedBase("RUN true\n");
  }, /FROM nothing/u);
});

/** A fetch that answers every URL with BODY and `status`. */
function fetchAnswering(status: number): typeof fetch {
  return () =>
    Promise.resolve(
      new Response(BODY, {
        status,
        statusText: status === 200 ? "OK" : "Not Found",
      }),
    );
}
