/** @file Reading the manifest a package tarball carries from its bytes. */

import { effect as it } from "@effect/vitest";
import { Effect, Exit } from "effect";
import { gzipSync } from "node:zlib";
import { describe, expect } from "vitest";
import { readPackedManifest } from "./packed-manifest.js";

/** A gzipped ustar archive holding `files`, as `npm pack` lays one out. */
const tarball = (files: Readonly<Record<string, string>>): Uint8Array => {
  const blocks: Uint8Array[] = [];
  for (const [name, text] of Object.entries(files)) {
    const body = new TextEncoder().encode(text);
    const header = new Uint8Array(512);
    header.set(new TextEncoder().encode(name), 0);
    header.set(
      new TextEncoder().encode(body.length.toString(8).padStart(11, "0")),
      124,
    );
    header[156] = 0x30;
    blocks.push(
      header,
      body,
      new Uint8Array((512 - (body.length % 512)) % 512),
    );
  }
  blocks.push(new Uint8Array(1024));
  return gzipSync(Buffer.concat(blocks));
};

const manifest = {
  name: "@moltzap/social-harness",
  version: "2026.1025.1",
  dependencies: { "@moltzap/social-harness": "2026.1025.1" },
};

// @agent-code-guard/regression-only: The tarball layout is npm's, pinned by its examples.
describe("a packed manifest", () => {
  it("is read from package/package.json, past the other entries", () =>
    Effect.gen(function* () {
      const read = yield* readPackedManifest(
        tarball({
          "package/README.md": "x".repeat(700),
          "package/package.json": JSON.stringify(manifest),
        }),
      );
      expect(read).toMatchObject(manifest);
    }));

  it("is missing from a tarball without one, or from bytes that are not a tarball", () =>
    Effect.gen(function* () {
      for (const bytes of [
        tarball({ "package/index.js": "" }),
        new TextEncoder().encode("not gzip"),
      ]) {
        const exit = yield* Effect.exit(readPackedManifest(bytes));
        expect(Exit.isFailure(exit)).toBe(true);
      }
    }));
});
