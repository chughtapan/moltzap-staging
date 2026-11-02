/** @file Which build identity a release's provenance must be signed for. */

import { effect as it } from "@effect/vitest";
import { Effect, Exit } from "effect";
import { describe, expect } from "vitest";
import { launcherError } from "./errors.js";
import { signedByReleaseIdentity } from "./provenance.js";

const identity = (repository: string): string =>
  `https://github.com/${repository}/.github/workflows/publish.yml@refs/heads/main`;

/** A Sigstore stand-in that accepts a bundle only for `signer`. */
const signedFor =
  (signer: string) =>
  (candidate: string): Effect.Effect<void, string> =>
    candidate === signer
      ? Effect.void
      : Effect.fail("certificate identity mismatch");

const refuse = (why: string) => launcherError("provenance", why);

describe("a release's provenance signer", () => {
  it("is accepted from social-harness/moltzap's release workflow", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        signedByReleaseIdentity(
          signedFor(identity("social-harness/moltzap")),
          refuse,
        ),
      );
      expect(Exit.isSuccess(exit)).toBe(true);
    }));

  it("is refused from any other repository", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        signedByReleaseIdentity(
          signedFor(identity("someone/other-repository")),
          refuse,
        ),
      );
      expect(Exit.isFailure(exit)).toBe(true);
    }));
});
