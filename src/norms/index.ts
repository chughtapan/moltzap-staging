/**
 * @file A daemon's norms. A deployment's norms directory holds the norm
 * documents, compiled to the contracts the firewall checks.
 */

import { FileSystem } from "@effect/platform";
import { Data, Effect } from "effect";
import { join } from "node:path";
import type { CompiledContract } from "../firewall/index.js";
import { compareAscii } from "../encoding.js";
import { compileNorm } from "./compiler.js";

/** The settings' copy of a norms directory: the contracts the firewall checks. */
export interface InstalledNorms {
  readonly contracts: readonly CompiledContract[];
}

/** A norms directory the daemon cannot use, and why. */
export class NormsConfigurationError extends Data.TaggedError(
  "NormsConfigurationError",
)<{ readonly reason: string }> {
  override get message(): string {
    return `The norms directory is invalid: ${this.reason}`;
  }
}

/**
 * Compiles every `.lisp` norm document directly in `directory`, in name
 * order. Norm names are unique.
 */
export const loadNorms = Effect.fn("loadNorms")(
  function* (directory: string) {
    const fs = yield* FileSystem.FileSystem;
    const files = yield* fs.readDirectory(directory);
    const contracts = yield* Effect.forEach(
      files.filter((file) => file.endsWith(".lisp")).sort(compareAscii),
      (file) =>
        fs
          .readFileString(join(directory, file))
          .pipe(Effect.flatMap(compileNorm)),
      { concurrency: 1 },
    );
    if (
      new Set(contracts.map((contract) => contract.name)).size !==
      contracts.length
    ) {
      return yield* new NormsConfigurationError({
        reason: "two documents name the same norm",
      });
    }
    return { contracts } satisfies InstalledNorms;
  },
  Effect.catchTags({
    BadArgument: (error) =>
      Effect.fail(new NormsConfigurationError({ reason: error.message })),
    SystemError: (error) =>
      Effect.fail(new NormsConfigurationError({ reason: error.message })),
    NormDocumentError: (error) =>
      Effect.fail(new NormsConfigurationError({ reason: error.detail })),
  }),
);

/** Whether the contracts have English rules, which need a decision provider. */
export function initialNormsHaveRules(installed: InstalledNorms): boolean {
  return installed.contracts.some((contract) => contract.textRules.length > 0);
}
