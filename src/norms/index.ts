/**
 * @file A daemon's norms. A deployment's norms directory holds the norm
 * documents each new agent starts with, compiled to the contracts its firewall
 * checks. After that each agent holds its own contracts in its firewall's
 * editable policy, and a norm document an edit adds is compiled here first.
 */

import { FileSystem } from "@effect/platform";
import { Data, Effect } from "effect";
import { join } from "node:path";
import { compareAscii } from "../encoding.js";
import {
  type CompiledContract,
  FirewallPolicyError,
  type PolicyEdit,
  type policyUpdateSchema,
} from "../firewall/index.js";
import { compileNorm } from "./compiler.js";

/** The settings' copy of a norms directory: the contracts each new agent holds. */
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

/** Whether the contracts each new agent holds have English rules, which need a decision provider. */
export function initialNormsHaveRules(installed: InstalledNorms): boolean {
  return installed.contracts.some((contract) => contract.textRules.length > 0);
}

/**
 * The edit a policy update amounts to once each `{norm}` document it adds is
 * compiled to a contract. A document that does not compile fails the update
 * with `invalid-rule` at its place in `add`, without its text.
 */
export const compiledPolicyEdit = Effect.fn("compiledPolicyEdit")(function* (
  update: typeof policyUpdateSchema.Type,
) {
  const add = yield* Effect.forEach(
    update.add ?? [],
    (
      added,
      index,
    ): Effect.Effect<
      NonNullable<PolicyEdit["add"]>[number],
      FirewallPolicyError
    > =>
      "norm" in added
        ? compileNorm(added.norm).pipe(
            Effect.mapBoth({
              onFailure: () =>
                new FirewallPolicyError({
                  reason: "invalid-rule",
                  problem: "schema",
                  path: ["add", index],
                }),
              onSuccess: (contract) => ({ contract }),
            }),
          )
        : Effect.succeed(added),
    { concurrency: 1 },
  );
  return { ...update, add } satisfies PolicyEdit;
});
