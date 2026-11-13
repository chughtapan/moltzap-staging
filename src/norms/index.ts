/** @file Loads pre-agreed documents and compiles their firewall contracts. */
import { FileSystem } from "@effect/platform";
import { Context, Data, Effect, Layer, Schema } from "effect";
import { join } from "node:path";
import { exactStruct } from "../encoding.js";
import {
  type CompiledContract,
  compiledContractSchema,
  type FirewallPolicyError,
} from "../firewall/index.js";
import { Firewall } from "../firewall/service.js";
import { compileNorm } from "./compiler.js";

/** Validated initial contracts shared by new agents of this daemon. */
export const installedNorms = exactStruct({
  contracts: Schema.Array(compiledContractSchema),
});
/** Loaded configuration retains the immutable source and compiled forms. */
export type InstalledNorms = typeof installedNorms.Type;
/** Reads the contracts currently held by the agent's firewall. */
export class HeldNorms extends Context.Tag("SocialHarness/HeldNorms")<
  HeldNorms,
  {
    readonly read: () => Effect.Effect<
      readonly CompiledContract[],
      FirewallPolicyError
    >;
  }
>() {
  /** Exposes the current policy, including edits made after startup. */
  static readonly layer = Layer.effect(
    HeldNorms,
    Effect.map(Firewall, (firewall) => ({ read: firewall.readContracts })),
  );
}
/** The directory could not be loaded or contains an invalid document. */
export class NormsConfigurationError extends Data.TaggedError(
  "NormsConfigurationError",
)<{ readonly reason: string }> {
  override get message() {
    return `The norms directory is invalid: ${this.reason}`;
  }
}
/** Every Lisp document in the directory configures a pre-agreed contract. */
export const loadNorms = Effect.fn("loadNorms")(
  function* (directory: string) {
    const fs = yield* FileSystem.FileSystem;
    const files = yield* fs.readDirectory(directory);
    const contracts = yield* Effect.forEach(
      files
        .filter((file) => file.endsWith(".lisp"))
        .sort((a, b) => a.localeCompare(b)),
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
    return { contracts };
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
/** English rules require a configured decision provider. */
export const initialNormsHaveRules = (installed: InstalledNorms) =>
  installed.contracts.some((contract) => contract.semanticRules.length > 0);

/** Translates one document to an immutable firewall contract. */
export { compileNorm } from "./compiler.js";
