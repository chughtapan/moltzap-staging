/**
 * @file A daemon's norms. A deployment's norms directory holds the norm
 * documents, compiled to the contracts its firewall's structure check applies.
 */

import { FileSystem } from "@effect/platform";
import { Clock, Data, Effect, Struct } from "effect";
import { join } from "node:path";
import { compareAscii } from "../encoding.js";
import { type StructureCheck, TextRule } from "../firewall/index.js";
import { compileNorm } from "./compiler.js";
import { type CompiledContract, formatAct } from "./contract.js";
import { fitAct, projectProtocol, readHeldAct } from "./protocol.js";

/** The settings' copy of a norms directory: its compiled contracts. */
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

/**
 * The structure check of a firewall whose agent holds `installed`. It replays
 * the earlier messages the firewall allowed to find the instance in progress,
 * then reads the message as an act of that instance or, when none is, of the
 * norm it names. An act that fits passes with its norm's English rules for
 * it. Any other message is refused under a rule named for the norm of the
 * instance in progress and the stage that failed. Without contracts every
 * message passes.
 */
export function structureCheck(installed: InstalledNorms): StructureCheck {
  const { contracts } = installed;
  return Effect.fn("structureCheck")(function* (message, earlier) {
    if (contracts.length === 0) {
      return { kind: "passed", textRules: [] };
    }
    const view = yield* projectProtocol(contracts, earlier);
    const now = yield* Clock.currentTimeMillis;
    let selected: CompiledContract | undefined;
    return yield* readHeldAct(view, contracts, message).pipe(
      Effect.tap((held) =>
        Effect.sync(() => {
          selected = held.contract;
        }),
      ),
      Effect.flatMap((held) =>
        Effect.map(fitAct(view, held, message, now), (form) => ({
          ...held,
          form,
        })),
      ),
      Effect.match({
        onFailure: (problem) => {
          const reply = refusalReply(view.instance?.contract)?.(problem.detail);
          return {
            kind: "refused" as const,
            reason: problem.reason,
            ruleId: `${view.instance?.contract.name ?? "norm"}-${problem.stage}`,
            feedback: problem.detail,
            detail: {
              ...(selected === undefined
                ? {}
                : { norm: selected.name, digest: selected.digest }),
              stage: problem.stage,
              explanation: problem.detail,
              ...(problem.field === undefined ? {} : { field: problem.field }),
            },
            ...(reply === undefined ? {} : { reply }),
          };
        },
        onSuccess: ({ act, contract, form }) => {
          const norm = { norm: contract.name, digest: contract.digest };
          const replyTo = refusalReply(contract);
          return {
            kind: "passed" as const,
            textRules: contract.textRules
              .filter((rule) => rule.act === undefined || rule.act === act.name)
              .map(
                (rule) =>
                  new TextRule({
                    ...Struct.omit(rule, "act"),
                    applies: "both",
                  }),
              ),
            protocol: {
              ...norm,
              act: act.name,
              description: contract.description,
              state: [...(view.instance?.facts.keys() ?? [])],
              form: form.fields,
            },
            detail: { ...norm, act: act.name, stage: "semantic" },
            ...(replyTo === undefined ? {} : { replyTo }),
          };
        },
      }),
    );
  });
}

/**
 * Phrases the act that tells the sender of a withheld message why, when
 * `contract` declares one in `:on-refusal` with a `reason` field.
 */
function refusalReply(contract?: CompiledContract) {
  const act = contract?.onRefusal;
  const form = contract?.forms.find((declared) => declared.act === act);
  if (
    contract === undefined ||
    act === undefined ||
    form === undefined ||
    !Object.hasOwn(form.fields.properties, "reason")
  ) {
    return undefined;
  }
  return (reason: string) =>
    formatAct({ name: act, norm: contract.name, fields: { reason } });
}
