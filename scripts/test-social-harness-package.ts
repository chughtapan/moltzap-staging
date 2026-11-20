/** @file Verifies the installed `sharnessd` command and the shipped message policy in an isolated consumer of the packed package. */
import { Effect } from "effect";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  installPackedConsumer,
  packProductClosure,
  readConsumerPins,
  requireCondition,
  runCommand,
  runGate,
} from "./packed-workspace.ts";

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));

/** An installed consumer receives the shipped message policy unchanged. */
const policyAssetScript = `
import { readFile } from "node:fs/promises";

const expected = await readFile(${JSON.stringify(`${workspaceRoot}/config/firewall/message-policy.json`)}, "utf8");
const actual = await readFile("node_modules/@social-harness/social-harness/config/firewall/message-policy.json", "utf8");
if (actual !== expected) throw new Error("packed message policy changed");
`;

runGate("sharness-social-harness-pack-", (temporaryRoot) =>
  Effect.gen(function* () {
    const { archives } = yield* packProductClosure(
      workspaceRoot,
      ".",
      temporaryRoot,
    );
    const pins = yield* readConsumerPins(workspaceRoot);
    const consumerRoot = yield* installPackedConsumer({
      temporaryRoot,
      name: "sharness-daemon-packed-consumer",
      archives,
      dependencies: { effect: pins.effect },
    });
    yield* verifyCommand(consumerRoot);
    yield* verifyPolicyAssets(consumerRoot);
    return "social-harness package consumer check passed";
  }),
);

/** The command the package installs runs from the consumer and prints its usage. */
function verifyCommand(consumerRoot: string) {
  return runCommand(
    join(consumerRoot, "node_modules", ".bin", "sharnessd"),
    ["--help"],
    { cwd: consumerRoot, env: { NODE_PATH: "" } },
  ).pipe(
    Effect.flatMap((printed) =>
      requireCondition(
        printed.includes("Usage: sharnessd"),
        `expected the installed sharnessd --help to print its usage, got:\n${printed}`,
      ),
    ),
  );
}

/** The installed package retains the shipped message policy. */
function verifyPolicyAssets(consumerRoot: string) {
  return runCommand(
    process.execPath,
    ["--input-type=module", "-e", policyAssetScript],
    { cwd: consumerRoot, env: { NODE_PATH: "" } },
  );
}
