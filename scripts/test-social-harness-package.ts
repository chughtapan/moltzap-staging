/** @file Verifies packed service entries, the installed `sharnessd` command and explicit firewall assets in an isolated consumer. */
import { Effect } from "effect";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { workspacePackages } from "./repo/packages.ts";
import {
  installPackedConsumer,
  packProductClosure,
  readConsumerPins,
  requireCondition,
  runCommand,
  runGate,
} from "./test/packed-workspace.ts";

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));

/** An installed consumer receives the selected preset and every shipped rule unchanged. */
const policyAssetScript = `
import { readFile, readdir } from "node:fs/promises";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const sourceRoot = ${JSON.stringify(`${workspaceRoot}/packages/social-harness/config/firewall`)};
const installedRoot = fileURLToPath(new URL("../config/firewall/", import.meta.resolve("@social-harness/social-harness")));
const entries = await readdir(sourceRoot, { recursive: true, withFileTypes: true });
const rules = entries.filter((entry) => entry.isFile() && entry.name === "BEHAVIOR.md");
if (rules.length === 0) throw new Error("source policy assets contain no rules");
const assets = ["message-policy.json", ...rules.map((rule) =>
  relative(sourceRoot, join(rule.parentPath, rule.name)),
)];
for (const asset of assets) {
  const expected = await readFile(join(sourceRoot, asset), "utf8");
  const actual = await readFile(join(installedRoot, asset), "utf8");
  if (actual !== expected) throw new Error("packed policy asset changed: " + asset);
}
`;

runGate("sharness-social-harness-pack-", (temporaryRoot) =>
  Effect.gen(function* () {
    const { archives } = yield* packProductClosure(
      workspaceRoot,
      "packages/social-harness",
      temporaryRoot,
    );
    const pins = yield* readConsumerPins(workspaceRoot);
    const consumerRoot = yield* installPackedConsumer({
      temporaryRoot,
      name: "sharness-daemon-packed-consumer",
      archives,
      dependencies: { effect: pins.effect },
    });
    const imports = publicImports();
    yield* runCommand(
      process.execPath,
      ["--input-type=module", "-e", imports],
      {
        cwd: consumerRoot,
        env: { NODE_PATH: "" },
      },
    );
    yield* runCommand(
      process.execPath,
      [
        "--conditions=sharness-test",
        "--input-type=module",
        "-e",
        'await import("@social-harness/social-harness/test");',
      ],
      {
        cwd: consumerRoot,
        env: { NODE_PATH: "" },
      },
    );
    yield* verifyCommand(consumerRoot);
    yield* verifyPolicyAssets(consumerRoot);
    return "social-harness package consumer check passed";
  }),
);

/** Source manifests select the entries an ordinary consumer can import. */
function publicImports(): string {
  return workspacePackages(workspaceRoot)
    .filter(({ name }) => name === "@social-harness/social-harness")
    .flatMap(({ name, exports }) =>
      Object.entries(exports)
        .filter(
          ([, target]) =>
            !(
              typeof target === "object" &&
              target !== null &&
              "sharness-test" in target
            ),
        )
        .map(([subpath]) =>
          subpath === "." ? name : `${name}/${subpath.slice(2)}`,
        ),
    )
    .map((specifier) => `await import(${JSON.stringify(specifier)});`)
    .join("\n");
}

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

/** The installed package retains the preset and its shipped behavior documents. */
function verifyPolicyAssets(consumerRoot: string) {
  return runCommand(
    process.execPath,
    ["--input-type=module", "-e", policyAssetScript],
    { cwd: consumerRoot, env: { NODE_PATH: "" } },
  );
}
