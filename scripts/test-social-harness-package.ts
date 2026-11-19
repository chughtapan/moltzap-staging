/** @file Verifies packed service entries, the installed `sharnessd` and `sharness-mcp` commands and explicit firewall assets in an isolated consumer. */
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
  workspacePackages,
} from "./packed-workspace.ts";

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));

/** A host that imports the main entry for its tool contracts and client loads no database driver. */
const hostEntryScript = `
import { registerHooks } from "node:module";

const loaded = [];
registerHooks({
  resolve(specifier, context, next) {
    const found = next(specifier, context);
    loaded.push(found.url);
    return found;
  },
});
await import("@social-harness/social-harness");
const drivers = loaded.filter((url) =>
  ["@effect/sql-pg", "kysely", "jose"].some((name) =>
    url.includes("/node_modules/" + name + "/"),
  ),
);
if (drivers.length > 0) throw new Error("the main entry loaded " + drivers.join(", "));
`;

/** An installed consumer receives the selected preset and every shipped rule unchanged. */
const policyAssetScript = `
import { readFile, readdir } from "node:fs/promises";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const sourceRoot = ${JSON.stringify(`${workspaceRoot}/config/firewall`)};
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
    const imports = yield* publicImports();
    yield* runCommand(
      process.execPath,
      ["--input-type=module", "-e", imports],
      {
        cwd: consumerRoot,
        env: { NODE_PATH: "" },
      },
    );
    yield* verifyCommand(consumerRoot, "sharnessd");
    yield* verifyCommand(consumerRoot, "sharness-mcp");
    yield* runCommand(
      process.execPath,
      ["--input-type=module", "-e", hostEntryScript],
      { cwd: consumerRoot, env: { NODE_PATH: "" } },
    );
    yield* verifyPolicyAssets(consumerRoot);
    return "social-harness package consumer check passed";
  }),
);

/** Every entry the source manifest names, as a consumer imports it. */
function publicImports() {
  return workspacePackages(workspaceRoot).pipe(
    Effect.map((packages) =>
      packages
        .filter(({ name }) => name === "@social-harness/social-harness")
        .flatMap(({ name, exports }) =>
          Object.keys(exports).map((subpath) =>
            subpath === "." ? name : `${name}/${subpath.slice(2)}`,
          ),
        )
        .map((specifier) => `await import(${JSON.stringify(specifier)});`)
        .join("\n"),
    ),
  );
}

/** A command the package installs runs from the consumer and prints its usage. */
function verifyCommand(consumerRoot: string, name: string) {
  return runCommand(
    join(consumerRoot, "node_modules", ".bin", name),
    ["--help"],
    { cwd: consumerRoot, env: { NODE_PATH: "" } },
  ).pipe(
    Effect.flatMap((printed) =>
      requireCondition(
        printed.includes(`Usage: ${name}`),
        `expected the installed ${name} --help to print its usage, got:\n${printed}`,
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
