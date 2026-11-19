/** @file Verifies packed service entries, the installed `sharnessd` command and explicit firewall assets in an isolated consumer. */
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

/** An installed consumer receives the shipped message policy unchanged. */
const policyAssetScript = `
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const expected = await readFile(${JSON.stringify(`${workspaceRoot}/config/firewall/message-policy.json`)}, "utf8");
const actual = await readFile(fileURLToPath(new URL("../config/firewall/message-policy.json", import.meta.resolve("@social-harness/social-harness"))), "utf8");
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
    const imports = yield* publicImports();
    yield* runCommand(
      process.execPath,
      ["--input-type=module", "-e", imports],
      {
        cwd: consumerRoot,
        env: { NODE_PATH: "" },
      },
    );
    yield* verifyCommand(consumerRoot);
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
