/**
 * @file Proves the packed `@social-harness/openclaw-channel` works in the OpenClaw
 * release it is tested against, the way a deployment uses it.
 *
 * It packs the channel's closure as a release does, installs the tarballs and
 * that OpenClaw into an isolated consumer, and checks three behaviours: the
 * package typechecks for a TypeScript consumer, OpenClaw loads it from its
 * install path and registers the `sharness` channel, and OpenClaw serves every
 * skill the packed Social Harness package ships to the model. Every check goes
 * through the channel's public entry point or OpenClaw's CLI, so a new
 * OpenClaw release changes only the `openclaw` entry of the
 * `pnpm-workspace.yaml` catalog.
 */
import { FileSystem } from "@effect/platform";
import { Effect, Schema } from "effect";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  checkConsumerTypes,
  type GateError,
  type GateServices,
  installPackedConsumer,
  PackGateError,
  packProductClosure,
  readConsumerPins,
  readJsonFile,
  readText,
  requireCondition,
  runCommand,
  runGate,
} from "./test/packed-workspace.ts";

/** Runs one OpenClaw CLI command that prints JSON and decodes its output. */
type OpenClawCli = <A, I>(
  args: readonly string[],
  schema: Schema.Schema<A, I>,
) => Effect.Effect<A, GateError, GateServices>;

const jsonObject = Schema.Record({ key: Schema.String, value: Schema.Unknown });
const openClawManifest = Schema.Struct({
  scripts: Schema.Struct({ postinstall: Schema.String }),
});
const pluginList = Schema.Struct({ plugins: Schema.Array(jsonObject) });

const workspaceRoot = dirname(dirname(fileURLToPath(import.meta.url)));

runGate("sharness-openclaw-pack-", (temporaryRoot) =>
  Effect.gen(function* () {
    const { archives } = yield* packProductClosure(
      workspaceRoot,
      "plugins/openclaw",
      temporaryRoot,
    );
    const pins = yield* readConsumerPins(workspaceRoot);
    /** The OpenClaw release the channel is built and tested against. */
    const openclawVersion = pins.openclaw;
    const consumerRoot = yield* installPackedConsumer({
      temporaryRoot,
      name: "sharness-openclaw-packed-consumer",
      archives,
      dependencies: { effect: pins.effect, openclaw: openclawVersion },
      devDependencies: { typescript: pins.typescript },
    });
    yield* verifyTypes(consumerRoot);
    const { openclaw } = yield* pluginHost(consumerRoot);
    yield* verifyPluginLoaded(openclaw, openclawVersion);
    yield* verifySkillsServed(openclaw, consumerRoot);
    return `OpenClaw ${openclawVersion} packed consumer check passed`;
  }),
);

/**
 * Requires that a TypeScript consumer compiles against the packed entry point.
 * @param consumerRoot Directory the packed closure is installed in.
 */
function verifyTypes(
  consumerRoot: string,
): Effect.Effect<void, GateError, GateServices> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.writeFileString(
      join(consumerRoot, "tsconfig.json"),
      `${JSON.stringify({
        compilerOptions: {
          module: "NodeNext",
          moduleResolution: "NodeNext",
          noEmit: true,
          skipLibCheck: false,
          strict: true,
          target: "ES2023",
        },
        include: ["check.ts"],
      })}\n`,
    );
    yield* fs.writeFileString(
      join(consumerRoot, "check.ts"),
      'import plugin from "@social-harness/openclaw-channel";\nconst pluginId: string = plugin.id;\nvoid pluginId;\n',
    );
    yield* checkConsumerTypes(consumerRoot);
  });
}

/**
 * Configure an isolated OpenClaw host that loads the channel by path, as the
 * agent image does. The consumer install skipped every lifecycle script, and
 * OpenClaw's CLI refuses to start from a package whose postinstall never ran,
 * so this runs OpenClaw's own. The pnpm install leaves package files as hard
 * links into its store, and OpenClaw silently skips a plugin whose files are
 * hard links, so the channel's files are rewritten as plain files first, as
 * an `npm` install leaves them.
 * @param consumerRoot Directory the packed closure is installed in.
 * @returns A runner for OpenClaw CLI commands that print JSON.
 */
function pluginHost(
  consumerRoot: string,
): Effect.Effect<{ readonly openclaw: OpenClawCli }, GateError, GateServices> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const pluginRoot = yield* fs.realPath(
      join(consumerRoot, "node_modules", "@social-harness", "openclaw-channel"),
    );
    const openclawRoot = yield* fs.realPath(
      join(consumerRoot, "node_modules", "openclaw"),
    );
    yield* unlinkPluginFiles(pluginRoot);
    const { scripts } = yield* readJsonFile(
      join(openclawRoot, "package.json"),
      openClawManifest,
    );
    yield* runCommand("sh", ["-c", scripts.postinstall], { cwd: openclawRoot });
    const stateRoot = join(consumerRoot, "openclaw-state");
    const configPath = join(stateRoot, "openclaw.json");
    yield* writeHostConfig(stateRoot, configPath, pluginRoot);
    const env = {
      HOME: stateRoot,
      OPENCLAW_CONFIG_PATH: configPath,
      OPENCLAW_HOME: stateRoot,
      OPENCLAW_STATE_DIR: stateRoot,
    };
    const openclaw: OpenClawCli = (args, schema) =>
      runCommand(
        process.execPath,
        [join(openclawRoot, "openclaw.mjs"), ...args, "--json"],
        { cwd: consumerRoot, env },
      ).pipe(
        Effect.flatMap((stdout) =>
          Schema.decodeUnknown(Schema.parseJson(schema))(stdout),
        ),
      );
    return { openclaw };
  });
}

/**
 * Rewrite every file under `root` as a plain file, replacing the hard link
 * pnpm installed.
 * @param root Directory to rewrite.
 */
function unlinkPluginFiles(
  root: string,
): Effect.Effect<void, GateError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const entries = yield* fs.readDirectory(root, { recursive: true });
    yield* Effect.forEach(
      entries.map((entry) => join(root, entry)),
      (file) =>
        Effect.gen(function* () {
          const info = yield* fs.stat(file);
          if (info.type === "File") {
            const content = yield* fs.readFile(file);
            yield* fs.remove(file);
            yield* fs.writeFile(file, content);
          }
        }),
      { concurrency: 1, discard: true },
    );
  });
}

function writeHostConfig(
  stateRoot: string,
  configPath: string,
  pluginRoot: string,
): Effect.Effect<void, GateError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.makeDirectory(stateRoot);
    yield* fs.writeFileString(
      configPath,
      `${JSON.stringify({
        channels: { sharness: { accounts: [{ id: "packed-agent" }] } },
        plugins: {
          load: { paths: [pluginRoot] },
          entries: { "social-harness": { enabled: true } },
        },
      })}\n`,
    );
  });
}

function verifyPluginLoaded(
  openclaw: OpenClawCli,
  openclawVersion: string,
): Effect.Effect<void, GateError, GateServices> {
  return Effect.gen(function* () {
    const { plugins } = yield* openclaw(["plugins", "list"], pluginList);
    const plugin = plugins.find(
      (candidate) => candidate.id === "social-harness",
    );
    yield* requireCondition(
      plugin?.enabled === true &&
        plugin.status === "loaded" &&
        JSON.stringify(plugin.channelIds) === JSON.stringify(["sharness"]),
      `expected OpenClaw ${openclawVersion} to load the channel and register "sharness", got ${JSON.stringify(plugin)}`,
    );
  });
}

/**
 * Requires that OpenClaw shows the model every skill the packed
 * `@social-harness/social-harness` ships, with that package's text: the
 * channel installs its skills rather than its own.
 * @param openclaw The isolated OpenClaw CLI.
 * @param consumerRoot Directory the packed closure is installed in.
 */
function verifySkillsServed(
  openclaw: OpenClawCli,
  consumerRoot: string,
): Effect.Effect<void, GateError, GateServices> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const skillsRoot = join(
      yield* fs.realPath(
        join(consumerRoot, "node_modules", "@social-harness", "social-harness"),
      ),
      "skills",
    );
    const names = yield* fs.readDirectory(skillsRoot);
    yield* requireCondition(
      names.length > 0,
      "expected the packed Social Harness package to ship its skills",
    );
    yield* Effect.forEach(
      names,
      (name) => verifySkillServed(openclaw, skillsRoot, name),
      { concurrency: 1, discard: true },
    );
  });
}

function verifySkillServed(
  openclaw: OpenClawCli,
  skillsRoot: string,
  name: string,
): Effect.Effect<void, GateError, GateServices> {
  return Effect.gen(function* () {
    const skill = yield* openclaw(["skills", "info", name], jsonObject);
    yield* requireCondition(
      skill.eligible === true && skill.modelVisible === true,
      `expected OpenClaw to show the ${name} skill to the model, got ${JSON.stringify(skill)}`,
    );
    const servedPath = skill.filePath;
    if (typeof servedPath !== "string") {
      return yield* new PackGateError({
        message: `expected OpenClaw to name the served skill file, got ${JSON.stringify(skill)}`,
      });
    }
    const [served, packed] = yield* Effect.all(
      [
        readText(servedPath, `OpenClaw serves a missing skill ${servedPath}`),
        readText(
          join(skillsRoot, name, "SKILL.md"),
          `packed Social Harness package ${name} skill is missing`,
        ),
      ],
      { concurrency: 2 },
    );
    yield* requireCondition(
      served === packed,
      `expected OpenClaw to serve the packed Social Harness package ${name} skill text`,
    );
  });
}
