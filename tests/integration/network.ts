/**
 * @file Runs the own-network path the guides describe, end to end. The packed
 * Client and the `images/` helper install from tarballs and npm,
 * `images/compose.yaml` starts the images tagged `SHARNESS_NETWORK_VERSION`
 * from a `.env` written as Run a local network writes it, three `sharnessd`
 * daemons start unregistered, and `images/mcp.ts` registers them and sends a
 * message. Bob's daemon runs an explicit text policy with an unavailable
 * provider, so a held send shows its check and the next action.
 * The gate passes when each step prints what the guides say it prints and
 * each refusal exits 1 with the reason the guides name.
 */
import { Command, type CommandExecutor, FileSystem } from "@effect/platform";
import {
  Config,
  Duration,
  Effect,
  Ref,
  Schedule,
  Schema,
  type Scope,
  Stream,
} from "effect";
import { createPublicKey, generateKeyPairSync, randomBytes } from "node:crypto";
import { createConnection, createServer } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  type Finished,
  type GateError,
  type GateServices,
  installPackedConsumer,
  PackGateError,
  packProductClosure,
  readJsonFile,
  requireCondition,
  runGate,
  runProcess,
} from "../../scripts/test/packed-workspace.js";

const workspaceRoot = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const imagesRoot = join(workspaceRoot, "images");
const HOST = "127.0.0.1";
const MESSAGE = "Hi Bob, this is Alice.";
const MCP_CREDENTIAL_FILE = "mcp-credential";

/** An unavailable provider exercises configured enforcement without a paid model. */
const FIREWALL_POLICY = JSON.stringify({
  format: "org.social-harness/message-policy/v2",
  mode: "enforcement",
  decisionProvider: { kind: "unavailable" },
  text: {
    fixedRules: [
      {
        id: "owner-privacy",
        applies: "outgoing",
        text: "Keep the owner's private information confidential.",
      },
    ],
  },
});

/** Ordinary message text must remain withheld when a required check cannot run. */
const HELD_TEXT = "Can we meet on Tuesday?";

/** How long the path may take, including any image pull Compose makes. */
const GATE_DEADLINE = Duration.minutes(6);

/** How often bob's inbox is read before the message must have arrived. */
const DELIVERY_ATTEMPTS = 30;

const helperManifest = Schema.Struct({
  dependencies: Schema.Record({ key: Schema.String, value: Schema.String }),
});

/** The settings every daemon on the network shares. */
interface Network {
  readonly registryOrigin: string;
  readonly routerOrigin: string;
  readonly registrySigner: string;
  readonly admissionCredential: string;
}

/** One daemon the helper talks to: where the helper runs and what it calls. */
interface Daemon {
  readonly name: string;
  readonly directory: string;
  readonly helper: string;
  readonly mcpUrl: string;
}

runGate("sharness-network-", (temporaryRoot) =>
  Effect.gen(function* () {
    const version = yield* Config.string("SHARNESS_NETWORK_VERSION").pipe(
      Effect.catchTag("ConfigError", () =>
        Effect.fail(
          new PackGateError({
            message:
              "set SHARNESS_NETWORK_VERSION to the tag of the Registry and Router images to run",
          }),
        ),
      ),
    );
    const consumerRoot = yield* installConsumer(temporaryRoot);
    return yield* Effect.scoped(ownNetworkPath(consumerRoot, version)).pipe(
      Effect.timeoutFail({
        duration: GATE_DEADLINE,
        onTimeout: () =>
          new PackGateError({
            message: `the own-network path did not finish within ${Duration.format(GATE_DEADLINE)}`,
          }),
      }),
    );
  }),
);

/**
 * Installs the packed Client with the helper's dependencies, and copies in
 * the two files a reader downloads.
 * @param temporaryRoot Scratch directory the gate owns.
 * @returns The consumer directory.
 */
function installConsumer(
  temporaryRoot: string,
): Effect.Effect<string, GateError, GateServices> {
  return Effect.gen(function* () {
    const { archives } = yield* packProductClosure(
      workspaceRoot,
      "packages/social-harness",
      temporaryRoot,
    );
    const manifest = yield* readJsonFile(
      join(imagesRoot, "package.json"),
      helperManifest,
    );
    const consumerRoot = yield* installPackedConsumer({
      temporaryRoot,
      name: "sharness-network",
      archives,
      dependencies: manifest.dependencies,
    });
    const fs = yield* FileSystem.FileSystem;
    for (const file of ["compose.yaml", "mcp.ts"]) {
      yield* fs.copyFile(join(imagesRoot, file), join(consumerRoot, file));
    }
    return consumerRoot;
  });
}

/**
 * Starts the network and three daemons in `consumerRoot`, then registers and
 * messages through the helper as the guides do.
 * @param consumerRoot Directory holding the installed packages and copied files.
 * @param version Image tag the Compose file runs.
 * @returns The line the gate prints when it passes.
 */
function ownNetworkPath(
  consumerRoot: string,
  version: string,
): Effect.Effect<string, GateError, GateServices | Scope.Scope> {
  return Effect.gen(function* () {
    const network = yield* startNetwork(consumerRoot, version);
    const [alice, bob, carol] = yield* Effect.all(
      [
        startDaemon(consumerRoot, network, "alice"),
        startDaemon(consumerRoot, network, "bob", FIREWALL_POLICY),
        startDaemon(consumerRoot, network, "carol"),
      ],
      { concurrency: 3 },
    );

    yield* expectOutput(
      alice,
      ["register", "alice"],
      ["registered agent:alice"],
    );
    yield* expectOutput(
      alice,
      ["register", "alice"],
      ["already registered: agent:alice"],
    );
    yield* expectOutput(bob, ["register", "bob"], ["registered agent:bob"]);
    yield* expectRefusal(carol, ["register", "alice"], ["name_taken"]);
    yield* expectOutput(alice, ["call", "status"], ['"kind": "registered"']);

    yield* expectOutput(alice, ["call", "send_message", sendTo("bob")], ["{"]);
    yield* expectOutput(
      bob,
      ["call", "read_inbox"],
      [MESSAGE, '"agentName": "alice"'],
    ).pipe(
      Effect.retry(
        Schedule.spaced(Duration.seconds(1)).pipe(
          Schedule.intersect(Schedule.recurs(DELIVERY_ATTEMPTS)),
        ),
      ),
    );
    yield* expectSendRefusals(alice, bob);
    return "own-network path passed";
  });
}

/**
 * Requires the send refusals the guides name: a recipient no agent holds, and
 * a send bob's own firewall refuses, which prints its check and next action.
 */
function expectSendRefusals(
  alice: Daemon,
  bob: Daemon,
): Effect.Effect<void, GateError, CommandExecutor.CommandExecutor> {
  return Effect.gen(function* () {
    yield* expectRefusal(
      alice,
      ["call", "send_message", sendTo("nobody")],
      ["send_message failed: unknown-agent"],
    );
    yield* expectRefusal(
      bob,
      ["call", "send_message", sendTo("alice", HELD_TEXT)],
      ["send_message failed:", "check-unavailable", "wait:"],
    );
  });
}

/** The `send_message` arguments Run sharnessd passes: `text` to `name`. */
function sendTo(name: string, text = MESSAGE): string {
  return JSON.stringify({ input: { to: `agent:${name}`, text } });
}

/**
 * Writes the keys and `.env` that Run a local network writes, then starts
 * Compose with `--detach --wait` for the enclosing scope.
 * @param consumerRoot Directory holding `compose.yaml`.
 * @param version Image tag the Compose file runs.
 * @returns The origins and secrets daemons join the network with.
 */
function startNetwork(
  consumerRoot: string,
  version: string,
): Effect.Effect<Network, GateError, GateServices | Scope.Scope> {
  return Effect.gen(function* () {
    const network = yield* writeNetworkFiles(consumerRoot, version);
    const project = `sharness-network-${randomBytes(8).toString("hex")}`;
    const compose = (...args: readonly string[]) =>
      runProcess("docker", [
        "compose",
        "--file",
        join(consumerRoot, "compose.yaml"),
        "--project-name",
        project,
        ...args,
      ]);
    yield* Effect.addFinalizer(() =>
      compose("down", "--volumes", "--remove-orphans").pipe(Effect.ignore),
    );
    const up = yield* compose("up", "--detach", "--wait");
    if (up.exitCode !== 0) {
      const logs = yield* compose("logs");
      return yield* Effect.fail(
        new PackGateError({
          message: `docker compose up --detach --wait exited with ${up.exitCode}:\n${up.stdout}${up.stderr}\n${logs.stdout}${logs.stderr}`,
        }),
      );
    }
    return network;
  });
}

/**
 * Writes the Registry key and the `.env` Compose reads, in the form Run a
 * local network writes them, on free ports.
 */
function writeNetworkFiles(
  consumerRoot: string,
  version: string,
): Effect.Effect<Network, GateError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const registryKey = makePrivateKeyPem();
    yield* fs.makeDirectory(join(consumerRoot, "keys"));
    yield* fs.writeFileString(
      join(consumerRoot, "keys", "registry-signing.pem"),
      registryKey,
      { mode: 0o600 },
    );
    const [registryPort, routerPort] = yield* Effect.all([
      freePort(),
      freePort(),
    ]);
    const network: Network = {
      registryOrigin: `http://${HOST}:${registryPort}`,
      routerOrigin: `http://${HOST}:${routerPort}`,
      registrySigner: publicJwk(registryKey),
      admissionCredential: randomBytes(32).toString("base64url"),
    };
    const settings = [
      `SHARNESS_NETWORK_VERSION=${version}`,
      `SHARNESS_NETWORK_ADMISSION_CREDENTIAL=${network.admissionCredential}`,
      `SHARNESS_NETWORK_REGISTRY_SIGNER='${network.registrySigner}'`,
      `SHARNESS_NETWORK_UID=${process.getuid?.() ?? 1000}`,
      `SHARNESS_NETWORK_GID=${process.getgid?.() ?? 1000}`,
      `SHARNESS_NETWORK_REGISTRY_PORT=${registryPort}`,
      `SHARNESS_NETWORK_ROUTER_PORT=${routerPort}`,
    ];
    yield* fs.writeFileString(
      join(consumerRoot, ".env"),
      `${settings.join("\n")}\n`,
      { mode: 0o600 },
    );
    return network;
  });
}

/**
 * Starts one unregistered `sharnessd` from the installed Client for the
 * enclosing scope. Returns once its MCP port accepts connections, and fails
 * with the daemon's output when it exits first.
 * @param consumerRoot Directory holding the installed packages.
 * @param network Settings the daemon joins with.
 * @param name Agent name, also the daemon's directory name.
 * @param policy A message-policy document for the daemon's firewall, if any.
 * @returns Where the helper reaches the daemon.
 */
function startDaemon(
  consumerRoot: string,
  network: Network,
  name: string,
  policy?: string,
): Effect.Effect<Daemon, GateError, GateServices | Scope.Scope> {
  return Effect.gen(function* () {
    const directory = join(consumerRoot, "agents", name);
    const port = yield* freePort();
    const environment = yield* writeAgentFiles(
      directory,
      network,
      port,
      policy,
    );
    const child = yield* Command.make(
      join(consumerRoot, "node_modules", ".bin", "sharnessd"),
    ).pipe(Command.env(environment), Command.start);
    const output = yield* Ref.make("");
    yield* Stream.merge(child.stdout, child.stderr).pipe(
      Stream.decodeText(),
      Stream.runForEach((chunk) => Ref.update(output, (all) => all + chunk)),
      Effect.forkScoped,
    );
    while (yield* child.isRunning.pipe(Effect.orElseSucceed(() => false))) {
      if (yield* canConnect(port)) {
        const helper = join(consumerRoot, "mcp.ts");
        return {
          name,
          directory,
          helper,
          mcpUrl: `http://${HOST}:${port}/mcp`,
        };
      }
      yield* Effect.sleep(Duration.millis(100));
    }
    return yield* Effect.fail(
      new PackGateError({
        message: `sharnessd for ${name} exited before it served port ${port}:\n${yield* Ref.get(output)}`,
      }),
    );
  });
}

/**
 * Writes the agent's private key and credentials beside its state directory.
 * A configured policy is saved as `message-policy.json` and named in
 * `SHARNESS_MESSAGE_POLICY_FILE`, as Turn on the message firewall does.
 */
function writeAgentFiles(
  directory: string,
  network: Network,
  port: number,
  policy?: string,
): Effect.Effect<Record<string, string>, GateError, FileSystem.FileSystem> {
  return Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.makeDirectory(join(directory, "state"), { recursive: true });
    const keyFile = join(directory, "agent-key.pem");
    const admissionFile = join(directory, "admission-credential");
    const mcpCredentialFile = join(directory, MCP_CREDENTIAL_FILE);
    yield* fs.writeFileString(keyFile, makePrivateKeyPem(), { mode: 0o600 });
    yield* fs.writeFileString(admissionFile, network.admissionCredential, {
      mode: 0o600,
    });
    yield* fs.writeFileString(
      mcpCredentialFile,
      randomBytes(32).toString("base64url"),
      { mode: 0o600 },
    );
    const policyFile = join(directory, "message-policy.json");
    if (policy !== undefined) {
      yield* fs.writeFileString(policyFile, policy);
    }
    return {
      ...(policy === undefined
        ? {}
        : { SHARNESS_MESSAGE_POLICY_FILE: policyFile }),
      SHARNESS_MCP_PORT: String(port),
      SHARNESS_MCP_CREDENTIAL_FILE: mcpCredentialFile,
      SHARNESS_STATE_DIRECTORY: join(directory, "state"),
      SHARNESS_REGISTRY_ORIGIN: network.registryOrigin,
      SHARNESS_REGISTRY_SIGNER_PUBLIC_KEY: network.registrySigner,
      SHARNESS_ROUTER_ORIGIN: network.routerOrigin,
      SHARNESS_AGENT_PRIVATE_KEY_FILE: keyFile,
      SHARNESS_ADMISSION_CREDENTIAL_FILE: admissionFile,
    };
  });
}

/**
 * Runs the helper for `daemon` and requires exit 0 with every `expected`
 * text in its standard output.
 */
function expectOutput(
  daemon: Daemon,
  args: readonly string[],
  expected: readonly string[],
): Effect.Effect<void, GateError, CommandExecutor.CommandExecutor> {
  return helper(daemon, args).pipe(
    Effect.flatMap(({ stdout, stderr, exitCode }) =>
      requireCondition(
        exitCode === 0 && expected.every((text) => stdout.includes(text)),
        `node mcp.ts ${args.join(" ")} for ${daemon.name}: expected exit 0 printing ${JSON.stringify(expected)}, got exit ${exitCode}:\n${stdout}${stderr}`,
      ),
    ),
  );
}

/**
 * A refusal names the failed operation and the reason or action the caller needs.
 */
function expectRefusal(
  daemon: Daemon,
  args: readonly string[],
  expected: readonly string[],
): Effect.Effect<void, GateError, CommandExecutor.CommandExecutor> {
  return helper(daemon, args).pipe(
    Effect.flatMap(({ stdout, stderr, exitCode }) =>
      requireCondition(
        exitCode === 1 && expected.every((text) => stderr.includes(text)),
        `node mcp.ts ${args.join(" ")} for ${daemon.name}: expected exit 1 naming ${JSON.stringify(expected)}; got exit ${exitCode}:\n${stdout}${stderr}`,
      ),
    ),
  );
}

/** Runs `node mcp.ts` in the daemon's directory against its MCP URL. */
function helper(
  daemon: Daemon,
  args: readonly string[],
): Effect.Effect<Finished, GateError, CommandExecutor.CommandExecutor> {
  return runProcess(process.execPath, [daemon.helper, ...args], {
    cwd: daemon.directory,
    env: {
      SHARNESS_MCP_URL: daemon.mcpUrl,
      SHARNESS_MCP_CREDENTIAL_FILE: join(daemon.directory, MCP_CREDENTIAL_FILE),
      NODE_PATH: "",
    },
  });
}

/** Whether a TCP connection to the loopback `port` succeeds. */
function canConnect(port: number): Effect.Effect<boolean> {
  return Effect.async<boolean>((resume) => {
    const socket = createConnection({ host: HOST, port });
    socket.once("connect", () => {
      socket.destroy();
      resume(Effect.succeed(true));
    });
    socket.once("error", () => {
      socket.destroy();
      resume(Effect.succeed(false));
    });
    return Effect.sync(() => {
      socket.destroy();
    });
  });
}

/** A loopback port no listener holds at the moment it is chosen. */
function freePort(): Effect.Effect<number, PackGateError> {
  return Effect.async<number, PackGateError>((resume) => {
    const server = createServer();
    server.once("error", () => {
      resume(Effect.fail(new PackGateError({ message: "no free port" })));
    });
    server.listen(0, HOST, () => {
      const address = server.address();
      server.close(() => {
        resume(
          address === null || typeof address === "string"
            ? Effect.fail(
                new PackGateError({ message: "listener reported no port" }),
              )
            : Effect.succeed(address.port),
        );
      });
    });
  });
}

/** A fresh unencrypted Ed25519 private key in PKCS#8 PEM. */
function makePrivateKeyPem(): string {
  return generateKeyPairSync("ed25519").privateKey.export({
    format: "pem",
    type: "pkcs8",
  });
}

/** The compact public JWK the Router and every daemon pin as the signer. */
function publicJwk(privateKeyPem: string): string {
  const { crv, kty, x } = createPublicKey(privateKeyPem).export({
    format: "jwk",
  });
  return JSON.stringify({ crv, kty, x });
}
