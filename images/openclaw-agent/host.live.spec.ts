/** @file Real OpenClaw and daemon containers deliver peer messages to local model stand-ins. */
import {
  type Network,
  startNetwork,
} from "@moltzap/integration-tests/process-network";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { once } from "node:events";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { fakeClaude } from "../tests/claude-fixture.ts";
import { startDaemonContainer } from "../tests/daemon-container.ts";

const docker = promisify(execFile);
/** Fixture state is copied before the normal host entrypoint secures its directories. */
const HOST_BOOTSTRAP = [
  "cp -R /probe/state /home/node/test-state",
  "exec bash /opt/social-harness/agent/host.sh",
].join(" && ");
const image = process.env.OPENCLAW_INTEGRATION_IMAGE;
const skip =
  image === undefined || process.env.DAEMON_INTEGRATION_IMAGE === undefined
    ? "set OPENCLAW_INTEGRATION_IMAGE and DAEMON_INTEGRATION_IMAGE to built images"
    : false;
const text = "peer message through the separate daemon container";

async function deliverTurn(
  network: Network,
  options: { runtime: string; directory: string; container: string },
  provider: { requests: string[] },
): Promise<undefined> {
  const { runtime, directory, container } = options;
  const target = await network.daemon(`openclaw-${runtime}`);
  const caller = await network.endpoint(`caller-${runtime}`);
  await caller.send({ to: target.address, text });
  await startHost({
    container,
    daemon: `sharness-daemon-${target.mcp.port}`,
    directory,
    mcp: target.mcp,
    mcpCredentialFile: target.mcpCredentialFile,
  });
  const received = await receivedTurn(directory, runtime, provider.requests);
  const logs = await docker("docker", ["logs", container]);
  assert.ok(
    received,
    `stand-in did not receive peer text\n${logs.stdout}\n${logs.stderr}`,
  );
}

interface HostOptions {
  readonly container: string;
  readonly daemon: string;
  readonly directory: string;
  readonly mcp: URL;
  readonly mcpCredentialFile: string;
}

/** The image user copies fixture state so OpenClaw owns the directories it secures. */
async function startHost(options: HostOptions): Promise<undefined> {
  const { container, daemon, directory, mcp, mcpCredentialFile } = options;
  const inspected = await docker("docker", [
    "inspect",
    "--format",
    '{{range .Mounts}}{{if eq .Destination "/opt/social-harness/launcher"}}{{.Name}}{{end}}{{end}}',
    daemon,
  ]);
  const volume = inspected.stdout.trim();
  assert.ok(volume.length > 0);
  await docker("docker", [
    "run",
    "--detach",
    "--name",
    container,
    "--network",
    `container:${daemon}`,
    "--mount",
    `type=volume,src=${volume},dst=/opt/social-harness/launcher,readonly`,
    "--mount",
    `type=bind,src=${directory},dst=/probe`,
    "--mount",
    `type=bind,src=${directory}/claude,dst=/home/node/.local/bin/claude,readonly`,
    "--env",
    "HOME=/home/node",
    "--env",
    "OPENCLAW_STATE_DIR=/home/node/test-state",
    "--env",
    "OPENCLAW_CONFIG_PATH=/home/node/test-state/openclaw.json",
    "--env",
    `SHARNESS_MCP_URL=${mcp.href}`,
    "--env",
    "SHARNESS_MCP_CREDENTIAL_FILE=/run/sharness-mcp-credential",
    "--mount",
    `type=bind,src=${mcpCredentialFile},dst=/run/sharness-mcp-credential,readonly`,
    "--env",
    "OPENCLAW_DISABLE_BONJOUR=1",
    "--env",
    "FAKE_CLAUDE_LOG=/probe/claude.ndjson",
    "--env",
    "ANTHROPIC_API_KEY=stand-in-key",
    "--entrypoint=bash",
    image ?? "",
    "-c",
    HOST_BOOTSTRAP,
  ]);
}

async function receivedTurn(
  directory: string,
  runtime: string,
  requests: string[],
): Promise<boolean> {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    const received =
      runtime === "provider"
        ? requests.join("\n")
        : await readFile(join(directory, "claude.ndjson"), "utf8").catch(
            () => "",
          );
    if (received.includes(text)) {
      return true;
    }
    await delay(250);
  }
  return false;
}

async function prepareHost(
  directory: string,
  runtime: string,
  port: number,
): Promise<undefined> {
  await chmod(directory, 0o777);
  const state = join(directory, "state");
  await mkdir(state, { mode: 0o777 });
  await chmod(state, 0o777);
  await writeFile(join(directory, "claude"), fakeClaude, { mode: 0o755 });
  await writeFile(
    join(state, "openclaw.json"),
    JSON.stringify(configuration(runtime, port)),
    { mode: 0o666 },
  );
}

function configuration(runtime: string, port: number) {
  const model =
    runtime === "claude-cli" ? "anthropic/claude-opus-4-8" : "probe/stand-in";
  return {
    gateway: {
      mode: "local",
      bind: "loopback",
      auth: { mode: "token", token: "dummy-local-gateway-token" },
    },
    plugins: {
      allow: ["anthropic", "social-harness"],
      entries: {
        anthropic: { enabled: true },
        "social-harness": { enabled: true },
      },
    },
    channels: {
      sharness: {
        accounts: [{ id: "default", enabled: true, mode: "shared" }],
      },
    },
    agents: {
      defaults: {
        workspace: "/home/node/test-workspace",
        model: { primary: model },
        models: {
          [model]:
            runtime === "claude-cli"
              ? { agentRuntime: { id: "claude-cli" } }
              : {},
        },
      },
    },
    ...providerConfiguration(runtime, port),
  };
}

function providerConfiguration(runtime: string, port: number) {
  if (runtime !== "provider") {
    return {};
  }
  return {
    models: {
      providers: {
        probe: {
          baseUrl: `http://127.0.0.1:${port}/v1`,
          apiKey: "stand-in-key",
          api: "openai-completions",
          models: [
            {
              id: "stand-in",
              name: "Stand-in",
              contextWindow: 128000,
              maxTokens: 2048,
            },
          ],
        },
      },
    },
  };
}

async function startProvider() {
  const requests: string[] = [];
  const server = createServer((request, response) => {
    void answer(request, response, requests).catch((cause: unknown) => {
      response.destroy(
        cause instanceof Error ? cause : new Error("stand-in failed"),
      );
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address !== null && typeof address !== "string");
  return {
    requests,
    port: address.port,
    close: async () => {
      server.closeAllConnections();
      await new Promise<undefined>((resolve) => {
        server.close(() => {
          resolve(undefined);
        });
      });
    },
  };
}

async function answer(
  request: IncomingMessage,
  response: ServerResponse,
  requests: string[],
): Promise<undefined> {
  request.setEncoding("utf8");
  let body = "";
  for await (const chunk of request) {
    if (typeof chunk === "string") {
      body += chunk;
    }
  }
  requests.push(body);
  response.writeHead(200, { "content-type": "text/event-stream" });
  response.end(completion());
}

/** SSE response accepted by OpenClaw's OpenAI-compatible provider. */
function completion(): string {
  const frame = {
    id: "chatcmpl-stand-in",
    object: "chat.completion.chunk",
    created: 1,
    model: "stand-in",
  };
  return (
    [
      {
        ...frame,
        choices: [
          {
            index: 0,
            delta: { role: "assistant", content: "acknowledged" },
            finish_reason: null,
          },
        ],
      },
      {
        ...frame,
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
        usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 },
      },
    ]
      .map((value) => `data: ${JSON.stringify(value)}\n\n`)
      .join("") + "data: [DONE]\n\n"
  );
}

for (const runtime of ["provider", "claude-cli"]) {
  test(
    `OpenClaw delivers a peer turn to the ${runtime} stand-in`,
    { skip, timeout: 300_000 },
    async () => {
      const directory = await mkdtemp(join(tmpdir(), "openclaw-container-"));
      const network = await startNetwork(startDaemonContainer);
      const provider = await startProvider();
      const container = `openclaw-host-${runtime}-${Date.now()}`;
      try {
        await prepareHost(directory, runtime, provider.port);
        await deliverTurn(network, { runtime, directory, container }, provider);
      } finally {
        await docker("docker", ["rm", "--force", container]).catch(
          () => undefined,
        );
        await provider.close();
        await network.close();
        await rm(directory, { recursive: true, force: true });
      }
    },
  );
}
