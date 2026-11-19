/** @file Real OpenClaw and daemon containers deliver a client's message to a local model stand-in. */
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { once } from "node:events";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
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
import { openDaemon } from "./daemon-container.ts";

const docker = promisify(execFile);
/** Fixture state is copied before the normal host entrypoint secures its directories. */
const HOST_BOOTSTRAP = [
  "cp -R /probe/state /home/node/test-state",
  "exec bash /opt/social-harness/agent/entrypoint.sh",
].join(" && ");
const image = process.env.OPENCLAW_INTEGRATION_IMAGE;
const skip =
  image === undefined || process.env.DAEMON_INTEGRATION_IMAGE === undefined
    ? "set OPENCLAW_INTEGRATION_IMAGE and DAEMON_INTEGRATION_IMAGE to built images"
    : false;
const text = "client message through the separate daemon container";
/** How long the stand-in waits for the client's text. */
const TURN_TIMEOUT_MILLIS = 180_000;

interface HostOptions {
  readonly container: string;
  readonly daemon: string;
  readonly directory: string;
  readonly mcp: URL;
  readonly mcpCredentialFile: string;
}

/**
 * Starts the host image on the daemon container's network. The image user
 * copies fixture state so OpenClaw owns the directories it secures, and reads
 * the credential file its agent's owner signed in with.
 */
async function startHost(options: HostOptions): Promise<undefined> {
  const { container, daemon, directory, mcp, mcpCredentialFile } = options;
  await docker("docker", [
    "run",
    "--detach",
    "--name",
    container,
    "--network",
    `container:${daemon}`,
    "--mount",
    `type=bind,src=${directory},dst=/probe`,
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
    "ANTHROPIC_API_KEY=stand-in-key",
    "--entrypoint=bash",
    image ?? "",
    "-c",
    HOST_BOOTSTRAP,
  ]);
}

/** Whether a request to the stand-in carries the client's text before the deadline. */
async function receivedTurn(requests: readonly string[]): Promise<boolean> {
  const deadline = Date.now() + TURN_TIMEOUT_MILLIS;
  while (Date.now() < deadline) {
    if (requests.join("\n").includes(text)) {
      return true;
    }
    await delay(250);
  }
  return false;
}

/** Writes an OpenClaw configuration whose one model is the stand-in listening on `port`. */
async function prepareHost(
  directory: string,
  port: number,
): Promise<undefined> {
  await chmod(directory, 0o777);
  const state = join(directory, "state");
  await mkdir(state, { mode: 0o777 });
  await chmod(state, 0o777);
  await writeFile(
    join(state, "openclaw.json"),
    JSON.stringify({
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
          model: { primary: "probe/stand-in" },
          models: { "probe/stand-in": {} },
        },
      },
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
    }),
    { mode: 0o666 },
  );
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

test(
  "OpenClaw delivers a client's turn to its model",
  { skip, timeout: 300_000 },
  async () => {
    const directory = await mkdtemp(join(tmpdir(), "openclaw-container-"));
    const daemon = await openDaemon();
    const provider = await startProvider();
    const container = `openclaw-host-${Date.now()}`;
    try {
      await prepareHost(directory, provider.port);
      const target = await daemon.signIn();
      const caller = await daemon.signIn("caller");
      await caller.send({ text });
      await startHost({
        container,
        daemon: daemon.container,
        directory,
        mcp: target.mcp,
        mcpCredentialFile: target.mcpCredentialFile,
      });
      const received = await receivedTurn(provider.requests);
      const logs = await docker("docker", ["logs", container]);
      assert.ok(
        received,
        `stand-in did not receive the client's text\n${logs.stdout}\n${logs.stderr}`,
      );
    } finally {
      await docker("docker", ["rm", "--force", container]).catch(
        () => undefined,
      );
      await provider.close();
      await daemon.close();
      await rm(directory, { recursive: true, force: true });
    }
  },
);
