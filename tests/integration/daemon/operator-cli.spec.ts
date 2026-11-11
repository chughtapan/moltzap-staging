/** @file The documented operator script calls a real daemon's tools. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { toolDefinitions } from "@social-harness/social-harness";
import { Effect, Schema } from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { runProcess } from "../../../scripts/test/packed-workspace.js";
import { processTrace } from "../scenario-fixture.js";
import { sharedDaemon } from "./daemon-fixture.js";

const CLI = fileURLToPath(new URL("../../../images/mcp.ts", import.meta.url));
const TEST_DIRECTORY = fileURLToPath(new URL("../.cache/", import.meta.url));

it(
  "reports the agent of the owner whose credential it reads",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
      yield* daemon.start();
      const alice = yield* daemon.signInAs("alice");
      const bob = yield* daemon.signInAs("bob");
      const status = (credentialFile: string) =>
        operator({ endpoint: daemon.endpoint, credentialFile }, [
          "call",
          "status",
        ]).pipe(
          Effect.flatMap(({ stdout }) =>
            Schema.decode(Schema.parseJson(toolDefinitions.status.output))(
              stdout,
            ),
          ),
        );
      expect((yield* status(alice.credentialFile)).agentName).toBe(
        alice.agent.agentName,
      );
      expect((yield* status(bob.credentialFile)).agentName).toBe(
        bob.agent.agentName,
      );
    }),
  ),
  120_000,
);

// @agent-code-guard/regression-only: The standalone CLI must reject cleartext DNS destinations before reading its private bearer file.
/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- The exit code and the refusal text are the CLI's public contract. */
it(
  "refuses credential delivery to an HTTP endpoint named by DNS",
  processTrace(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const refused = yield* operator(
        {
          endpoint: new URL("http://localhost:1/mcp"),
          credentialFile: join(directory, "unread-credential"),
        },
        ["call", "status"],
      );
      expect(refused.exitCode).toBe(1);
      expect(refused.stderr).toContain(
        "MCP URL must use HTTPS or literal HTTP loopback",
      );
    }),
  ),
);
/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore ordinary assertions after the operator contract scenario. */

/** Runs a copy of the script, which resolves the integration package's installed public dependencies. */
function operator(
  target: { readonly endpoint: URL; readonly credentialFile: string },
  args: readonly string[],
) {
  return Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      yield* fs.makeDirectory(TEST_DIRECTORY, { recursive: true });
      const directory = yield* fs.makeTempDirectoryScoped({
        directory: TEST_DIRECTORY,
        prefix: ".operator-cli-",
      });
      const script = join(directory, "mcp.ts");
      yield* fs.copyFile(CLI, script);
      return yield* runProcess(process.execPath, [script, ...args], {
        cwd: directory,
        env: {
          SHARNESS_MCP_URL: target.endpoint.href,
          SHARNESS_MCP_CREDENTIAL_FILE: target.credentialFile,
          NODE_PATH: "",
        },
      });
    }),
  ).pipe(Effect.provide(NodeContext.layer));
}
