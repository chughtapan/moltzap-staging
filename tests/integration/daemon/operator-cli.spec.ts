/** @file The documented operator CLI registers against real daemon and network processes. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import { AgentIdentityCard, toolDefinitions } from "@moltzap/social-harness";
import { Effect, Schema } from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { runProcess } from "../../../scripts/test/packed-workspace.js";
import { processTrace } from "../scenario-fixture.js";
import {
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  configureDaemonFixture,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  makeRegistrationRequest,
  reservePort,
} from "./daemon-fixture.js";

const CLI = fileURLToPath(new URL("../../../images/mcp.ts", import.meta.url));
const TEST_DIRECTORY = fileURLToPath(new URL("../.cache/", import.meta.url));
const registrationFile = (fixture: DaemonProcessFixture) =>
  join(fixture.stateDirectory, "registration.json");

// @agent-code-guard/regression-only: Operator retries must use saved registration identity, recover a lost reply, and discard a final name conflict.
/* eslint-disable agent-code-guard/no-hardcoded-assertion-literals -- Exit codes, tool status and closed failure reasons are the CLI's public contract. */
it(
  "reports the saved identity, clears an obsolete request and refuses a different name",
  processTrace(
    Effect.gen(function* () {
      const infrastructure = yield* acquireProcessInfrastructure;
      const alice = yield* makeDaemonProcessFixture(
        infrastructure,
        "cli-alice",
      );
      const other = yield* makeDaemonProcessFixture(
        infrastructure,
        "cli-other",
      );
      yield* acquireDaemonProcess(alice);
      yield* acquireDaemonProcess(other);
      const fs = yield* FileSystem.FileSystem;
      const request = makeRegistrationRequest(alice);
      yield* fs.writeFileString(
        registrationFile(alice),
        JSON.stringify(request),
      );

      const registered = yield* operator(alice, ["register", alice.agentName]);
      expect(registered.exitCode, registered.stderr).toBe(0);
      expect(registered.stdout.trim()).toBe(
        `already registered: agent:${alice.agentName}`,
      );
      expect((yield* identity(alice)).principalId).toBe(request.principalId);

      yield* fs.writeFileString(
        registrationFile(alice),
        JSON.stringify(request),
      );
      const recovered = yield* operator(alice, ["register", alice.agentName]);
      expect(recovered.exitCode).toBe(0);
      expect(recovered.stdout.trim()).toBe(
        `already registered: agent:${alice.agentName}`,
      );
      expect(yield* fs.exists(registrationFile(alice))).toBe(false);

      const refused = yield* operator(other, ["register", alice.agentName]);
      expect(refused.exitCode).toBe(1);
      expect(refused.stderr).toContain(
        `already registered as agent:${other.agentName}`,
      );
      expect(refused.stderr).toContain("Docs: ");
      const changed = yield* operator(other, ["register", other.agentName]);
      expect(changed.exitCode).toBe(0);
      expect((yield* identity(other)).agentName).toBe(other.agentName);
    }),
  ),
  120_000,
);

// @agent-code-guard/regression-only: A failed first registration must reuse the SQL identity after its dependency is repaired.
it(
  "reuses the saved registration after a dependency failure and daemon restart",
  processTrace(
    Effect.gen(function* () {
      const infrastructure = yield* acquireProcessInfrastructure;
      const fixture = yield* makeDaemonProcessFixture(
        infrastructure,
        "cli-retry",
      );
      const unavailable = yield* configureDaemonFixture(fixture, {
        registryOrigin: `http://127.0.0.1:${yield* reservePort}`,
      });
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* acquireDaemonProcess(unavailable);
          const failed = yield* operator(fixture, [
            "register",
            fixture.agentName,
          ]);
          expect(failed.exitCode).toBe(1);
          expect(failed.stderr).toContain("could not connect");
        }),
      );
      yield* configureDaemonFixture(fixture, {
        registryOrigin: infrastructure.registryOrigin.origin,
      });
      yield* acquireDaemonProcess(fixture);
      const retried = yield* operator(fixture, ["register", fixture.agentName]);
      expect(retried.exitCode).toBe(0);
      expect((yield* identity(fixture)).principalId).toBe(
        fixture.registration.principalId,
      );
    }),
  ),
  120_000,
);

// @agent-code-guard/regression-only: The standalone CLI must reject cleartext DNS destinations before reading its private bearer file.
it(
  "refuses credential delivery to an HTTP endpoint named by DNS",
  processTrace(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const refused = yield* operator(
        {
          stateDirectory: directory,
          endpoint: new URL("http://localhost:1/mcp"),
          mcpCredentialFile: join(directory, "unread-credential"),
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
/* eslint-enable agent-code-guard/no-hardcoded-assertion-literals -- Restore ordinary assertions after the operator contract scenarios. */

function identity(fixture: DaemonProcessFixture) {
  return operator(fixture, ["call", "status"]).pipe(
    Effect.flatMap(({ stdout }) =>
      Schema.decode(Schema.parseJson(toolDefinitions.status.output))(stdout),
    ),
    Effect.flatMap((status) =>
      status.kind === "registered"
        ? Schema.decodeUnknown(AgentIdentityCard)(status.identityCard)
        : Effect.dieMessage("operator status did not contain a saved identity"),
    ),
  );
}

/** The copied CLI resolves the integration package's installed public dependencies. */
function operator(
  fixture: Pick<
    DaemonProcessFixture,
    "stateDirectory" | "endpoint" | "mcpCredentialFile"
  >,
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
        cwd: fixture.stateDirectory,
        env: {
          SHARNESS_MCP_URL: fixture.endpoint.href,
          SHARNESS_MCP_CREDENTIAL_FILE: fixture.mcpCredentialFile,
          NODE_PATH: "",
        },
      });
    }),
  ).pipe(Effect.provide(NodeContext.layer));
}
