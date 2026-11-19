/** @file The daemon refuses to start with an invalid message policy file until the settings no longer name it. */

import { FileSystem } from "@effect/platform";
import { Effect } from "effect";
import { join } from "node:path";
import { expect, it } from "vitest";
import { sharedDaemon, stopProcess } from "../daemon/daemon-fixture.js";
import { processTrace } from "../scenario-fixture.js";

/** What the daemon prints when it cannot use its message policy file. */
const INVALID_POLICY =
  "The configured message policy or provider credential is invalid";

const invalidDocument = Effect.gen(function* () {
  const daemon = yield* sharedDaemon();
  const original = yield* daemon.start();
  const { agent } = yield* daemon.signInAs("carol");
  yield* stopProcess(original);
  const fs = yield* FileSystem.FileSystem;
  const messagePolicyFile = join(daemon.directory, "invalid-policy.json");
  yield* fs.writeFileString(
    messagePolicyFile,
    JSON.stringify({
      format: "org.social-harness/message-policy/v2",
      mode: "off",
    }),
  );
  yield* daemon.configure({ messagePolicyFile });
  const failure = yield* daemon.startupFailure();
  expect(failure.exitCode).not.toBe(0);
  expect(failure.logs).toContain(INVALID_POLICY);
  yield* daemon.configure({ messagePolicyFile: undefined });
  yield* daemon.start();
  expect((yield* daemon.signInAs("carol")).agent).toEqual(agent);
});

it(
  "rejects an invalid message policy file and starts again once the settings no longer name it",
  processTrace(invalidDocument),
  60_000,
);
