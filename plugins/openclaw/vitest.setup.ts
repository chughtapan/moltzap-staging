/**
 * @file OpenClaw's public inbound runner records sessions in an isolated
 * temporary state directory.
 */

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, vi } from "vitest";

/** Temporary OpenClaw state directory shared by setup and test configuration. */
export const openClawTestStateDirectory = mkdtempSync(
  join(tmpdir(), "sharness-openclaw-channel-test-"),
);

vi.stubEnv("OPENCLAW_STATE_DIR", openClawTestStateDirectory);

afterAll(() => {
  vi.unstubAllEnvs();
  rmSync(openClawTestStateDirectory, { recursive: true, force: true });
});
