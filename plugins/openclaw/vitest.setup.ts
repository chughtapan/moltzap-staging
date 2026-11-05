/**
 * @file OpenClaw's public inbound runner records sessions in an isolated
 * temporary state directory.
 */

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, vi } from "vitest";

/** Temporary OpenClaw state directory shared by setup and test configuration. */
export const openClawTestStateDirectory = mkdtempSync(
  join(tmpdir(), "sharness-openclaw-channel-test-"),
);

/** Private bearer file supplied to the channel during connection tests. */
export const openClawTestMcpCredentialFile = join(
  openClawTestStateDirectory,
  "mcp-credential",
);
writeFileSync(openClawTestMcpCredentialFile, "fixture-mcp-bearer", {
  mode: 0o600,
});

vi.stubEnv("OPENCLAW_STATE_DIR", openClawTestStateDirectory);

afterAll(() => {
  vi.unstubAllEnvs();
  rmSync(openClawTestStateDirectory, { recursive: true, force: true });
});
