#!/usr/bin/env node
/**
 * @file Checks that each tracked `AGENTS.md` has a sibling `CLAUDE.md` symlink.
 *
 * The stored target must be `AGENTS.md`. Comparing link targets keeps both
 * hosts on one instruction source even after its contents change.
 * Exits 0 when all links conform, or 1 with each violation and its remedy.
 */
import { execFileSync } from "node:child_process";
import { lstatSync, readlinkSync } from "node:fs";
import { dirname, join } from "node:path";

// eslint-disable-next-line sonarjs/no-os-command-from-path -- Developer tooling intentionally resolves installed commands through the caller’s PATH.
const repoRoot = execFileSync("git", ["rev-parse", "--show-toplevel"], {
  encoding: "utf8",
}).trim();

interface Violation {
  readonly path: string;
  readonly problem: string;
}

function main(): void {
  const agentFiles = tracked("*AGENTS.md");
  const violations: Violation[] = [];
  for (const agentFile of agentFiles) {
    const violation = checkAgentFile(agentFile);
    if (violation !== null) {
      violations.push(violation);
    }
  }
  if (violations.length === 0) {
    console.log(
      `[check-agent-files] PASS — ${agentFiles.length} AGENTS.md file(s), each with a CLAUDE.md symlink.`,
    );
    process.exit(0);
  }
  console.error("[check-agent-files] FAIL\n");
  for (const v of violations) {
    console.error(`  ${v.path}`);
    console.error(`    ${v.problem}`);
    console.error(
      "    → fix with: git rm --cached <path> && rm <path> && " +
        "ln -s AGENTS.md <path> && git add <path>\n",
    );
  }
  process.exit(1);
}

function tracked(pattern: string): readonly string[] {
  // eslint-disable-next-line sonarjs/no-os-command-from-path -- Developer tooling intentionally resolves installed commands through the caller’s PATH.
  return execFileSync("git", ["-C", repoRoot, "ls-files", pattern], {
    encoding: "utf8",
  })
    .split("\n")
    .filter((p) => p.length > 0);
}

function checkAgentFile(agentFile: string): Violation | null {
  const claudeFile = join(dirname(agentFile), "CLAUDE.md");
  const absolute = join(repoRoot, claudeFile);
  let stat;
  try {
    stat = lstatSync(absolute);
    // eslint-disable-next-line agent-code-guard/bare-catch -- The violation reports the inaccessible sibling without changing the gate’s diagnostics.
  } catch {
    return {
      path: claudeFile,
      problem: `missing — ${agentFile} has no sibling CLAUDE.md`,
    };
  }
  if (!stat.isSymbolicLink()) {
    return {
      path: claudeFile,
      problem: "is a regular file; it must be a symlink to AGENTS.md",
    };
  }
  const target = readlinkSync(absolute);
  if (target !== "AGENTS.md") {
    return {
      path: claudeFile,
      problem: `points at \`${target}\`, not the sibling \`AGENTS.md\``,
    };
  }
  return null;
}

main();
