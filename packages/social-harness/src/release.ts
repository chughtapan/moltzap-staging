/** @file Client release metadata shared by protocol peers, policy provenance and telemetry. */

import packageJson from "../package.json" with { type: "json" };

/** The Client release reported by MCP peers and retained in policy provenance and telemetry. */
export const packageVersion: string = packageJson.version;
