/** @file Reports the installed package version. */

import packageJson from "../package.json" with { type: "json" };

/** The version of this package, as the daemon reports it to MCP clients and in telemetry. */
export const packageVersion: string = packageJson.version;
