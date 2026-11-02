/** @file Reports the installed Social Harness package version. */

import packageJson from "../package.json" with { type: "json" };

/** The installed `@moltzap/social-harness` package version. */
export const packageVersion: string = packageJson.version;
