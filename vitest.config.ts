/** @file Vitest configuration for the package's unit suite. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "social-harness",
          include: ["src/**/*.spec.ts"],
          // A property that builds a database on each run creates a PostgreSQL schema each time, on a server other suites share.
          testTimeout: 30_000,
        },
      },
    ],
  },
});
