/** @file Vitest configuration for the unit suites: the package, both channel plugins and the documentation tooling. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: [
      {
        resolve: { conditions: ["sharness-test"] },
        ssr: { resolve: { conditions: ["sharness-test"] } },
        test: { name: "social-harness", include: ["src/**/*.spec.ts"] },
      },
      {
        test: {
          name: "openclaw-channel",
          include: ["plugins/openclaw/src/**/*.spec.ts"],
          setupFiles: ["./plugins/openclaw/vitest.setup.ts"],
        },
      },
      {
        test: {
          name: "nanoclaw-channel",
          include: ["plugins/nanoclaw/src/**/*.spec.ts"],
        },
      },
      {
        test: {
          name: "docs-tooling",
          include: ["scripts/docs/mermaid-lint.spec.ts"],
        },
      },
    ],
  },
});
