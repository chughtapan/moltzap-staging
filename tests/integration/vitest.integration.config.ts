/** @file Vitest configuration for the cross-package process integration lane. */
import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-test", "sharness-internal"] },
  ssr: { resolve: { conditions: ["sharness-test", "sharness-internal"] } },
  test: {
    include: ["**/*.spec.ts"],
    exclude: [
      ...configDefaults.exclude,
      "communication/published-peers.spec.ts",
      "daemon/launcher-upgrades.spec.ts",
      "images/**",
    ],
    maxWorkers: 2,
    passWithNoTests: false,
  },
});
