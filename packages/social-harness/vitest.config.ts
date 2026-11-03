/** @file Vitest configuration for the package unit suite. */
import { configDefaults, defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-test", "sharness-internal"] },
  ssr: { resolve: { conditions: ["sharness-test", "sharness-internal"] } },
  test: {
    include: ["src/**/*.spec.ts"],
    exclude: [...configDefaults.exclude, "src/**/*.integration.spec.ts"],
  },
});
