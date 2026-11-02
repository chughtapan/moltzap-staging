/** @file Vitest configuration for NanoClaw's adapter boundary tests. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/*.spec.ts"],
  },
});
