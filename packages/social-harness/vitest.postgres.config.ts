/** @file Runs PostgreSQL qualification explicitly against the supplied test database. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/database/*.integration.spec.ts"],
    testTimeout: 30_000,
  },
});
