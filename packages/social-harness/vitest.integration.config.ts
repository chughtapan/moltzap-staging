/** @file Client process feedback through its installed executable. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["integration/**/*.integration.test.ts"],
    passWithNoTests: false,
  },
});
