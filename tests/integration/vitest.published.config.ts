/** @file Communication with independently published peers. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-test", "sharness-internal"] },
  ssr: { resolve: { conditions: ["sharness-test", "sharness-internal"] } },
  test: {
    include: ["tests/integration/communication/published-peers.spec.ts"],
    maxWorkers: 1,
    passWithNoTests: false,
  },
});
