/** @file Published-peer communication and installed launcher upgrade scenarios. */
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: { conditions: ["sharness-test", "sharness-internal"] },
  ssr: { resolve: { conditions: ["sharness-test", "sharness-internal"] } },
  test: {
    include: [
      "tests/integration/communication/published-peers.spec.ts",
      "tests/integration/daemon/launcher-upgrades.spec.ts",
    ],
    maxWorkers: 1,
    passWithNoTests: false,
  },
});
