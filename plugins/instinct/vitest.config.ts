/** @file Hermetic provider and Client handoff tests; no external email is sent. */
import { defineConfig } from "vitest/config";

export default defineConfig({ test: { include: ["src/**/*.spec.ts"] } });
