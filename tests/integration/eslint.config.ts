/** @file Process suites use integration rules, and image scenarios use their Node test boundary. */
import {
  agentImageEslintConfig,
  integrationEslintConfig,
} from "../../eslint.shared.js";

export default [
  ...integrationEslintConfig(import.meta.dirname).map((config) =>
    "files" in config
      ? {
          ...config,
          ignores: ["images/**"],
        }
      : config,
  ),
  ...agentImageEslintConfig(import.meta.dirname).map((config) =>
    "files" in config
      ? { ...config, files: config.files.map((file) => `images/${file}`) }
      : config,
  ),
];
