/**
 * @file ESLint configuration for the root package: the shared package preset
 * on `src/`, the root preset on root modules, `images/` helpers and
 * `scripts/`, and the size limits of the package's largest modules.
 */
import {
  packageEslintConfig,
  ROOT_PROJECTS,
  rootEslintConfig,
} from "./eslint.shared.js";

/** Message schemas and their ordered assessment share one checking boundary. */
const messageCheckRules = {
  files: [
    "src/firewall/check.ts",
    "src/firewall/service.ts",
    "src/firewall/check.spec.ts",
  ],
  rules: {
    "max-lines": [
      "error",
      { max: 1700, skipBlankLines: true, skipComments: true },
    ],
  },
};

export default [
  {
    ignores: [
      "src/vendor/effect-mcp/McpSchema.ts",
      "src/vendor/effect-mcp/McpServer.ts",
    ],
  },
  ...rootEslintConfig(import.meta.dirname),
  ...packageEslintConfig({
    projects: ROOT_PROJECTS,
    tsconfigRootDir: import.meta.dirname,
  }),
  {
    files: ["src/communication/messages.ts"],
    rules: {
      "max-lines": [
        "error",
        { max: 2400, skipBlankLines: true, skipComments: true },
      ],
    },
  },
  {
    files: ["src/firewall/rules.spec.ts"],
    rules: {
      "max-lines-per-function": [
        "error",
        { max: 500, skipBlankLines: true, skipComments: true },
      ],
    },
  },
  messageCheckRules,
  {
    files: [
      "src/harness/social-harness.ts",
      "src/harness/inbox.ts",
      "src/firewall/service.ts",
      "src/firewall/store.ts",
    ],
    rules: {
      "max-lines-per-function": [
        "error",
        { max: 600, skipBlankLines: true, skipComments: true },
      ],
      complexity: ["error", { max: 32 }],
      "sonarjs/cognitive-complexity": ["error", 35],
      "max-depth": ["error", 4],
      "sonarjs/nested-control-flow": ["error", { maximumNestingLevel: 4 }],
      "sonarjs/no-nested-functions": ["error", { threshold: 6 }],
      "max-params": ["error", 7],
    },
  },
];
