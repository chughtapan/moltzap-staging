/**
 * @file Package ESLint configuration: the shared workspace preset plus the
 * rules that keep envelope signing in its owning modules.
 */
import { packageEslintConfig } from "../../eslint.shared.js";

/** Test files, which sign peer envelopes directly. */
const testFiles = ["src/**/*.spec.ts", "src/**/__tests__/**"];

/**
 * Keeps `communication/conversations/service.ts` the one production module that signs
 * an outer envelope, which it stores before the Router worker sends it.
 * `communication/records.ts` defines the signers.
 */
const soleOuterSigner = {
  files: ["src/**/*.ts"],
  ignores: [
    "src/communication/conversations/conversation.ts",
    "src/communication/records.ts",
    ...testFiles,
  ],
  rules: {
    "no-restricted-imports": [
      "error",
      {
        patterns: [
          {
            regex: "(^|/)records\\.js$",
            importNames: ["signOuterPacket", "signOuterEvidence"],
            message:
              "Only communication/conversations/service.ts signs outer envelopes; queue the body through the engine outbox.",
          },
        ],
      },
    ],
  },
};

/**
 * Keeps `communication/records.ts` the one module that calls
 * Identity's `SignedMessage.sign`, which it does for an outer envelope only
 * after sealing its body. The guard covers who signs, not what the outbox
 * enqueues: its port accepts any `SignedMessage`.
 */
const sealingSigner = {
  files: ["src/**/*.ts"],
  ignores: ["src/communication/records.ts", ...testFiles],
  rules: {
    "no-restricted-properties": [
      "error",
      {
        object: "SignedMessage",
        property: "sign",
        message:
          "Only communication/records.ts signs a SignedMessage, after sealing every outer body; queue the body through the engine outbox.",
      },
    ],
  },
};

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
  ...packageEslintConfig({
    projects: ["./tsconfig.json", "./tsconfig.test.json"],
    tsconfigRootDir: import.meta.dirname,
  }),
  {
    files: [
      "src/identity/http-authentication.ts",
      "src/identity/http-authentication.spec.ts",
      "src/identity/message-security.spec.ts",
      "src/communication/conversations/certification.ts",
      "src/identity/message-security.ts",
      "src/communication/messages.ts",
      "src/communication/records.ts",
      "src/communication/service.ts",
    ],
    rules: {
      "max-lines": [
        "error",
        { max: 2400, skipBlankLines: true, skipComments: true },
      ],
    },
  },
  {
    files: ["src/communication/store.ts"],
    rules: {
      "max-lines": [
        "error",
        { max: 4200, skipBlankLines: true, skipComments: true },
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
  soleOuterSigner,
  sealingSigner,
  messageCheckRules,
  {
    files: [
      "src/harness/social-harness.ts",
      "src/harness/inbox.ts",
      "src/firewall/service.ts",
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
  {
    files: [
      "src/communication/conversations/reanchor.ts",
      "src/communication/conversations/recovery.ts",
      "src/communication/router/connection.ts",
      "src/communication/collectives.ts",
      "src/communication/records.spec.ts",
      "src/launcher/installation.ts",
      "src/communication/conversations/history.ts",
    ],
    rules: {
      "max-lines": [
        "error",
        { max: 1500, skipBlankLines: true, skipComments: true },
      ],
    },
  },
];
