/**
 * @file The repository's one ESLint flat configuration. Each section names
 * the paths it covers: the root package's `src/`, the root modules with the
 * `images/` helper and `scripts/`, each plugin, the agent-image tooling and
 * the integration tests.
 */
import type { Rule } from "eslint";
import type * as ESTree from "estree";
import comments from "@eslint-community/eslint-plugin-eslint-comments";
import { plugin as guard } from "eslint-plugin-agent-code-guard";

type FunctionNode = (
  | ESTree.ArrowFunctionExpression
  | ESTree.FunctionDeclaration
  | ESTree.FunctionExpression
) &
  Rule.NodeParentExtension;

type StrictRules = ReturnType<typeof makeStrictRules>;
type GuardPreset = typeof guard.configs.documentation;

/** Build outputs, dependencies, agent state and the vendored MCP server are not linted. */
const globalIgnores = {
  ignores: [
    "**/dist/**",
    "**/node_modules/**",
    "**/*.d.ts",
    ".claude/**",
    ".repos/**",
    "src/vendor/effect-mcp/McpSchema.ts",
    "src/vendor/effect-mcp/McpServer.ts",
  ],
};

/** ESLint and Vitest load a configuration module through its default export. */
const configModuleRules = {
  files: ["*.config.ts", "**/*.config.ts"],
  rules: { "import-x/no-default-export": "off" },
};

/** Per-file line ceiling, counting neither blanks nor comments. */
const DEFAULT_MAX_LINES = 1050;

const PACKAGE_SOURCE_FILES = [
  "src/**/*.ts",
  "src/**/*.cts",
  "src/**/*.mts",
  "*.ts",
  "*.cts",
  "*.mts",
];

const TEST_SUPPORT_FILES = [
  "src/**/*.spec.ts",
  "src/__tests__/**/*.ts",
  "src/**/__tests__/**/*.ts",
  "src/testing/**/*.ts",
  "src/test-utils/**/*.ts",
];

/**
 * `@failure` is the project-wide convention for Effect error-channel
 * documentation, registered here rather than in prose.
 */
const CUSTOM_JSDOC_TAGS = ["failure"];

/** Signatures describe parameters and returns; prose adds caller obligations. */
const documentationRules = {
  "jsdoc/require-param": "off",
  "jsdoc/require-returns": "off",
};

/**
 * The `max-non-trivial-classes-per-file` default exemption list covers
 * Effect's own tag-class factories (`Context.Tag`, `Data.TaggedError`, ...) but
 * not `@effect/rpc`'s `RpcMiddleware.Tag`, which is the same kind of factory:
 * `class X extends RpcMiddleware.Tag&lt;X&gt;()(name, opts) {}` declares a zero-body
 * Tag, not a real implementation. The per-method `AuthMiddleware` descriptors
 * are one such Tag per method, co-located by design, so the factory is added to
 * the exemption list workspace-wide.
 */
const TAG_CLASS_FACTORIES = [
  "Data.TaggedError",
  "Data.TaggedClass",
  "Data.Class",
  "Data.Error",
  "Schema.Class",
  "Schema.TaggedClass",
  "Schema.TaggedError",
  "Schema.TaggedRequest",
  "Context.Tag",
  "Context.Reference",
  "Effect.Service",
  "Effect.Tag",
  "RpcMiddleware.Tag",
];

/**
 * Effect.gen abandons its generator when a yielded effect fails, so a
 * `finally` block cannot provide reliable cleanup on that path. This rule
 * flags try/finally inside Effect-driven generators while leaving plain
 * generators alone, where iteration runs finally through `.return()`.
 * Effect.ensuring and Effect.acquireRelease preserve cleanup on every path.
 */
const genFinallyRule: Rule.RuleModule = {
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow try/finally inside Effect-driven generator bodies; use Effect.ensuring",
    },
    schema: [],
    messages: {
      genFinally:
        "try/finally inside Effect.gen — no finally code runs when a yielded effect fails; use Effect.ensuring",
    },
  },
  create(context) {
    const functionStack: FunctionNode[] = [];
    const enter = (node: FunctionNode): void => {
      functionStack.push(node);
    };
    const exit = (): void => {
      functionStack.pop();
    };
    return {
      FunctionDeclaration: enter,
      "FunctionDeclaration:exit": exit,
      FunctionExpression: enter,
      "FunctionExpression:exit": exit,
      ArrowFunctionExpression: enter,
      "ArrowFunctionExpression:exit": exit,
      TryStatement(node) {
        const fn = functionStack.at(-1);
        if (
          node.finalizer != null &&
          fn !== undefined &&
          isEffectDrivenGenerator(fn)
        ) {
          context.report({ node, messageId: "genFinally" });
        }
      },
    };
  },
};

const strictPlugins = {
  ...guard.configs.strict.plugins,
  "local-guard": { rules: { "gen-finally": genFinallyRule } },
};

/** One TypeScript project covers every linted file. */
const languageOptions = {
  ...guard.configs.strict.languageOptions,
  globals: {
    AbortController: "readonly",
    AbortSignal: "readonly",
    Buffer: "readonly",
    Response: "readonly",
    TextDecoder: "readonly",
    TextEncoder: "readonly",
    URL: "readonly",
    clearInterval: "readonly",
    clearTimeout: "readonly",
    console: "readonly",
    crypto: "readonly",
    fetch: "readonly",
    process: "readonly",
    setInterval: "readonly",
    setTimeout: "readonly",
  },
  parserOptions: {
    project: ["./tsconfig.check.json"],
    tsconfigRootDir: import.meta.dirname,
  },
};

/** Rules for `// eslint-disable` comments in every TypeScript file. */
const disableCommentRules = {
  files: ["**/*.ts", "**/*.cts", "**/*.mts"],
  languageOptions,
  plugins: { "eslint-comments": comments },
  rules: {
    "eslint-comments/require-description": ["error", { ignore: [] }],
  },
};

/**
 * A package's sections, with paths relative to the package: the strict preset
 * on its sources and root modules, plus the test and barrel-documentation
 * presets.
 */
function packageSections(maxLines = DEFAULT_MAX_LINES) {
  const strictRules = makeStrictRules(maxLines);
  return [
    {
      ...makeStrictBlock(PACKAGE_SOURCE_FILES, strictRules),
      ignores: ["**/*.spec.ts"],
      rules: {
        ...strictRules,
        "jsdoc/check-tag-names": ["error", { definedTags: CUSTOM_JSDOC_TAGS }],
      },
    },
    makeStrictBlock(TEST_SUPPORT_FILES, strictRules),
    {
      ...makePresetBlock(["src/**/index.ts"], guard.configs.documentation),
      rules: { ...guard.configs.documentation.rules, ...documentationRules },
    },
    disableCommentRules,
    configModuleRules,
  ];
}

/**
 * Sections for the root modules, the `images/` helper, and the Node-run
 * scripts under `scripts/`.
 * Owner specs stay together as their behavior coverage grows.
 */
function rootSections() {
  return [
    makeStrictBlock(
      ["*.ts", "images/*.ts", "scripts/**/*.ts"],
      makeStrictRules(DEFAULT_MAX_LINES),
    ),
    disableCommentRules,
    configModuleRules,
    {
      files: ["scripts/**/*.spec.ts"],
      rules: { "max-lines": "off" },
    },
  ];
}

/**
 * Sections for one agent image's tooling, with paths relative to its folder.
 * Image bootstrap programs run on Node built-ins before runtime packages are
 * installed. Their process fixtures use node:test's Promise-native boundary.
 */
function agentImageSections() {
  return [
    ...integrationSections(),
    {
      files: ["**/*.ts"],
      rules: {
        "agent-code-guard/async-keyword": "off",
        "agent-code-guard/promise-type": "off",
        "agent-code-guard/no-raw-throw-new-error": "off",
        "agent-code-guard/prefer-effect-platform": "off",
        "agent-code-guard/no-process-env-at-runtime": "off",
        "@typescript-eslint/no-floating-promises": [
          "error",
          {
            allowForKnownSafeCalls: [
              {
                from: "package",
                package: "node:test",
                name: ["describe", "it", "suite", "test"],
              },
            ],
          },
        ],
      },
    },
    {
      files: ["**/*.spec.ts"],
      rules: {
        "agent-code-guard/no-hardcoded-assertion-literals": "off",
        "sonarjs/no-os-command-from-path": "off",
      },
    },
  ];
}

/** Sections for process suites: the strict preset on every TypeScript file plus the integration-test preset on specs. */
function integrationSections() {
  return [
    makeStrictBlock(["**/*.ts"], makeStrictRules(DEFAULT_MAX_LINES)),
    makePresetBlock(["**/*.spec.ts"], guard.configs.integrationTests),
    {
      files: ["**/*.spec.ts"],
      rules: { "agent-code-guard/no-example-only-tests": "off" },
    },
    disableCommentRules,
    configModuleRules,
  ];
}

function makeStrictRules(maxLines: number) {
  return {
    ...guard.configs.strict.rules,
    "max-lines-per-function": [
      "error",
      { max: 120, skipBlankLines: true, skipComments: true, IIFEs: true },
    ],
    "sonarjs/max-lines-per-function": "off",
    "max-statements": "off",
    complexity: ["error", { max: 16 }],
    "sonarjs/cyclomatic-complexity": "off",
    "sonarjs/cognitive-complexity": ["error", 20],
    "max-nested-callbacks": ["error", 5],
    "agent-code-guard/no-vacuous-jsdoc": "error",
    ...documentationRules,
    "agent-code-guard/prefer-stepdown-function-order": "error",
    "agent-code-guard/require-stable-file-shell": "error",
    // The architecture analyzer owns deterministic file, folder, domain, and
    // workspace-package cycle detection in one cached whole-project pass.
    "import-x/no-cycle": "off",
    // The TypeScript-aware rule reports the same deprecated-symbol uses without
    // repeating Sonar's expensive type walk for every file.
    "sonarjs/deprecation": "off",
    "@typescript-eslint/naming-convention": [
      "error",
      {
        selector: ["classProperty", "objectLiteralProperty", "typeProperty"],
        modifiers: ["requiresQuotes"],
        format: null,
      },
      ...presetEntryOptions("@typescript-eslint/naming-convention"),
    ],
    "@typescript-eslint/no-invalid-void-type": [
      "error",
      {
        allowAsThisParameter: false,
        allowInGenericTypeArguments: [
          "Deferred.Deferred",
          "Effect.Effect",
          "Either.Either",
          "Exit.Exit",
          "Fiber.RuntimeFiber",
        ],
      },
    ],
    "agent-code-guard/max-non-trivial-classes-per-file": [
      "error",
      { max: 1, factories: TAG_CLASS_FACTORIES },
    ],
    "max-lines": [
      "error",
      { max: maxLines, skipBlankLines: true, skipComments: true },
    ],
    // Disabled: knip runs once at the workspace root (whole-monorepo)
    // via `pnpm lint`; per-package lint scripts run eslint only.
    "agent-code-guard/require-knip-in-lint": "off",
    // Effect and Option constructors intentionally return distinct closed variants
    // through one declared union. SonarJS treats those variants as inconsistent
    // return types even when TypeScript proves the public return type.
    "sonarjs/function-return-type": "off",
    // TypeScript's control-flow analysis owns these checks. SonarJS loses the
    // established narrowing for Effect Schema unions and generic array methods,
    // so it reports errors for statically typed object and string operands.
    "sonarjs/in-operator-type-error": "off",
    "sonarjs/argument-type": "off",
    // `sharnessd` takes `--help` or nothing, which needs no argument parser.
    "agent-code-guard/prefer-effect-platform": ["error", { disable: ["argv"] }],
    "local-guard/gen-finally": "error",
  };
}

/**
 * The options a strict preset rule entry carries after its severity, so a
 * workspace override can extend the preset instead of replacing it.
 */
function presetEntryOptions(rule: string): readonly unknown[] {
  const entry = guard.configs.strict.rules[rule];
  return Array.isArray(entry) ? entry.slice(1) : [];
}

function makeStrictBlock(files: readonly string[], rules: StrictRules) {
  return {
    files: [...files],
    languageOptions,
    plugins: strictPlugins,
    settings: guard.configs.strict.settings,
    rules,
  };
}

function makePresetBlock(files: readonly string[], preset: GuardPreset) {
  return {
    files: [...files],
    languageOptions,
    plugins: preset.plugins,
    rules: preset.rules,
  };
}

function isEffectDrivenGenerator(fn: FunctionNode): boolean {
  const call = fn.parent;
  if (!fn.generator || call.type !== "CallExpression") {
    return false;
  }
  return (
    call.arguments.some((argument) => argument === fn) &&
    isEffectGeneratorRunner(call.callee)
  );
}

/** True for `Effect.gen` and for the `Effect.fn(...)` call that wraps a body. */
function isEffectGeneratorRunner(callee: ESTree.Node): boolean {
  return (
    isEffectMember(callee, "gen") ||
    (callee.type === "CallExpression" && isEffectMember(callee.callee, "fn"))
  );
}

function isEffectMember(node: ESTree.Node, name: string): boolean {
  return (
    node.type === "MemberExpression" &&
    isIdentifier(node.object, "Effect") &&
    isIdentifier(node.property, name)
  );
}

function isIdentifier(node: ESTree.Node, name: string): boolean {
  return node.type === "Identifier" && node.name === name;
}

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

/**
 * Places a project's sections at its folder: every `files` and `ignores`
 * pattern is read relative to `folder`.
 */
function at<Section extends object>(
  folder: string,
  sections: readonly Section[],
): Section[] {
  const within = (patterns: readonly string[]) =>
    patterns.map((pattern) => `${folder}/${pattern}`);
  return sections.map((section) => ({
    ...section,
    ...("files" in section && isPatterns(section.files)
      ? { files: within(section.files) }
      : {}),
    ...("ignores" in section && isPatterns(section.ignores)
      ? { ignores: within(section.ignores) }
      : {}),
  }));
}

function isPatterns(value: unknown): value is readonly string[] {
  return Array.isArray(value);
}

/**
 * `plugins/openclaw/src/plugin.ts` keeps the OpenClaw registration and channel
 * callbacks together so a reader can see the complete host contract in one
 * place, which needs a higher line ceiling.
 */
const OPENCLAW_MAX_LINES = 1200;

/**
 * Process suites take the integration sections; the image scenarios under
 * `images/` take the agent-image sections for their Node test boundary.
 */
const integrationTestSections = [
  ...integrationSections().map((section) => ({
    ...section,
    ignores: ["images/**"],
  })),
  ...at("images", agentImageSections()),
];

export default [
  globalIgnores,
  ...rootSections(),
  ...packageSections(),
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
  ...at("plugins/openclaw", packageSections(OPENCLAW_MAX_LINES)),
  ...at("plugins/nanoclaw", packageSections()),
  ...at("images/openclaw-agent", agentImageSections()),
  ...at("images/nanoclaw-agent", agentImageSections()),
  ...at("tests/integration", integrationTestSections),
];
