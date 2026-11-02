#!/usr/bin/env node
/**
 * @file Checks version copies that package managers cannot reconcile: the
 * `images/` helper dependencies readers install outside the workspace,
 * published Node engine ranges, the development Node pin, peer support floors,
 * and the Effect source tag.
 */
import { readFileSync } from "node:fs";
import path from "node:path";
import { parse } from "yaml";
import {
  isJsonObject,
  type JsonObject,
  objectAt,
  readJsonObject,
  stringAt,
} from "./json.ts";
import { workspacePackages } from "./packages.ts";

const INSTALLED_SECTIONS = [
  "dependencies",
  "devDependencies",
  "optionalDependencies",
];

const EFFECT_SCRIPT = "scripts/setup/prepare-effect.sh";
const HELPERS_MANIFEST = "images/package.json";

const repo = process.cwd();
const packages = workspacePackages(repo);
const failures: string[] = [];

/** One package manifest and the repository-relative path failures name. */
interface Manifest {
  readonly where: string;
  readonly manifest: JsonObject;
}

/** One declared dependency of one manifest. */
interface Declaration {
  readonly where: string;
  readonly section: string;
  readonly name: string;
  readonly value: unknown;
}

type Version = readonly [number, number, number];

/**
 * Comparator operators, longest first so `>=` is not read as `>`. The empty
 * operator means equality.
 */
const OPERATORS: ReadonlyArray<readonly [string, (order: number) => boolean]> =
  [
    [">=", (order) => order >= 0],
    ["<=", (order) => order <= 0],
    [">", (order) => order > 0],
    ["<", (order) => order < 0],
    ["=", (order) => order === 0],
    ["", (order) => order === 0],
  ];

/** One semver comparator: an operator followed by a version. */
interface Comparator {
  readonly holds: (version: Version) => boolean;
}

function main(): void {
  const workspace = readWorkspace();
  const catalog = stringRecord(workspace, "catalog");
  const overrides = stringRecord(workspace, "overrides");
  const root = manifestAt("package.json");
  const workspaceManifests = [
    root,
    ...packages.map(({ dir }) => manifestAt(`${dir}/package.json`)),
  ];
  const helpers = manifestAt(HELPERS_MANIFEST);

  checkInstalledSpecifiers(workspaceManifests);
  checkPeerFloors(workspaceManifests, catalog);
  checkHelperCopies(helpers, catalog);
  checkEffectCheckout(overrides);
  checkNodeRange(root, helpers);

  if (failures.length > 0) {
    console.error("[check-dependency-versions] FAIL");
    for (const failure of failures) {
      console.error(`  ${failure}`);
    }
    process.exit(1);
  }
  console.log(
    `[check-dependency-versions] OK — ${Object.keys(catalog).length} catalog entries, ${workspaceManifests.length} workspace manifests and ${HELPERS_MANIFEST}`,
  );
}

function readWorkspace(): JsonObject {
  const value: unknown = parse(
    readFileSync(path.join(repo, "pnpm-workspace.yaml"), "utf8"),
  );
  if (!isJsonObject(value)) {
    failures.push("pnpm-workspace.yaml: expected a mapping");
    return {};
  }
  return value;
}

function stringRecord(
  source: JsonObject,
  key: string,
): Readonly<Record<string, string>> {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(objectAt(source, key))) {
    if (typeof value === "string") {
      out[name] = value;
    } else {
      failures.push(`pnpm-workspace.yaml: ${key}.${name} must be a string`);
    }
  }
  return out;
}

function manifestAt(where: string): Manifest {
  return { where, manifest: readJsonObject(path.join(repo, where)) };
}

/** Requires workspace manifests to take npm versions from the catalog. */
function checkInstalledSpecifiers(
  manifests: readonly Manifest[],
): ReadonlySet<string> {
  const declarations = manifests.flatMap((manifest) =>
    declarationsOf(manifest, INSTALLED_SECTIONS),
  );
  for (const { where, section, name, value } of declarations) {
    if (value !== "catalog:" && value !== "workspace:*") {
      failures.push(
        `${where}: ${section}.${name} is ${JSON.stringify(value)}; declare "catalog:" and set the range in the pnpm-workspace.yaml catalog`,
      );
    }
  }
  return new Set(
    declarations
      .filter((declaration) => declaration.value === "catalog:")
      .map((declaration) => declaration.name),
  );
}

/** Requires the tested catalog pin to satisfy each manifest's peer support floor. */
function checkPeerFloors(
  manifests: readonly Manifest[],
  catalog: Readonly<Record<string, string>>,
): void {
  const peers = manifests.flatMap((manifest) =>
    declarationsOf(manifest, ["peerDependencies"]),
  );
  for (const { where, name, value } of peers) {
    const floor =
      typeof value === "string" ? parseComparator(value) : undefined;
    const pin = parseVersion(catalog[name] ?? "");
    if (floor === undefined || pin === undefined || !floor.holds(pin)) {
      failures.push(
        `${where}: peerDependencies.${name} is ${JSON.stringify(value)}; the catalog pin ${catalog[name] ?? "(missing)"} must be an exact version inside it`,
      );
    }
  }
}

/** Requires the independently installed helper manifest to copy catalog ranges. */
function checkHelperCopies(
  helpers: Manifest,
  catalog: Readonly<Record<string, string>>,
): void {
  for (const { where, section, name, value } of declarationsOf(
    helpers,
    INSTALLED_SECTIONS,
  )) {
    if (value !== catalog[name]) {
      failures.push(
        `${where}: ${section}.${name} is ${JSON.stringify(value)}, expected the catalog range ${JSON.stringify(catalog[name] ?? "missing")}`,
      );
    }
  }
}

function declarationsOf(
  manifest: Manifest,
  sections: readonly string[],
): readonly Declaration[] {
  return sections.flatMap((section) =>
    Object.entries(objectAt(manifest.manifest, section)).map(
      ([name, value]) => ({ where: manifest.where, section, name, value }),
    ),
  );
}

function checkEffectCheckout(
  overrides: Readonly<Record<string, string>>,
): void {
  const tag = /^tag="effect@([^"]+)"$/m.exec(
    readFileSync(path.join(repo, EFFECT_SCRIPT), "utf8"),
  )?.[1];
  if (tag !== overrides.effect) {
    failures.push(
      `${EFFECT_SCRIPT}: clones effect@${tag ?? "missing"}, expected the pnpm-workspace.yaml override effect@${overrides.effect ?? "missing"}; update tag and expected together`,
    );
  }
}

function checkNodeRange(root: Manifest, helpers: Manifest): void {
  const nodeRange = stringAt(objectAt(root.manifest, "engines"), "node");
  if (nodeRange === undefined) {
    failures.push(
      "package.json: engines.node is missing; it is the supported Node.js range",
    );
    return;
  }
  checkEngineCopies(nodeRange, helpers);
  checkNodePin(nodeRange);
}

/**
 * Requires published and helper manifests to carry the root Node range.
 * Other manifests may omit it, but any declared range must match.
 */
function checkEngineCopies(nodeRange: string, helpers: Manifest): void {
  const copies = [
    ...packages.map(({ dir, published }) => ({
      required: published,
      ...manifestAt(`${dir}/package.json`),
    })),
    { required: true, ...helpers },
  ];
  for (const { where, manifest, required } of copies) {
    const node = stringAt(objectAt(manifest, "engines"), "node");
    if ((required || node !== undefined) && node !== nodeRange) {
      failures.push(
        `${where}: engines.node is "${node ?? "missing"}", expected the root package.json range "${nodeRange}"`,
      );
    }
  }
}

function checkNodePin(nodeRange: string): void {
  const pin = readFileSync(path.join(repo, ".node-version"), "utf8").trim();
  const version = parseVersion(pin);
  const comparators = nodeRange.split(/\s+/).map(parseComparator);
  if (version === undefined || comparators.includes(undefined)) {
    failures.push(
      `.node-version "${pin}" or engines.node "${nodeRange}" is not a version and a space-separated comparator list`,
    );
    return;
  }
  if (!comparators.every((c) => c?.holds(version) === true)) {
    failures.push(
      `.node-version: ${pin} is outside the root engines.node range "${nodeRange}"`,
    );
  }
}

function parseComparator(text: string): Comparator | undefined {
  const entry = OPERATORS.find(([operator]) => text.startsWith(operator));
  const bound = parseVersion(text.slice(entry?.[0].length ?? 0));
  if (entry === undefined || bound === undefined) {
    return undefined;
  }
  const [, accepts] = entry;
  return { holds: (version) => accepts(compareVersions(version, bound)) };
}

function compareVersions(left: Version, right: Version): number {
  const index = left.findIndex((part, i) => part !== right[i]);
  return index === -1 ? 0 : (left[index] ?? 0) - (right[index] ?? 0);
}

function parseVersion(text: string): Version | undefined {
  const match = /^v?(\d+)(?:\.(\d+))?(?:\.(\d+))?$/.exec(text);
  if (match === null) {
    return undefined;
  }
  return [Number(match[1]), Number(match[2] ?? 0), Number(match[3] ?? 0)];
}

main();
