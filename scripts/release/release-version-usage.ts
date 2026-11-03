/** @file Finds code that may read release metadata omitted from image keys. */
import ts from "typescript";
import { erasedImport, runtimeCall } from "./module-graph.ts";

/** Detects runtime manifest and environment reads whose release fields image keys omit. */
export function readsManifestVersion(text: string): boolean {
  const source = ts.createSourceFile(
    "module.ts",
    text,
    ts.ScriptTarget.Latest,
    true,
  );
  const manifests = new Set<string>();
  let reads = false;
  function visit(node: ts.Node): void {
    reads =
      syntaxVersionRead(node, manifests) ||
      reads ||
      manifestFileRead(node) ||
      npmVersionRead(node);
    ts.forEachChild(node, visit);
  }
  visit(source);
  return reads;
}

function syntaxVersionRead(node: ts.Node, manifests: Set<string>): boolean {
  if (ts.isImportDeclaration(node)) {
    return manifestImport(node, manifests);
  }
  if (
    ts.isVariableDeclaration(node) &&
    node.initializer !== undefined &&
    manifestExpression(node.initializer, manifests)
  ) {
    return manifestBinding(node.name, manifests);
  }
  return (
    (ts.isExportAssignment(node) &&
      manifestExpression(node.expression, manifests)) ||
    expressionVersionRead(node, manifests)
  );
}

/** Filesystem manifest reads conservatively consume version metadata regardless of later parsing. */

function manifestFileRead(node: ts.Node): boolean {
  if (
    !ts.isCallExpression(node) ||
    !["readFile", "readFileSync"].includes(callName(node.expression))
  ) {
    return false;
  }
  const argument = node.arguments[0];
  if (argument === undefined) {
    return false;
  }
  const target = manifestPath(argument);
  return target !== undefined && manifestSpecifier(target);
}

function manifestPath(node: ts.Expression): ts.Expression | undefined {
  if (ts.isNewExpression(node) && callName(node.expression) === "URL") {
    return node.arguments?.[0];
  }
  if (
    ts.isCallExpression(node) &&
    ["join", "resolve"].includes(callName(node.expression))
  ) {
    return node.arguments.find((argument) => manifestSpecifier(argument));
  }
  return node;
}

function callName(node: ts.Expression): string {
  if (ts.isIdentifier(node)) {
    return node.text;
  }
  return ts.isPropertyAccessExpression(node) ? node.name.text : "";
}

function npmVersionRead(node: ts.Node): boolean {
  if (
    ts.isVariableDeclaration(node) &&
    node.initializer?.getText() === "process.env" &&
    ts.isObjectBindingPattern(node.name)
  ) {
    return node.name.elements.some(
      (item) =>
        (item.propertyName ?? item.name).getText().replace(/["']/gu, "") ===
        "npm_package_version",
    );
  }
  if (ts.isPropertyAccessExpression(node)) {
    return (
      node.expression.getText() === "process.env" &&
      node.name.text === "npm_package_version"
    );
  }
  return npmElementRead(node);
}

function npmElementRead(node: ts.Node): boolean {
  return (
    ts.isElementAccessExpression(node) &&
    node.expression.getText() === "process.env" &&
    ts.isStringLiteral(node.argumentExpression) &&
    node.argumentExpression.text === "npm_package_version"
  );
}

function manifestImport(
  node: ts.ImportDeclaration,
  manifests: Set<string>,
): boolean {
  if (
    erasedImport(node.importClause) ||
    !manifestSpecifier(node.moduleSpecifier)
  ) {
    return false;
  }
  const clause = node.importClause;
  if (clause?.name !== undefined) {
    manifests.add(clause.name.text);
  }
  const bindings = clause?.namedBindings;
  if (bindings === undefined) {
    return false;
  }
  if (ts.isNamespaceImport(bindings)) {
    manifests.add(bindings.name.text);
    return false;
  }
  return bindings.elements.some(
    (item) =>
      !item.isTypeOnly &&
      ["version", "sharnessWireVersion", "default"].includes(
        (item.propertyName ?? item.name).text,
      ),
  );
}

function manifestBinding(
  name: ts.BindingName,
  manifests: Set<string>,
): boolean {
  if (ts.isIdentifier(name)) {
    manifests.add(name.text);
    return false;
  }
  return (
    ts.isObjectBindingPattern(name) &&
    name.elements.some((item) =>
      ["version", "sharnessWireVersion"].includes(
        (item.propertyName ?? item.name).getText().replace(/["']/gu, ""),
      ),
    )
  );
}

function expressionVersionRead(
  node: ts.Node,
  manifests: ReadonlySet<string>,
): boolean {
  return manifestExport(node) || versionAccess(node, manifests);
}

function versionAccess(node: ts.Node, manifests: ReadonlySet<string>): boolean {
  if (ts.isPropertyAccessExpression(node)) {
    return (
      ["version", "sharnessWireVersion"].includes(node.name.text) &&
      manifestExpression(node.expression, manifests)
    );
  }
  if (
    ts.isElementAccessExpression(node) &&
    ts.isStringLiteral(node.argumentExpression)
  ) {
    return (
      ["version", "sharnessWireVersion"].includes(
        node.argumentExpression.text,
      ) && manifestExpression(node.expression, manifests)
    );
  }
  return false;
}

function manifestExpression(
  node: ts.Node,
  manifests: ReadonlySet<string>,
): boolean {
  if (ts.isIdentifier(node)) {
    return manifests.has(node.text);
  }
  if (ts.isAwaitExpression(node) || ts.isParenthesizedExpression(node)) {
    return manifestExpression(node.expression, manifests);
  }
  if (ts.isPropertyAccessExpression(node) && node.name.text === "default") {
    return manifestExpression(node.expression, manifests);
  }
  return ts.isCallExpression(node) && manifestCall(node);
}

function manifestCall(node: ts.CallExpression): boolean {
  return runtimeCall(node) && node.arguments.some(manifestSpecifier);
}

function manifestExport(node: ts.Node): boolean {
  if (
    !ts.isExportDeclaration(node) ||
    node.isTypeOnly ||
    node.moduleSpecifier === undefined
  ) {
    return false;
  }
  if (!manifestSpecifier(node.moduleSpecifier)) {
    return false;
  }
  const bindings = node.exportClause;
  if (bindings === undefined) {
    return true;
  }
  return (
    ts.isNamedExports(bindings) &&
    bindings.elements.some(
      (item) =>
        !item.isTypeOnly &&
        ["version", "sharnessWireVersion", "default"].includes(
          (item.propertyName ?? item.name).text,
        ),
    )
  );
}

function manifestSpecifier(node: ts.Node): boolean {
  return ts.isStringLiteral(node) && /(?:^|\/)package\.json$/u.test(node.text);
}
