/** @file Inventories the named context inputs of root-context image recipes. */
import { absurd } from "effect";
import path from "node:path";
import { ImageInputError, readJson, type Tree } from "./image-input-tree.ts";

/** The installed runtime shipped by a root-context image recipe. */
export type DockerRuntime = "service" | "daemon" | "host";

const DAEMON_STAGE: ReadonlySet<string> = new Set([
  "COPY --from=build --chown=node:node /deploy/ /opt/social-harness/",
  'ENV PATH="/opt/social-harness/node_modules/.bin:${PATH}"',
  "USER node",
  'ENTRYPOINT ["sharnessd"]',
]);

const HOST_COPIES: ReadonlySet<string> = new Set([
  "COPY --from=build --chown=node:node /deploy/ /opt/social-harness/",
  "COPY --from=build /build/images/openclaw-agent/ /opt/social-harness/agent/",
]);

const HOST_PRODUCTION_PACKAGES =
  "npm pkg delete --prefix images/openclaw-agent devDependencies && npm pkg delete --prefix plugins/openclaw peerDependencies.openclaw peerDependenciesMeta.openclaw";

/**
 * Inventories named context sources after validating every instruction.
 * Named roots are inventoried conservatively; ignore rules bind as a fixed input.
 * Stage copies consume the already inventoried build output.
 */
export function dockerContextInputs(
  tree: Tree,
  file: string,
  service: string,
  runtime: DockerRuntime,
): string[] {
  const inputs: string[] = [];
  let stage = 0;
  const commands = buildCommands(tree, service, runtime);
  const lines = dockerLines(tree.read(file), file);
  for (const line of lines.filter((line) => !ignored(line.text))) {
    stage = instructionInputs(
      tree,
      line,
      { stage, service, runtime, commands },
      inputs,
    );
  }
  if (stage !== 2) {
    refuse({ file, number: lines.length, text: "expected two FROM stages" });
  }
  return inputs;
}

interface DockerLine {
  readonly file: string;
  readonly number: number;
  readonly text: string;
}

interface DockerStage {
  readonly stage: number;
  readonly service: string;
  readonly runtime: DockerRuntime;
  readonly commands: ReadonlySet<string>;
}

/** Joins Docker continuations before validating instructions, retaining their first line number. */
function dockerLines(source: string, file: string): DockerLine[] {
  const lines: DockerLine[] = [];
  let pending = "";
  let number = 1;
  for (const [index, raw] of source.split(/\r?\n/u).entries()) {
    if (ignored(raw.trim())) {
      continue;
    }
    if (pending === "") {
      number = index + 1;
    }
    pending += raw.trim();
    if (pending.endsWith("\\")) {
      pending = pending.slice(0, -1).trimEnd() + " ";
    } else {
      lines.push({ file, number, text: pending });
      pending = "";
    }
  }
  if (pending !== "") {
    refuse({ file, number, text: pending });
  }
  return lines;
}

function ignored(line: string): boolean {
  return (
    line === "" ||
    (line.startsWith("#") && !/^#\s*(?:syntax|escape)\s*=/iu.test(line))
  );
}

function buildCommands(
  tree: Tree,
  service: string,
  runtime: DockerRuntime,
): ReadonlySet<string> {
  const manager = readJson(tree, "package.json").packageManager;
  return new Set([
    `corepack enable && corepack prepare ${String(manager)} --activate`,
    `pnpm install --frozen-lockfile --filter ${service}...`,
    `pnpm nx run ${service}:build`,
    ...(runtime === "host" ? [HOST_PRODUCTION_PACKAGES] : []),
    `pnpm deploy --filter ${service} --prod --legacy /deploy`,
  ]);
}

function instructionInputs(
  tree: Tree,
  line: DockerLine,
  state: DockerStage,
  inputs: string[],
): number {
  if (line.text.startsWith("FROM ")) {
    return nextStage(line, state);
  }
  if (line.text.startsWith("COPY ") && state.stage === 1) {
    inputs.push(...contextCopy(tree, line));
  } else if (!allowedInstruction(line.text, state)) {
    refuse(line);
  }
  return state.stage;
}

function nextStage(line: DockerLine, state: DockerStage): number {
  const { stage, runtime } = state;
  if (runtime === "host" && stage === 1) {
    if (line.text !== "FROM ${OPENCLAW_BASE_IMAGE} AS agent") {
      refuse(line);
    }
    return 2;
  }
  const pinned = /^FROM node:[\w.-]+@sha256:[a-f\d]{64}(?: AS build)?$/u.test(
    line.text,
  );
  if (
    !pinned ||
    stage >= 2 ||
    line.text.endsWith(" AS build") !== (stage === 0)
  ) {
    refuse(line);
  }
  return stage + 1;
}

function allowedInstruction(line: string, state: DockerStage): boolean {
  if (state.stage === 0 && state.runtime === "host") {
    return /^ARG OPENCLAW_BASE_IMAGE=[\w./-]+@sha256:[a-f\d]{64}$/u.test(line);
  }
  if (state.stage === 1) {
    return state.commands.has(line.slice(4))
      ? line.startsWith("RUN ")
      : [
          "WORKDIR /build",
          "ENV HUSKY=0 NX_DAEMON=false NX_NO_CLOUD=true",
        ].includes(line);
  }
  if (state.stage !== 2) {
    return false;
  }
  if (
    /^LABEL org\.opencontainers\.image\.(?:source|description)="[^"\\$`]+"$/u.test(
      line,
    )
  ) {
    return true;
  }
  switch (state.runtime) {
    case "service":
      return serviceStage(line, state.service);
    case "daemon":
      return DAEMON_STAGE.has(line);
    case "host":
      return (
        HOST_COPIES.has(line) ||
        /^(?:USER|ARG|LABEL|ENV|WORKDIR|ENTRYPOINT) /u.test(line) ||
        (line.startsWith("RUN ") && !/^RUN\s+--/u.test(line))
      );
    default:
      return absurd(state.runtime);
  }
}

function contextCopy(tree: Tree, line: DockerLine): string[] {
  const sources = line.text.slice(5).split(/\s+/u);
  const destination = sources.pop() ?? "";
  if (
    sources.length === 0 ||
    !sources.every(namedPath) ||
    !copyDestination(tree, sources, destination)
  ) {
    refuse(line);
  }
  return sources.flatMap((source) => copiedFiles(tree, source, line));
}

function copyDestination(
  tree: Tree,
  sources: readonly string[],
  destination: string,
): boolean {
  if (destination === "./") {
    return sources.every(
      (source) => !source.includes("/") && tree.files.has(source),
    );
  }
  return (
    sources.length === 1 &&
    namedPath(destination) &&
    destination.replace(/\/$/u, "") === sources[0]?.replace(/\/$/u, "")
  );
}

function copiedFiles(tree: Tree, source: string, line: DockerLine): string[] {
  const name = source.replace(/\/$/u, "");
  const files = [...tree.files.keys()].filter(
    (candidate) => candidate === name || candidate.startsWith(`${name}/`),
  );
  if (files.length === 0) {
    refuse(line);
  }
  return files.filter((candidate) => candidate !== "pnpm-lock.yaml");
}

function namedPath(value: string): boolean {
  return (
    /^[\w.-]+(?:\/[\w.-]+)*\/?$/u.test(value) &&
    value.split("/").every((segment) => segment !== "." && segment !== "..") &&
    !value.startsWith("-") &&
    path.posix.normalize(value) === value
  );
}

function serviceStage(line: string, service: string): boolean {
  const name = service.split("/").at(-1) ?? "";
  return (
    [
      "COPY --from=build /deploy/ /app/",
      "COPY --from=build /build/LICENSE /build/NOTICE /app/",
      "WORKDIR /app",
      "USER node",
      'ENV PATH="/app/node_modules/.bin:${PATH}"',
      `ENTRYPOINT ["/app/bin/sharness-${name}"]`,
    ].includes(line) ||
    /^EXPOSE [1-9]\d{0,4}$/u.test(line) ||
    new RegExp(
      `^ENV SHARNESS_${name.toUpperCase()}_HOST=0\\.0\\.0\\.0 SHARNESS_${name.toUpperCase()}_PORT=[1-9]\\d{0,4}$`,
      "u",
    ).test(line)
  );
}

function refuse(line: DockerLine): never {
  throw new ImageInputError({
    message: `Unsupported Dockerfile form in ${line.file} line ${line.number}: ${line.text}. Docs: https://docs.moltzap.xyz/guides/contributing#unsupported-dockerfile-form`,
  });
}
