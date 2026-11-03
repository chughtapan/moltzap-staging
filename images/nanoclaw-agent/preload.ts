/**
 * @file Registers the image's channel, gateway and session driver before
 * NanoClaw selects them. Runtime-loaded exports are checked before use.
 */
import { pathToFileURL } from "node:url";

import {
  type DriverCapabilities,
  installProcessSessionDriver,
  type SessionDriver,
  type SessionSpec,
} from "./session-processes.ts";

const APP_ROOT = "/opt/social-harness/nanoclaw/app";
const GATEWAY_PROVIDER_KIND = "social-harness-process";

type ModuleRecord = Readonly<Record<string, unknown>>;

await importModule("modules/index.js");
await importModule("channels/sharness.js");

const [registry, types, gatewayRegistry, hostLifecycle] = await Promise.all([
  importModule("drivers/driver-registry.js"),
  importModule("drivers/types.js"),
  importModule("gateway-providers/gateway-provider-registry.js"),
  importModule("host-lifecycle.js"),
]);
call(gatewayRegistry, "registerGatewayProvider", GATEWAY_PROVIDER_KIND, () =>
  Object.freeze({
    kind: GATEWAY_PROVIDER_KIND,
    contribute: () => Promise.resolve(Object.freeze({})),
  }),
);
installProcessSessionDriver({
  registerSessionDriver: (
    kind: string,
    factory: (policy: unknown) => SessionDriver,
  ) => {
    call(registry, "registerSessionDriver", kind, factory);
  },
  onHostShutdown: (stop: () => Promise<undefined>) => {
    call(hostLifecycle, "onHostShutdown", stop);
  },
  validateSpec: (
    spec: SessionSpec,
    policy: unknown,
    capabilities: DriverCapabilities,
  ) => {
    call(types, "validateSpec", spec, policy, capabilities);
  },
  specInvalid: (detail: string) => {
    const failure = call(types, "specInvalid", detail);
    return failure instanceof Error ? failure : new Error(detail);
  },
});

async function importModule(relativePath: string): Promise<ModuleRecord> {
  const loaded: unknown = await import(
    pathToFileURL(`${APP_ROOT}/dist/${relativePath}`).href
  );
  if (!isRecord(loaded)) {
    throw new Error(`NanoClaw module ${relativePath} did not load`);
  }
  return loaded;
}

function call(module: ModuleRecord, name: string, ...args: unknown[]): unknown {
  const member = module[name];
  if (typeof member !== "function") {
    throw new Error(`NanoClaw module does not export the function ${name}`);
  }
  const result: unknown = Reflect.apply(member, module, args);
  return result;
}

function isRecord(value: unknown): value is ModuleRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
