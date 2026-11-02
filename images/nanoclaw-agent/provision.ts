/**
 * @file Initializes the image's native NanoClaw agent group idempotently.
 * Runtime-loaded exports are checked before use.
 */
import { cp, mkdir, readdir } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const DEFAULT_APP_ROOT = "/opt/social-harness/nanoclaw/app";
const AGENT_GROUP_ID = "agent";
const PRINCIPAL_CHANNEL = "cli";
const PRINCIPAL_MESSAGING_GROUP_ID = "social-harness-principal";
const PRINCIPAL_PLATFORM_ID = "local";

/** One MCP server: remote over streamable HTTP, or a stdio command. */
export type McpServer =
  | { readonly name: string; readonly url: string }
  | {
      readonly name: string;
      readonly command: string;
      readonly args: readonly string[];
      readonly env: Readonly<Record<string, string>>;
    };

/** Seeds the native agent group and configures its model, workspace and MCP access. */
export interface NanoClawConfig {
  readonly agentName: string;
  readonly stateDirectory: string;
  readonly workspaceDirectory: string;
  readonly modelId?: string;
  readonly mcpServers: readonly McpServer[];
}

/** Where NanoClaw's compiled application lives. */
export interface ProvisionOptions {
  readonly appRoot?: string;
}

type ModuleRecord = Readonly<Record<string, unknown>>;

interface NanoClawModules {
  readonly agentGroups: ModuleRecord;
  readonly connection: ModuleRecord;
  readonly containerConfigs: ModuleRecord;
  readonly configModule: ModuleRecord;
  readonly groupInit: ModuleRecord;
  readonly messagingGroups: ModuleRecord;
  readonly migrations: ModuleRecord;
  readonly upgrade: ModuleRecord;
}

interface AgentGroup {
  readonly id: string;
  readonly folder: string;
  readonly row: ModuleRecord;
}

/**
 * Provisions the agent group and owner-local CLI gateway. Social Harness
 * conversations and sessions begin only when NanoClaw receives traffic.
 */
export async function provisionNanoClaw(
  config: NanoClawConfig,
  options: ProvisionOptions = {},
): Promise<undefined> {
  const modules = await importModules(options.appRoot ?? DEFAULT_APP_ROOT);
  const { connection } = modules;
  const db = await call(
    connection,
    "initDb",
    stringExport(modules.configModule, "CENTRAL_DB_PATH"),
    {
      role: "tool",
    },
  );
  try {
    await call(modules.migrations, "runMigrations", db, undefined, {
      mode: "auto",
    });
    const now = new Date().toISOString();
    const agent = await ensureAgentGroup(modules.agentGroups, config, now);
    await call(modules.groupInit, "initGroupFilesystem", agent.row, {
      provider: "claude",
    });
    await wirePrincipalGateway(modules.messagingGroups, agent.id, now);
    await seedWorkspace(
      config.workspaceDirectory,
      join(config.stateDirectory, "groups", agent.folder),
    );
    await configureContainer(modules.containerConfigs, agent.id, config);
    call(modules.upgrade, "writeUpgradeState", {
      via: "social-harness-agent-image",
      projectRoot: config.stateDirectory,
    });
  } finally {
    await call(connection, "closeDb");
  }
}

/** Loads the module barrel first to register the stock host's migrations. */
async function importModules(appRoot: string): Promise<NanoClawModules> {
  await importModule(appRoot, "modules/index.js");
  const [
    agentGroups,
    connection,
    containerConfigs,
    configModule,
    groupInit,
    messagingGroups,
    migrations,
    upgrade,
  ] = await Promise.all([
    importModule(appRoot, "db/agent-groups.js"),
    importModule(appRoot, "db/connection.js"),
    importModule(appRoot, "db/container-configs.js"),
    importModule(appRoot, "config.js"),
    importModule(appRoot, "group-init.js"),
    importModule(appRoot, "db/messaging-groups.js"),
    importModule(appRoot, "db/migrations/index.js"),
    importModule(appRoot, "upgrade-state.js"),
  ]);
  return {
    agentGroups,
    connection,
    containerConfigs,
    configModule,
    groupInit,
    messagingGroups,
    migrations,
    upgrade,
  };
}

async function ensureAgentGroup(
  agentGroups: ModuleRecord,
  config: NanoClawConfig,
  now: string,
): Promise<AgentGroup> {
  const existing: unknown = await call(
    agentGroups,
    "getAgentGroup",
    AGENT_GROUP_ID,
  );
  if (existing === undefined) {
    const row = {
      id: AGENT_GROUP_ID,
      name: config.agentName,
      folder: AGENT_GROUP_ID,
      agent_provider: null,
      created_at: now,
    };
    await call(agentGroups, "createAgentGroup", row);
    return { id: AGENT_GROUP_ID, folder: AGENT_GROUP_ID, row };
  }
  const agent = decodeAgentGroup(existing);
  if (agent.row.name === config.agentName) {
    return agent;
  }
  await call(agentGroups, "updateAgentGroup", agent.id, {
    name: config.agentName,
  });
  return { ...agent, row: { ...agent.row, name: config.agentName } };
}

async function wirePrincipalGateway(
  messagingGroups: ModuleRecord,
  agentGroupId: string,
  now: string,
): Promise<undefined> {
  const groupId = await ensurePrincipalGroup(messagingGroups, now);
  const wiring: unknown = await call(
    messagingGroups,
    "getMessagingGroupAgentByPair",
    groupId,
    agentGroupId,
  );
  if (wiring === undefined) {
    await call(messagingGroups, "createMessagingGroupAgent", {
      id: "social-harness-principal-agent",
      messaging_group_id: groupId,
      agent_group_id: agentGroupId,
      engage_mode: "pattern",
      engage_pattern: ".",
      sender_scope: "all",
      ignored_message_policy: "drop",
      session_mode: "agent-shared",
      priority: 0,
      created_at: now,
    });
  }
}

async function ensurePrincipalGroup(
  messagingGroups: ModuleRecord,
  now: string,
): Promise<string> {
  const existing: unknown = await call(
    messagingGroups,
    "getMessagingGroupByPlatform",
    PRINCIPAL_CHANNEL,
    PRINCIPAL_PLATFORM_ID,
    PRINCIPAL_CHANNEL,
  );
  if (existing !== undefined) {
    return stringField(existing, "id");
  }
  await call(messagingGroups, "createMessagingGroup", {
    id: PRINCIPAL_MESSAGING_GROUP_ID,
    channel_type: PRINCIPAL_CHANNEL,
    platform_id: PRINCIPAL_PLATFORM_ID,
    instance: PRINCIPAL_CHANNEL,
    name: "Social Harness principal",
    is_group: 0,
    unknown_sender_policy: "public",
    created_at: now,
  });
  return PRINCIPAL_MESSAGING_GROUP_ID;
}

/** Preserves existing agent files. A missing seed copies nothing. */
async function seedWorkspace(
  sourceDirectory: string,
  targetDirectory: string,
): Promise<undefined> {
  await mkdir(targetDirectory, { recursive: true });
  const entries = await seedEntries(sourceDirectory);
  for (const entry of entries) {
    await cp(
      join(sourceDirectory, entry.name),
      join(targetDirectory, entry.name),
      {
        recursive: entry.isDirectory(),
        force: false,
        errorOnExist: false,
      },
    );
  }
}

async function seedEntries(
  sourceDirectory: string,
): Promise<ReadonlyArray<{ name: string; isDirectory(): boolean }>> {
  try {
    return await readdir(sourceDirectory, { withFileTypes: true });
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") {
      return [];
    }
    throw error;
  }
}

async function configureContainer(
  containerConfigs: ModuleRecord,
  agentGroupId: string,
  config: NanoClawConfig,
): Promise<undefined> {
  await call(containerConfigs, "updateContainerConfigScalars", agentGroupId, {
    assistant_name: config.agentName,
    ...(config.modelId === undefined ? {} : { model: config.modelId }),
  });
  await call(
    containerConfigs,
    "updateContainerConfigJson",
    agentGroupId,
    "mcp_servers",
    mcpServerRecord(config.mcpServers),
  );
}

function mcpServerRecord(
  servers: readonly McpServer[],
): Readonly<Record<string, unknown>> {
  return Object.fromEntries(
    servers.map((server) => [
      server.name,
      "url" in server
        ? { type: "http", url: server.url }
        : { command: server.command, args: server.args, env: server.env },
    ]),
  );
}

function decodeAgentGroup(value: unknown): AgentGroup {
  if (!isRecord(value)) {
    throw new Error("NanoClaw returned an agent group that is not an object");
  }
  return {
    id: stringField(value, "id"),
    folder: stringField(value, "folder"),
    row: value,
  };
}

function stringField(value: unknown, name: string): string {
  const field = isRecord(value) ? value[name] : undefined;
  if (typeof field !== "string") {
    throw new Error(`NanoClaw returned a row without a string ${name}`);
  }
  return field;
}

function stringExport(module: ModuleRecord, name: string): string {
  const value = module[name];
  if (typeof value !== "string") {
    throw new Error(`NanoClaw module does not export the string ${name}`);
  }
  return value;
}

async function importModule(
  appRoot: string,
  relativePath: string,
): Promise<ModuleRecord> {
  const loaded: unknown = await import(
    pathToFileURL(`${appRoot}/dist/${relativePath}`).href
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
