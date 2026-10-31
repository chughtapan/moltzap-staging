/** @file Argument diagnostics and discoverability through the MCP HTTP interface. */

import { scopedLive as it } from "@effect/vitest";
import {
  CLIENT_CAPABILITIES_META_KEY,
  CLIENT_INFO_META_KEY,
  PROTOCOL_VERSION_META_KEY,
  ProtocolErrorCode,
} from "@modelcontextprotocol/server";
import { Effect, Option, Schema, SchemaAST } from "effect";
import { describe, expect } from "vitest";
import { requestId } from "../../__tests__/collective-operation-fixtures.js";
import { openInboxFixture } from "../../__tests__/inbox-fixture.js";
import {
  mcpIdentity,
  mcpTestCredential,
  mcpTestHeaders,
} from "../../__tests__/mcp-http-fixtures.js";
import { stateDirectory } from "../../__tests__/store-schema-fixtures.js";
import { FirewallPolicyError } from "../../firewall/config.js";
import { decodeHarnessSendErrorData } from "../../host-protocol/operations.js";
import { AgentName, PrincipalId } from "../../identity/identifiers.js";
import { Identity } from "../../identity/local.js";
import { OperationId } from "../../identity/registry/contract.js";
import {
  type HarnessMcpOperations,
  makeHarnessMcpHttpHandler,
} from "./index.js";

const unused = () => Effect.dieMessage("invalid input reached an operation");
const operations: HarnessMcpOperations = {
  readStatus: () => Effect.succeed({ kind: "unregistered" }),
  readInboxSummary: () =>
    Effect.succeed({ pendingCount: 0, newestSequence: 0 }),
  register: unused,
  searchConversations: unused,
  readConversation: unused,
  readEvent: unused,
  readInbox: unused,
  send: unused,
  acknowledgeDelivery: unused,
};

const acquireHandler = (selected: HarnessMcpOperations, registered = true) =>
  Effect.gen(function* () {
    const store = yield* openInboxFixture(stateDirectory());
    const handler = yield* makeHarnessMcpHttpHandler({
      eventStore: store,
      credential: mcpTestCredential,
      implementation: { name: "tool-contract-test", version: "1" },
      operations: selected,
    }).pipe(Effect.provideService(Identity, mcpIdentity(registered)));
    yield* Effect.addFinalizer(() =>
      Effect.tryPromise(() => handler.close()).pipe(Effect.orDie),
    );
    return handler;
  });

const responseSchema = Schema.parseJson(
  Schema.Struct({
    error: Schema.optional(
      Schema.Struct({
        code: Schema.Number,
        message: Schema.String,
        data: Schema.optional(Schema.Unknown),
      }),
    ),
    result: Schema.optional(Schema.Unknown),
  }),
);
type Handler = Effect.Effect.Success<ReturnType<typeof acquireHandler>>;

/** Uses the modern per-request envelope without opening a subscription or socket. */
function request(
  handler: Handler,
  method: string,
  params: Readonly<Record<string, unknown>>,
  metadata: Readonly<Record<string, unknown>> = {},
) {
  return Effect.tryPromise(() =>
    handler.fetch(
      new Request("http://127.0.0.1/mcp", {
        method: "POST",
        headers: {
          ...mcpTestHeaders,
          accept: "application/json, text/event-stream",
          "content-type": "application/json",
          "mcp-method": method,
          ...(typeof params.name === "string"
            ? { "mcp-name": params.name }
            : {}),
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method,
          params: {
            ...params,
            _meta: {
              ...metadata,
              [CLIENT_INFO_META_KEY]: {
                name: "tool-contract-test",
                version: "1",
              },
              [CLIENT_CAPABILITIES_META_KEY]: {},
              [PROTOCOL_VERSION_META_KEY]: "2026-07-28",
            },
          },
        }),
      }),
    ),
  ).pipe(
    Effect.flatMap((response) => Effect.tryPromise(() => response.text())),
    Effect.flatMap(Schema.decodeUnknown(responseSchema)),
  );
}

/** Malformed calls and every field their diagnostics name: only the union arm the input selects. */
const invalidCalls = [
  { name: "status", arguments: { unexpected: true }, paths: [["unexpected"]] },
  { name: "read_event", arguments: { eventId: "bad" }, paths: [["eventId"]] },
  { name: "acknowledge_delivery", arguments: {}, paths: [["deliveryToken"]] },
  {
    name: "policy_update",
    arguments: {
      add: [
        { id: "invalid-direction", applies: "sideways", text: "Be brief." },
      ],
    },
    paths: [["add", 0, "applies"]],
  },
];
const invalidInvocationCalls = [
  {
    name: "send_message",
    arguments: { input: { to: "agent:bob", text: 123 } },
    paths: [["input", "text"]],
  },
  {
    name: "send_message",
    arguments: { input: { to: "group:agent:a,agent:b,agent:c", text: "hi" } },
    paths: [["input", "to"]],
  },
  {
    name: "send_message",
    arguments: {
      input: {
        to: "agent:bob",
        collectiveResponse: {
          operationId: requestId,
          action: "accept",
          content: { choices: [1] },
        },
      },
    },
    paths: [["input", "collectiveResponse", "content", "choices", 0]],
  },
];
/** Malformed registration fields and the schema whose description each error message states for its field. */
const invalidRegistrations = [
  { field: "operationId", value: "bad", schema: OperationId },
  { field: "principalId", value: "bad", schema: PrincipalId },
  {
    field: "operationId",
    value: `opn_${"A".repeat(22)}\n`,
    schema: OperationId,
  },
  { field: "operationId", value: `prn_${"A".repeat(22)}`, schema: OperationId },
  {
    field: "principalId",
    value: `prn_${"A".repeat(21)}B`,
    schema: PrincipalId,
  },
  { field: "operationId", value: `opn_${"A".repeat(21)}`, schema: OperationId },
  { field: "agentName", value: "al", schema: AgentName },
  { field: "agentName", value: "Bad_Name", schema: AgentName },
];
const refusedPolicyEdit = new FirewallPolicyError({
  reason: "invalid-rule",
  problem: "duplicate-id",
  ruleId: "meeting-booking",
  path: ["add", 0],
});
/** Operation failures and the data each MCP error carries. */
const semanticFailures = [
  {
    name: "read_conversation",
    arguments: { continuation: `${"A".repeat(42)}B` },
    data: { reason: "invalid-continuation" },
  },
  {
    name: "search_conversations",
    arguments: { afterAddress: "group:carol,bob,alice" },
    data: { reason: "invalid-address" },
  },
  {
    name: "policy_update",
    arguments: {
      add: [
        {
          id: "meeting-booking",
          applies: "both",
          text: "Each member holds a slot before confirming it.",
          guidance: "Hold a slot before you confirm one.",
        },
      ],
    },
    data: {
      reason: "invalid-rule",
      problem: "duplicate-id",
      ruleId: "meeting-booking",
      path: ["add", 0],
    },
  },
];

const catalogSchema = Schema.Struct({
  tools: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      description: Schema.String,
      inputSchema: Schema.Record({ key: Schema.String, value: Schema.Unknown }),
    }),
  ),
});

const issuePathsSchema = Schema.Struct({
  issues: Schema.Array(
    Schema.Struct({
      path: Schema.Array(Schema.Union(Schema.String, Schema.Number)),
    }),
  ),
});

/** The diagnostics name exactly `paths`, and the error's message names each path too. */
function checkDiagnostics(
  paths: ReadonlyArray<ReadonlyArray<string | number>>,
  error?: { readonly message: string; readonly data?: unknown },
) {
  return Schema.decodeUnknown(issuePathsSchema)(error?.data).pipe(
    Effect.tap((diagnostics) =>
      Effect.sync(() => {
        expect(diagnostics.issues.map((issue) => issue.path)).toEqual(paths);
        for (const path of paths) {
          expect(error?.message).toContain(`${path.join(".")}: `);
        }
      }),
    ),
  );
}

function checkDiagnostic(data: unknown, path: ReadonlyArray<string | number>) {
  return Schema.decodeUnknown(
    Schema.Struct({
      issues: Schema.Array(
        Schema.Struct({
          path: Schema.Array(Schema.Union(Schema.String, Schema.Number)),
          message: Schema.NonEmptyString,
        }),
      ),
    }),
  )(data).pipe(
    Effect.tap((diagnostics) =>
      Effect.sync(() => {
        expect(diagnostics.issues.map((issue) => issue.path)).toContainEqual(
          path,
        );
      }),
    ),
  );
}

function registrationOperation(
  accepted: unknown[],
): HarnessMcpOperations["register"] {
  return (input) =>
    Effect.sync(() => {
      accepted.push(input);
      return { kind: "name_taken" as const };
    });
}

function checksInvocationPrivacy() {
  return Effect.gen(function* () {
    const handler = yield* acquireHandler(operations);
    const sentinel = "private-mcp-sentinel-1304";
    const malformedText = yield* request(handler, "tools/call", {
      name: "send_message",
      arguments: { input: { to: "agent:bob", text: `${sentinel}\ud800` } },
    });
    expect(malformedText.error).toMatchObject({
      code: ProtocolErrorCode.InvalidParams,
    });
    yield* checkDiagnostic(malformedText.error?.data, ["input", "text"]);
    expect(JSON.stringify(malformedText.error)).not.toContain(sentinel);
  });
}

function checksStructuralPrivacy() {
  return Effect.gen(function* () {
    const handler = yield* acquireHandler(operations);
    const sentinel = "private-token-sentinel-1304";
    const response = yield* request(handler, "tools/call", {
      name: "acknowledge_delivery",
      arguments: { deliveryToken: { secret: sentinel } },
    });
    expect(response.error).toMatchObject({
      code: ProtocolErrorCode.InvalidParams,
    });
    yield* checkDiagnostic(response.error?.data, ["deliveryToken"]);
    expect(JSON.stringify(response.error)).not.toContain(sentinel);
  });
}

function checksDiagnosticCap() {
  return Effect.gen(function* () {
    const handler = yield* acquireHandler(operations, false);
    const sentinel = "private-excess-sentinel-1304";
    const response = yield* request(handler, "tools/call", {
      name: "status",
      arguments: Object.fromEntries(
        Array.from({ length: 25 }, () => sentinel).map((value, index) => [
          `extra${index}`,
          value,
        ]),
      ),
    });
    expect(response.error).toMatchObject({
      code: ProtocolErrorCode.InvalidParams,
    });
    const diagnostics = yield* checkDiagnostic(response.error?.data, [
      "extra0",
    ]);
    expect(diagnostics.issues).toHaveLength(20);
    expect(JSON.stringify(response.error)).not.toContain(sentinel);
  });
}

// @agent-code-guard/regression-only: malformed tool inputs name their fields without entering an operation.
describe("MCP tool argument diagnostics", () => {
  it.each(invalidInvocationCalls)(
    "retains invocation diagnostics for $name",
    (call) =>
      Effect.gen(function* () {
        const handler = yield* acquireHandler(operations);
        const response = yield* request(handler, "tools/call", {
          name: call.name,
          arguments: call.arguments,
        });
        expect(response.error).toMatchObject({
          code: ProtocolErrorCode.InvalidParams,
        });
        yield* checkDiagnostics(call.paths, response.error);
        const failure = yield* decodeHarnessSendErrorData(response.error?.data);
        expect(failure).toMatchObject({ reason: "content-invalid" });
      }),
  );

  it(
    "keeps message text and runtime tokens out of invocation errors",
    checksInvocationPrivacy,
  );

  it("keeps token values out of structural errors", checksStructuralPrivacy);

  it(
    "caps structural diagnostics at twenty without exposing values",
    checksDiagnosticCap,
  );
});

// @agent-code-guard/regression-only: structural failures retain paths without entering operations.
describe("MCP structural argument diagnostics", () => {
  it.each(invalidCalls)("names the invalid field for $name", (call) =>
    Effect.gen(function* () {
      const handler = yield* acquireHandler({
        ...operations,
        readPolicy: unused,
        updatePolicy: unused,
        readDecisions: unused,
      });
      const response = yield* request(handler, "tools/call", {
        name: call.name,
        arguments: call.arguments,
      });
      expect(response.error).toMatchObject({
        code: ProtocolErrorCode.InvalidParams,
      });
      yield* checkDiagnostics(call.paths, response.error);
    }),
  );
});

// @agent-code-guard/regression-only: malformed identities must be rejected before registration side effects.
describe("MCP registration validation", () => {
  it.each(invalidRegistrations)(
    "rejects registration $field: $value before execution",
    (call) =>
      Effect.gen(function* () {
        const accepted: unknown[] = [];
        const handler = yield* acquireHandler(
          {
            ...operations,
            register: registrationOperation(accepted),
          },
          false,
        );
        const valid = {
          operationId: `opn_${"A".repeat(22)}`,
          principalId: `prn_${"A".repeat(22)}`,
          agentName: "alice",
        };
        const response = yield* request(handler, "tools/call", {
          name: "register",
          arguments: { ...valid, [call.field]: call.value },
        });
        expect(response.error).toMatchObject({
          code: ProtocolErrorCode.InvalidParams,
        });
        yield* checkDiagnostic(response.error?.data, [call.field]);
        const rule = Option.getOrThrow(
          SchemaAST.getDescriptionAnnotation(call.schema.ast),
        );
        expect(response.error?.message).toContain(
          `${call.field}: Expected ${rule}`,
        );
        expect(accepted).toEqual([]);
        const success = yield* request(handler, "tools/call", {
          name: "register",
          arguments: valid,
        });
        expect(success.result).toMatchObject({
          structuredContent: { kind: "name_taken" },
        });
        expect(accepted).toEqual([valid]);
      }),
  );
});

// @agent-code-guard/regression-only: structural diagnostics must preserve semantic operation failure reasons.
describe("MCP semantic failures", () => {
  it.each(semanticFailures)("names the failure of $name", (call) =>
    Effect.gen(function* () {
      const handler = yield* acquireHandler({
        ...operations,
        readConversation: () => Effect.fail({ reason: "invalid-continuation" }),
        searchConversations: () => Effect.fail({ reason: "invalid-address" }),
        readPolicy: unused,
        updatePolicy: () => Effect.fail(refusedPolicyEdit),
        readDecisions: unused,
      });
      const response = yield* request(handler, "tools/call", {
        name: call.name,
        arguments: call.arguments,
      });
      expect(response.error).toMatchObject({
        code: ProtocolErrorCode.InternalError,
        data: call.data,
      });
    }),
  );
});

const fieldDescriptionsSchema = Schema.Record({
  key: Schema.String,
  value: Schema.Struct({ description: Schema.String }),
});

// @agent-code-guard/regression-only: descriptions expose identifier, retry and pagination intent to tool consumers.
it("describes every MCP tool and the key input fields", () =>
  Effect.gen(function* () {
    const inactive = yield* acquireHandler(operations, false);
    const active = yield* acquireHandler(operations);
    const inactiveResponse = yield* request(inactive, "tools/list", {});
    const activeResponse = yield* request(active, "tools/list", {});
    const before = yield* Schema.decodeUnknown(catalogSchema)(
      inactiveResponse.result,
    );
    const after = yield* Schema.decodeUnknown(catalogSchema)(
      activeResponse.result,
    );
    for (const tool of [...before.tools, ...after.tools]) {
      expect(tool.description.trim().length).toBeGreaterThan(0);
    }
    const registration = yield* Schema.decodeUnknown(fieldDescriptionsSchema)(
      before.tools.find((tool) => tool.name === "register")?.inputSchema
        .properties,
    );
    expect(registration.operationId?.description.trim().length).toBeGreaterThan(
      0,
    );
    expect(registration.principalId?.description.trim().length).toBeGreaterThan(
      0,
    );
    expect(registration.agentName?.description.trim().length).toBeGreaterThan(
      0,
    );
  }));
