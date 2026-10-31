/** @file Owner policy changes through a running public daemon and its MCP boundary. */

import { FileSystem } from "@effect/platform";
import { NodeContext } from "@effect/platform-node";
import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import {
  Daemon,
  type DecisionProvider,
  type DecisionRequest,
  type MessageMiddleware,
} from "@moltzap/social-harness/service";
import {
  ConfigProvider,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Redacted,
  Schedule,
  Schema,
} from "effect";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  acquireDaemonManagementClient,
  acquireDaemonProcess,
  acquireProcessInfrastructure,
  type DaemonProcessFixture,
  makeDaemonProcessFixture,
  makeRegistrationRequest,
} from "../daemon/daemon-process-harness.js";
import { processTrace } from "../process-trace.js";
import {
  anyString,
  arrayContaining,
  objectContaining,
} from "../test-matchers.js";

const pair = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const alice = yield* makeDaemonProcessFixture(infrastructure, "alice");
  const bob = yield* makeDaemonProcessFixture(infrastructure, "bob");
  yield* acquireDaemonProcess(bob);
  const management = yield* acquireDaemonManagementClient(bob);
  yield* management.register(makeRegistrationRequest(bob));
  return { alice, bob };
});

const settings = {
  timeoutMilliseconds: 10000,
  middlewareTimeoutMilliseconds: 8000,
  maximumInputBytes: 1000000,
  maximumResultBytes: 1000000,
};

const provider: DecisionProvider = {
  decide: (request) => Effect.succeed(answer(request)),
};

const options: Daemon.FirewallOptions = {
  mode: "enforcement",
  incoming: [],
  outgoing: [],
  settings,
  facts: [],
  codeDigest: "a".repeat(64),
  text: {
    provider,
    settings: {
      minimumProbability: 0.5,
      maximumRequestBytes: 1000000,
      timeoutMilliseconds: 5000,
    },
  },
};

const readSchema = Schema.Struct({
  rules: Schema.Array(Schema.Struct({ id: Schema.String })),
  fixedRules: Schema.Array(Schema.Unknown),
  activeRevision: Schema.String,
});

const read = (client: Client) =>
  call(client, "policy_read").pipe(
    Effect.flatMap(Schema.decodeUnknown(readSchema)),
  );

const updateSchema = Schema.Struct({
  activeRevision: Schema.String,
});

const update = (client: Client, args: Readonly<Record<string, unknown>>) =>
  call(client, "policy_update", args).pipe(
    Effect.flatMap(Schema.decodeUnknown(updateSchema)),
  );

const secretText = "private candidate";

const send = (client: Client) =>
  call(client, "send_message", {
    input: { to: "agent:bob", text: secretText },
  });

const blockedRule = {
  id: "private",
  applies: "outgoing",
  text: "Never disclose private text.",
} as const;

interface FixedPolicyScenario {
  readonly client: Client;
  readonly codeRule: {
    readonly id: string;
    readonly guidance: string;
  };
  readonly middleware: MessageMiddleware;
}

const acquirePolicyCheckScenario = Effect.gen(function* () {
  const { alice } = yield* pair;
  const entered = yield* Deferred.make<undefined>();
  const release = yield* Deferred.make<undefined>();
  const middleware: MessageMiddleware = {
    id: "code",
    revision: "1",
    rules: [{ id: "fixed-code" }],
    check: () =>
      Deferred.succeed(entered, undefined).pipe(
        Effect.zipRight(Deferred.await(release)),
        Effect.as({
          findings: [{ ruleId: "fixed-code", outcome: "pass" }],
          expectations: [],
        }),
      ),
  };
  yield* start(alice, {
    ...options,
    outgoing: [middleware],
  });
  const client = yield* connect(alice);
  yield* register(client, alice);
  return { entered, release, client };
});

const fixedRule = {
  id: "owner",
  applies: "both",
  text: "Follow the owner.",
  guidance: "Ask the owner.",
} as const;

const acquireFixedPolicyScenario = Effect.gen(function* () {
  const { alice } = yield* pair;
  const codeRule = {
    id: "no-links",
    guidance: "Send the page title instead of the link.",
  } as const;
  const middleware: MessageMiddleware = {
    id: "links",
    revision: "1",
    rules: [codeRule],
    check: () =>
      Effect.succeed({
        findings: [{ ruleId: codeRule.id, outcome: "pass" }],
        expectations: [],
      }),
  };
  yield* start(alice, {
    ...options,
    incoming: [middleware],
    outgoing: [middleware],
    text: {
      provider,
      settings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 1000000,
        timeoutMilliseconds: 5000,
      },
      fixedRules: [fixedRule],
    },
  });
  const client = yield* connect(alice);
  yield* register(client, alice);
  return { codeRule, middleware, client };
});

const swapDuringCheck = Effect.gen(function* () {
  const scenario = yield* acquirePolicyCheckScenario;
  const { client, entered } = scenario;
  const before = yield* read(client);
  expect(
    yield* update(client, { remove: ["fixed-code"] }).pipe(Effect.isFailure),
  ).toBe(true);
  const inFlight = yield* send(client).pipe(Effect.forkScoped);
  yield* Deferred.await(entered);
  const changed = yield* update(client, { add: [blockedRule] });
  expect(changed.activeRevision).not.toBe(before.activeRevision);
  yield* Deferred.succeed(scenario.release, undefined);
  yield* Fiber.join(inFlight);
  expect(yield* send(client).pipe(Effect.isFailure)).toBe(true);
});

const persistAndValidate = Effect.gen(function* () {
  const { alice } = yield* pair;
  const fixedOptions = {
    ...options,
    text: {
      ...options.text,
      provider,
      settings: {
        minimumProbability: 0.5,
        maximumRequestBytes: 1000000,
        timeoutMilliseconds: 5000,
      },
      fixedRules: [fixedRule],
    },
  };
  const runtime = yield* start(alice, fixedOptions);
  const client = yield* connect(alice);
  yield* register(client, alice);
  const changed = yield* update(client, { add: [blockedRule] });
  expect(
    yield* update(client, { remove: [fixedRule.id] }).pipe(Effect.isFailure),
  ).toBe(true);
  expect(
    yield* update(client, { add: [{ ...blockedRule, id: fixedRule.id }] }).pipe(
      Effect.isFailure,
    ),
  ).toBe(true);
  expect(
    yield* update(client, {
      add: [{ id: "invalid", applies: "sideways", text: "Follow the owner." }],
    }).pipe(Effect.isFailure),
  ).toBe(true);
  expect(yield* read(client)).toMatchObject({
    activeRevision: changed.activeRevision,
    fixedRules: arrayContaining([{ from: "text", ...fixedRule }]),
    rules: [{ id: blockedRule.id }],
  });
  expect(yield* send(client).pipe(Effect.isFailure)).toBe(true);
  yield* Fiber.interrupt(runtime);
  const restarted = yield* start(alice, fixedOptions);
  const fresh = yield* connect(alice);
  expect(yield* read(fresh)).toMatchObject({
    activeRevision: changed.activeRevision,
    rules: [{ id: blockedRule.id }],
  });
  expect(yield* send(fresh).pipe(Effect.isFailure)).toBe(true);
  yield* update(fresh, { remove: [blockedRule.id] });
  yield* send(fresh);
  yield* Fiber.interrupt(restarted);
});

const readFixed = acquireFixedPolicyScenario.pipe(
  Effect.flatMap(expectFixedPolicyCannotBeEdited),
);

const concurrentEdits = Effect.gen(function* () {
  const { alice } = yield* pair;
  const runtime = yield* start(alice, options);
  const client = yield* connect(alice);
  yield* register(client, alice);
  const additions = [
    fixedRule,
    { ...fixedRule, id: "second" },
    { ...fixedRule, id: "third" },
  ];
  yield* Effect.forEach(additions, (rule) => update(client, { add: [rule] }), {
    concurrency: 4,
  });
  const before = yield* read(client);
  expect(before.rules.map((rule) => rule.id)).toEqual(
    arrayContaining(additions.map((rule) => rule.id)),
  );
  yield* send(client);
  yield* Fiber.interrupt(runtime);
  yield* start(alice, options);
  const fresh = yield* connect(alice);
  expect(yield* read(fresh)).toEqual(before);
});

const authenticatedPolicyEdits = Effect.gen(function* () {
  const { alice } = yield* pair;
  const fs = yield* FileSystem.FileSystem;
  const token = "agent-policy-test-token-".repeat(3);
  const credentialPath = join(alice.stateDirectory, "agent-credential");
  yield* fs.writeFileString(credentialPath, token);
  yield* start(alice, options, {
    SHARNESS_MCP_CREDENTIAL_FILE: credentialPath,
  });
  const client = yield* connect(alice, token);
  yield* register(client, alice);
  const catalog = yield* Effect.tryPromise(() => client.listTools());
  expect(catalog.tools).toEqual(
    arrayContaining([
      objectContaining({ name: "policy_read" }),
      objectContaining({ name: "policy_update" }),
    ]),
  );
  yield* update(client, { add: [blockedRule] });
  const adopted = {
    id: "shared-review",
    applies: "incoming",
    text: "Reject peer scheduling requests without prior confirmation.",
    feedback: { text: "Confirm first.", nextAction: "wait" },
  };
  yield* update(client, { add: [adopted] });
  expect((yield* read(client)).rules).toEqual([
    { id: blockedRule.id },
    { id: adopted.id },
  ]);
  yield* update(client, { remove: [blockedRule.id, adopted.id] });
  expect((yield* read(client)).rules).toEqual([]);
});

describe("editable firewall through the daemon public API", () => {
  it(
    "applies edits to later sends while an in-flight check keeps its policy",
    processTrace(
      Effect.scoped(swapDuringCheck).pipe(Effect.provide(NodeContext.layer)),
    ),
    60000,
  );
  it(
    "rejects invalid and fixed edits and restores active policy after restart",
    processTrace(persistAndValidate.pipe(Effect.provide(NodeContext.layer))),
    60000,
  );
  it(
    "returns fixed natural-language text and code rule descriptions and refuses to edit them",
    processTrace(readFixed.pipe(Effect.provide(NodeContext.layer))),
    60000,
  );
  it(
    "serializes concurrent additions without losing rules across restart",
    processTrace(concurrentEdits.pipe(Effect.provide(NodeContext.layer))),
    60000,
  );
  it(
    "lets the authenticated agent read and change editable policy",
    processTrace(
      authenticatedPolicyEdits.pipe(Effect.provide(NodeContext.layer)),
    ),
    60000,
  );
});

function expectFixedPolicyCannotBeEdited(state: FixedPolicyScenario) {
  return Effect.gen(function* () {
    const { codeRule, middleware, client } = state;
    const policy = yield* call(client, "policy_read");
    const removeCode = yield* call(client, "policy_update", {
      remove: [codeRule.id],
    }).pipe(Effect.flip);
    const replaceText = yield* call(client, "policy_update", {
      add: [{ ...blockedRule, id: fixedRule.id }],
    }).pipe(Effect.flip);
    expect(policy).toEqual({
      rules: [],
      fixedRules: arrayContaining([
        objectContaining({
          from: "code",
          middleware: middleware.id,
          ...codeRule,
        }),
        objectContaining({ from: "text", ...fixedRule }),
      ]),
      activeRevision: anyString,
    });
    expect(removeCode.cause).toMatchObject({ data: { reason: "fixed-rule" } });
    expect(replaceText.cause).toMatchObject({
      data: { reason: "fixed-rule" },
    });
    expect(yield* call(client, "policy_read")).toEqual(policy);
  });
}

function answer(request: DecisionRequest) {
  return {
    provider: "fixture",
    model: "deterministic",
    usage: {},
    raw: secretText,
    answers: Object.fromEntries(
      Object.entries(request.questions).map(([id, question]) => {
        const selected =
          request.stage === "compliance" && question.ruleId === blockedRule.id
            ? "violation"
            : (["compliant", "autonomous", "none", "relevant"].find((choice) =>
                Object.hasOwn(question.criteria, choice),
              ) ?? "unresolved");
        return [
          id,
          {
            choice: selected,
            probabilities: Object.fromEntries(
              Object.keys(question.criteria).map((choice) => [
                choice,
                choice === selected ? 1 : 0,
              ]),
            ),
          },
        ];
      }),
    ),
  };
}

function start(
  fixture: DaemonProcessFixture,
  policy: Daemon.FirewallOptions,
  extra: Readonly<Record<string, string>> = {},
) {
  return Layer.launch(Daemon.withFirewall(policy)).pipe(
    Effect.withConfigProvider(
      ConfigProvider.fromMap(
        new Map(Object.entries({ ...fixture.environment, ...extra })),
      ),
    ),
    Effect.forkScoped,
  );
}

function connect(fixture: DaemonProcessFixture, bearer?: string) {
  return Effect.gen(function* () {
    const credential = bearer ?? Redacted.value(fixture.mcpCredential);
    const client = new Client(
      { name: "policy-test", version: "1" },
      { capabilities: {}, versionNegotiation: { mode: { pin: "2026-07-28" } } },
    );
    yield* Effect.acquireRelease(
      Effect.tryPromise(() =>
        client.connect(
          new StreamableHTTPClientTransport(fixture.endpoint, {
            requestInit: { headers: { authorization: `Bearer ${credential}` } },
          }),
        ),
      ).pipe(
        Effect.retry(
          Schedule.intersect(
            Schedule.spaced("25 millis"),
            Schedule.recurs(200),
          ),
        ),
      ),
      () => Effect.tryPromise(() => client.close()).pipe(Effect.ignore),
    );
    return client;
  });
}

function register(client: Client, fixture: DaemonProcessFixture) {
  return call(client, "register", { ...makeRegistrationRequest(fixture) });
}

function call(
  client: Client,
  name: string,
  args: Readonly<Record<string, unknown>> = {},
) {
  return Effect.tryPromise(() =>
    client.callTool({ name, arguments: args }),
  ).pipe(Effect.map((result) => result.structuredContent));
}
