/** @file End-to-end Registry and Router process interoperability checks. */
import { FileSystem, HttpClient, HttpClientRequest } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import {
  AgentIdentityCard,
  AgentName,
  AgentSigningAuthority,
  type AgentSigningAuthority as AgentSigningAuthorityValue,
  EnvelopeId,
  LIVE_WIRE_VERSIONS,
  type LiveSignedMessage,
  PrincipalId,
  SignedMessage,
  type SignedMessage as SignedMessageValue,
  type VerifiedAgentIdentityCard,
} from "@moltzap/social-harness";
import {
  Registry,
  registryContract,
  Router,
  type routerContract,
} from "@moltzap/social-harness/internal";
import { signAgentRequest, WireVersions } from "@moltzap/social-harness/test";
import { Duration, Effect, Encoding, Layer, Redacted, Schema } from "effect";
import { randomBytes } from "node:crypto";
import { join } from "node:path";
import { expect, it } from "vitest";
import {
  acquireDaemonManagementClient,
  acquireDaemonProcesses,
  acquireProcessInfrastructure,
  awaitReady,
  CONCURRENT_DAEMONS,
  hasExited,
  LOOPBACK_HOST,
  makePrivateKeyPem,
  makeRegistrationRequest,
  makeSigningAuthority,
  managedProcess,
  processTestError,
  reservePort,
  ROUTER_BINARY,
  type RunningProcess,
  serviceReady,
  startPgliteRegistry,
  stopProcess,
} from "../daemon/daemon-fixture.js";
import { acquireLoopbackServer } from "../loopback-server.js";
import {
  makeRegistryRunner,
  type RegistryRunner,
} from "../registry/registry-fixture.js";
import { processTrace } from "../scenario-fixture.js";
const ADMISSION_CREDENTIAL = "registry-router-e2e-admission";
const REGISTERED_KIND = "registered";
const FOUND_KIND = "found";
const BATCH_KIND = "batch";
const ACCEPTED_KIND = "accepted";
const ROUTER_RESTARTED_KIND = "router_restarted";
const CURSOR_INVALID_RESULT = Object.freeze({ kind: "cursor_invalid" });
const OK_STATUS = 200;
const NO_CONTENT_STATUS = 204;
const AUTHENTICATION_FAILED_STATUS = 401;
const OVERLOADED_STATUS = 429;
const UNAVAILABLE_STATUS = 503;
const JSON_CONTENT_TYPE = "application/json";
const MESSAGE_INVALID_BODY = '{"kind":"message_invalid"}';
const OVERLOADED_BODY = '{"error":"overloaded"}';
const AUTHENTICATION_FAILED_BODY = '{"error":"authentication_failed"}';
const UNAVAILABLE_BODY = '{"error":"unavailable"}';
const BATCH_BODY_FRAGMENT = '"kind":"batch"';
const outsideWireWindow = {
  emitted: "1900.101.0",
  live: ["1900.101.0"],
  history: ["1900.101.0"],
};
interface RegisteredAgent {
  readonly signingAuthority: AgentSigningAuthorityValue;
  readonly identityCard: VerifiedAgentIdentityCard;
}

interface RegisteredAgents {
  readonly sender: RegisteredAgent;
  readonly firstRecipient: RegisteredAgent;
  readonly secondRecipient: RegisteredAgent;
  readonly nonRecipient: RegisteredAgent;
}

interface Infrastructure {
  readonly registryOrigin: URL;
  readonly registrySignerPublicKey: ReturnType<
    typeof AgentSigningAuthority.publicKey
  >;
  readonly registryProcess: RunningProcess;
  readonly routerOrigin: URL;
  readonly routerPort: number;
}

type RegistryLookupRequest = typeof registryContract.RegistryLookupRequest.Type;
type RegistryRegisterRequest =
  typeof registryContract.RegistryRegisterRequest.Type;
type RouterPollRequest = typeof routerContract.RouterPollRequest.Type;
type RouterPollResult = typeof routerContract.RouterPollResult.Type;
type RouterSendRequest = typeof routerContract.RouterSendRequest.Type;
type RouterSendResult = typeof routerContract.RouterSendResult.Type;

type BatchResult = Extract<RouterPollResult, { readonly kind: "batch" }>;
type AcceptedResult = Extract<RouterSendResult, { readonly kind: "accepted" }>;
type RouterRunner = <A, E>(
  effect: Effect.Effect<A, E, Router>,
) => Effect.Effect<A, E>;

interface PollAnchors {
  readonly firstRecipient: BatchResult;
  readonly secondRecipient: BatchResult;
  readonly nonRecipient: BatchResult;
}

interface SentMessage {
  readonly initialRequest: RouterSendRequest;
  readonly accepted: AcceptedResult;
  readonly firstRecipientDelivery: BatchResult;
}

interface HttpObservation {
  readonly status: number;
  readonly contentType: string;
  readonly body: string;
}

const randomIdentifierPayload = (): string =>
  randomBytes(16).toString("base64url");

const makeOperationId = () =>
  Schema.decodeUnknownSync(registryContract.RegistrationId)(
    `opn_${randomIdentifierPayload()}`,
  );

const makePrincipalId = () =>
  Schema.decodeUnknownSync(PrincipalId)(`prn_${randomIdentifierPayload()}`);

const makeEnvelopeId = () =>
  Schema.decodeUnknownSync(EnvelopeId)(`env_${randomIdentifierPayload()}`);

/** Process requests use the production HTTP transport with bounded poll and send waits. */
const makeRouterRunner = (origin: URL): RouterRunner => {
  const routerLayer = Router.layer({
    origin,
    sendTimeout: Duration.seconds(5),
    pollTimeout: Duration.seconds(35),
  }).pipe(Layer.provide(NodeHttpClient.layer));
  return (effect) => effect.pipe(Effect.provide(routerLayer));
};

const signAuthenticatedRequest = (
  origin: URL,
  path: string,
  agent: RegisteredAgent,
  encodedRequest: unknown,
) =>
  signAgentRequest({
    httpRequest: HttpClientRequest.post(new URL(path, origin)),
    callerAgentId: agent.identityCard.agentId,
    encodedRequest,
    signingAuthority: agent.signingAuthority,
  });

const observeHttpRequest = (request: HttpClientRequest.HttpClientRequest) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.execute(request);
    const body = yield* response.text;
    return {
      status: response.status,
      contentType: response.headers["content-type"] ?? "",
      body,
    } satisfies HttpObservation;
  });

const expectHttpObservation = (
  observation: HttpObservation,
  status: number,
  body: string,
): void => {
  expect(observation.status).toBe(status);
  expect(observation.contentType).toBe(JSON_CONTENT_TYPE);
  expect(observation.body).toBe(body);
};

const assertHealthy = (
  runningProcess: RunningProcess,
  service: "sharness-registry" | "sharness-router",
  origin: URL,
) =>
  Effect.gen(function* () {
    yield* awaitReady(
      runningProcess,
      serviceReady(runningProcess, service, origin),
      "health response",
    );
    expect(hasExited(runningProcess.child)).toBe(false);
    const health = yield* observeHttpRequest(
      HttpClientRequest.get(new URL("/healthz", origin)),
    );
    expect(health.status).toBe(NO_CONTENT_STATUS);
    expect(health.body).toBe("");
  });

const registerAgent = (name: string, runRegistry: RegistryRunner) =>
  Effect.gen(function* () {
    const signingAuthority = yield* makeSigningAuthority();
    const request = {
      operationId: makeOperationId(),
      principalId: makePrincipalId(),
      agentName: Schema.decodeUnknownSync(AgentName)(name),
      publicKey: AgentSigningAuthority.publicKey(signingAuthority),
    } satisfies RegistryRegisterRequest;
    const result = yield* runRegistry(
      Registry.register({
        request,
        admissionCredential: Redacted.make(ADMISSION_CREDENTIAL),
        signingAuthority,
      }),
    );
    expect(result.kind).toBe(REGISTERED_KIND);
    if (result.kind !== REGISTERED_KIND) {
      return yield* Effect.fail(
        processTestError(`registration returned ${result.kind}`),
      );
    }
    return {
      signingAuthority,
      identityCard: result.identityCard,
    } satisfies RegisteredAgent;
  });

const requireBatch = (result: RouterPollResult): BatchResult => {
  expect(result.kind).toBe(BATCH_KIND);
  if (result.kind !== BATCH_KIND) {
    throw processTestError(`poll returned ${result.kind}`);
  }
  return result;
};

const requireAccepted = (result: RouterSendResult): AcceptedResult => {
  expect(result.kind).toBe(ACCEPTED_KIND);
  if (result.kind !== ACCEPTED_KIND) {
    throw processTestError(`send returned ${result.kind}`);
  }
  return result;
};

const encodeAgentCard = (identityCard: VerifiedAgentIdentityCard) =>
  Schema.encode(AgentIdentityCard)(identityCard);

const encodeSignedMessage = (signedMessage: SignedMessageValue) =>
  Schema.encode(SignedMessage)(signedMessage);

const makeRegistryIdentity = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const temporaryDirectory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "sharness-registry-router-",
  });
  const registryPrivateKeyPath = join(temporaryDirectory, "registry-key.pem");
  const registryPrivateKeyPem = makePrivateKeyPem();
  yield* fileSystem.writeFileString(
    registryPrivateKeyPath,
    registryPrivateKeyPem,
    { mode: 0o600 },
  );
  const registrySigningAuthority = yield* makeSigningAuthority(
    registryPrivateKeyPem,
  );
  const registrySignerPublicKey = AgentSigningAuthority.publicKey(
    registrySigningAuthority,
  );
  return { registryPrivateKeyPath, registrySignerPublicKey };
});

const startRegistryInfrastructure = Effect.gen(function* () {
  const { registryPrivateKeyPath, registrySignerPublicKey } =
    yield* makeRegistryIdentity;
  const [postgresqlPort, registryPort] = yield* Effect.all(
    [reservePort, reservePort] as const,
    { concurrency: 2 },
  );
  const registryOrigin = new URL(`http://${LOOPBACK_HOST}:${registryPort}`);
  const registry = yield* startPgliteRegistry({
    postgresqlPort,
    registryPort,
    registryKeyPath: registryPrivateKeyPath,
    admissionCredential: ADMISSION_CREDENTIAL,
  });
  yield* assertHealthy(registry, "sharness-registry", registryOrigin);
  return {
    registryOrigin,
    registryProcess: registry,
    registrySignerPublicKey,
  };
});

const startInfrastructure = Effect.gen(function* () {
  const registry = yield* startRegistryInfrastructure;
  const routerPort = yield* reservePort;
  return {
    ...registry,
    routerOrigin: new URL(`http://${LOOPBACK_HOST}:${routerPort}`),
    routerPort,
  } satisfies Infrastructure;
});

const startRouterAt = (
  infrastructure: Infrastructure,
  port: number,
  environment: Readonly<Record<string, string>> = {},
) =>
  Effect.gen(function* () {
    const router = yield* managedProcess(ROUTER_BINARY, [], {
      SHARNESS_ROUTER_HOST: LOOPBACK_HOST,
      SHARNESS_ROUTER_PORT: String(port),
      SHARNESS_ROUTER_REGISTRY_ORIGIN: infrastructure.registryOrigin.origin,
      SHARNESS_ROUTER_REGISTRY_SIGNER_PUBLIC_KEY: JSON.stringify({
        crv: infrastructure.registrySignerPublicKey.crv,
        kty: infrastructure.registrySignerPublicKey.kty,
        x: infrastructure.registrySignerPublicKey.x,
      }),
      ...environment,
    });
    yield* assertHealthy(
      router,
      "sharness-router",
      new URL(`http://${LOOPBACK_HOST}:${port}`),
    );
    return router;
  });

const startRouter = (infrastructure: Infrastructure) =>
  startRouterAt(infrastructure, infrastructure.routerPort);

const registerAgents = (runRegistry: RegistryRunner) =>
  Effect.gen(function* () {
    const [sender, firstRecipient, secondRecipient, nonRecipient] =
      yield* Effect.all(
        [
          registerAgent("sender-agent", runRegistry),
          registerAgent("recipient-one", runRegistry),
          registerAgent("recipient-two", runRegistry),
          registerAgent("non-recipient", runRegistry),
        ] as const,
        { concurrency: 1 },
      );
    return {
      sender,
      firstRecipient,
      secondRecipient,
      nonRecipient,
    } satisfies RegisteredAgents;
  });

const assertAgentLookups = (
  agents: RegisteredAgents,
  runRegistry: RegistryRunner,
) =>
  Effect.forEach(
    [
      agents.sender,
      agents.firstRecipient,
      agents.secondRecipient,
      agents.nonRecipient,
    ],
    (registeredAgent) =>
      Effect.gen(function* () {
        const request = {
          agentId: registeredAgent.identityCard.agentId,
        } satisfies RegistryLookupRequest;
        const lookup = yield* runRegistry(Registry.lookup(request));
        expect(lookup.kind).toBe(FOUND_KIND);
        if (lookup.kind !== FOUND_KIND) {
          return yield* Effect.fail(
            processTestError("registered identity was not found"),
          );
        }
        const [actual, expected] = yield* Effect.all(
          [
            encodeAgentCard(lookup.identityCard),
            encodeAgentCard(registeredAgent.identityCard),
          ] as const,
          { concurrency: 2 },
        );
        expect(actual).toStrictEqual(expected);
      }),
    { concurrency: 1, discard: true },
  );

const anchorFeeds = (agents: RegisteredAgents, runRouter: RouterRunner) =>
  Effect.gen(function* () {
    const anchor = (agent: RegisteredAgent) =>
      runRouter(
        Router.poll({
          request: {} satisfies RouterPollRequest,
          callerAgentId: agent.identityCard.agentId,
          signingAuthority: agent.signingAuthority,
        }),
      ).pipe(Effect.map(requireBatch));
    const [firstRecipient, secondRecipient, nonRecipient] = yield* Effect.all(
      [
        anchor(agents.firstRecipient),
        anchor(agents.secondRecipient),
        anchor(agents.nonRecipient),
      ] as const,
      { concurrency: 3 },
    );
    expect(firstRecipient.signedMessages).toHaveLength(0);
    expect(secondRecipient.routerInstanceId).toBe(
      firstRecipient.routerInstanceId,
    );
    expect(nonRecipient.routerInstanceId).toBe(firstRecipient.routerInstanceId);
    return {
      firstRecipient,
      secondRecipient,
      nonRecipient,
    } satisfies PollAnchors;
  });

const invalidSignedMessageShapes: readonly unknown[] = Object.freeze([
  "not-a-signed-message",
  null,
  Object.freeze([]),
]);

const assertInvalidSignedMessageShapes = (
  infrastructure: Infrastructure,
  sender: RegisteredAgent,
  routerInstanceId: BatchResult["routerInstanceId"],
) =>
  Effect.forEach(
    invalidSignedMessageShapes,
    (signedMessage) =>
      Effect.gen(function* () {
        const request = yield* signAuthenticatedRequest(
          infrastructure.routerOrigin,
          "/v1/messages:send",
          sender,
          {
            expectedRouterInstanceId: routerInstanceId,
            mode: "initial",
            signedMessage,
          },
        );
        const observation = yield* observeHttpRequest(request);
        expectHttpObservation(observation, OK_STATUS, MESSAGE_INVALID_BODY);
      }),
    { concurrency: 1, discard: true },
  );

const signMessage = (
  agents: RegisteredAgents,
): Effect.Effect<LiveSignedMessage, unknown> =>
  SignedMessage.sign({
    identityCard: agents.sender.identityCard,
    signingAuthority: agents.sender.signingAuthority,
    recipientAgentIds: new Set([
      agents.firstRecipient.identityCard.agentId,
      agents.secondRecipient.identityCard.agentId,
    ]),
    envelopeId: makeEnvelopeId(),
    body: new TextEncoder().encode("opaque-registry-router-e2e"),
  });

const assertNonceCapacityWithoutEviction = (
  infrastructure: Infrastructure,
  sender: RegisteredAgent,
) =>
  Effect.gen(function* () {
    const port = yield* reservePort;
    const origin = new URL(`http://${LOOPBACK_HOST}:${port}`);
    const router = yield* startRouterAt(infrastructure, port, {
      SHARNESS_ROUTER_LIVE_NONCE_CAPACITY: "1",
    });
    const firstRequest = yield* signAuthenticatedRequest(
      origin,
      "/v1/messages:poll",
      sender,
      {},
    );
    const first = yield* observeHttpRequest(firstRequest);
    expect(first.status).toBe(OK_STATUS);
    expect(first.contentType).toBe(JSON_CONTENT_TYPE);
    expect(first.body).toContain(BATCH_BODY_FRAGMENT);

    const novelRequest = yield* signAuthenticatedRequest(
      origin,
      "/v1/messages:poll",
      sender,
      {},
    );
    expectHttpObservation(
      yield* observeHttpRequest(novelRequest),
      OVERLOADED_STATUS,
      OVERLOADED_BODY,
    );
    expectHttpObservation(
      yield* observeHttpRequest(firstRequest),
      AUTHENTICATION_FAILED_STATUS,
      AUTHENTICATION_FAILED_BODY,
    );
    yield* stopProcess(router);
  });

const sendInitial = (
  signedMessage: LiveSignedMessage,
  sender: RegisteredAgent,
  expectedRouterInstanceId: BatchResult["routerInstanceId"],
  runRouter: RouterRunner,
) =>
  Effect.gen(function* () {
    const initialRequest = {
      expectedRouterInstanceId,
      mode: "initial",
      signedMessage,
    } satisfies RouterSendRequest;
    const accepted = requireAccepted(
      yield* runRouter(
        Router.send({
          request: initialRequest,
          callerAgentId: sender.identityCard.agentId,
          signingAuthority: sender.signingAuthority,
        }),
      ),
    );
    expect(accepted.routerInstanceId).toBe(expectedRouterInstanceId);
    return { initialRequest, accepted };
  });

const pollDeliveries = (
  agents: RegisteredAgents,
  anchors: PollAnchors,
  runRouter: RouterRunner,
) => {
  const continuePoll = (
    agent: RegisteredAgent,
    pollCursor: NonNullable<RouterPollRequest["pollCursor"]>,
  ) =>
    runRouter(
      Router.poll({
        request: { pollCursor } satisfies RouterPollRequest,
        callerAgentId: agent.identityCard.agentId,
        signingAuthority: agent.signingAuthority,
      }),
    ).pipe(Effect.map(requireBatch));
  return Effect.all(
    [
      continuePoll(agents.firstRecipient, anchors.firstRecipient.pollCursor),
      continuePoll(agents.secondRecipient, anchors.secondRecipient.pollCursor),
      continuePoll(agents.nonRecipient, anchors.nonRecipient.pollCursor),
    ] as const,
    { concurrency: 3 },
  );
};

const assertExactDeliveries = (
  signedMessage: SignedMessageValue,
  agents: RegisteredAgents,
  firstDelivery: BatchResult,
  secondDelivery: BatchResult,
) =>
  Effect.gen(function* () {
    const firstMessage = firstDelivery.signedMessages[0];
    const secondMessage = secondDelivery.signedMessages[0];
    if (firstMessage === undefined || secondMessage === undefined) {
      return yield* Effect.fail(
        processTestError("explicit recipient did not receive the message"),
      );
    }
    yield* Effect.all(
      [
        SignedMessage.verify({
          signedMessage: firstMessage,
          identityCard: agents.sender.identityCard,
        }),
        SignedMessage.verify({
          signedMessage: secondMessage,
          identityCard: agents.sender.identityCard,
        }),
      ] as const,
      { concurrency: 2 },
    );
    const [expected, firstActual, secondActual] = yield* Effect.all(
      [
        encodeSignedMessage(signedMessage),
        encodeSignedMessage(firstMessage),
        encodeSignedMessage(secondMessage),
      ] as const,
      { concurrency: 3 },
    );
    expect(firstActual).toStrictEqual(expected);
    expect(secondActual).toStrictEqual(expected);
  });

const sendAndReceive = (
  agents: RegisteredAgents,
  anchors: PollAnchors,
  runRouter: RouterRunner,
) =>
  Effect.gen(function* () {
    const signedMessage = yield* signMessage(agents);
    const { initialRequest, accepted } = yield* sendInitial(
      signedMessage,
      agents.sender,
      anchors.firstRecipient.routerInstanceId,
      runRouter,
    );
    const [firstDelivery, secondDelivery, nonRecipientDelivery] =
      yield* pollDeliveries(agents, anchors, runRouter);
    expect(firstDelivery.signedMessages).toHaveLength(1);
    expect(secondDelivery.signedMessages).toHaveLength(1);
    expect(nonRecipientDelivery.signedMessages).toHaveLength(0);
    expect(firstDelivery.routerInstanceId).toBe(accepted.routerInstanceId);
    expect(secondDelivery.routerInstanceId).toBe(accepted.routerInstanceId);
    yield* assertExactDeliveries(
      signedMessage,
      agents,
      firstDelivery,
      secondDelivery,
    );
    const retry = yield* runRouter(
      Router.send({
        request: { ...initialRequest, mode: "retry" },
        callerAgentId: agents.sender.identityCard.agentId,
        signingAuthority: agents.sender.signingAuthority,
      }),
    );
    expect(retry).toStrictEqual(accepted);
    return {
      initialRequest,
      accepted,
      firstRecipientDelivery: firstDelivery,
    } satisfies SentMessage;
  });

const assertRestartFence = (
  sentMessage: SentMessage,
  sender: RegisteredAgent,
  runRouter: RouterRunner,
) =>
  Effect.gen(function* () {
    const fencedSend = yield* runRouter(
      Router.send({
        request: sentMessage.initialRequest,
        callerAgentId: sender.identityCard.agentId,
        signingAuthority: sender.signingAuthority,
      }),
    );
    expect(fencedSend.kind).toBe(ROUTER_RESTARTED_KIND);
    if (fencedSend.kind !== ROUTER_RESTARTED_KIND) {
      return yield* Effect.fail(
        processTestError(`restart fence returned ${fencedSend.kind}`),
      );
    }
    expect(fencedSend.routerInstanceId).not.toBe(
      sentMessage.accepted.routerInstanceId,
    );
    return fencedSend.routerInstanceId;
  });

const assertRetryIndexReset = (
  sentMessage: SentMessage,
  sender: RegisteredAgent,
  currentRouterInstanceId: BatchResult["routerInstanceId"],
  runRouter: RouterRunner,
) =>
  Effect.gen(function* () {
    const retry = yield* runRouter(
      Router.send({
        request: {
          ...sentMessage.initialRequest,
          expectedRouterInstanceId: currentRouterInstanceId,
          mode: "retry",
        },
        callerAgentId: sender.identityCard.agentId,
        signingAuthority: sender.signingAuthority,
      }),
    );
    expect(retry).toStrictEqual({ kind: "retry_identity_unknown" });
  });

const assertMalformedMessageRemainsFenced = (
  infrastructure: Infrastructure,
  sentMessage: SentMessage,
  sender: RegisteredAgent,
  currentRouterInstanceId: BatchResult["routerInstanceId"],
) =>
  Effect.gen(function* () {
    const request = yield* signAuthenticatedRequest(
      infrastructure.routerOrigin,
      "/v1/messages:send",
      sender,
      {
        expectedRouterInstanceId: sentMessage.accepted.routerInstanceId,
        mode: "initial",
        signedMessage: null,
      },
    );
    expectHttpObservation(
      yield* observeHttpRequest(request),
      OK_STATUS,
      JSON.stringify({
        kind: ROUTER_RESTARTED_KIND,
        routerInstanceId: currentRouterInstanceId,
      }),
    );
  });

/** Noncanonical protected headers fail at the authenticated public poll boundary. */
const assertNoncanonicalCursorHeaders = (
  infrastructure: Infrastructure,
  firstRecipient: RegisteredAgent,
  anchor: BatchResult,
) =>
  Effect.forEach(
    [
      '{"enc":"A256GCM","alg":"dir","typ":"application/vnd.sharness.poll-cursor+jwe"}',
      '{"alg":"dir","enc":"A256GCM","typ":"application/vnd.sharness.poll-cursor+jwe","x":0}',
    ],
    (header) =>
      Effect.gen(function* () {
        const encodedHeader = Encoding.encodeBase64Url(
          new TextEncoder().encode(header),
        );
        const [, ...remainingSegments] = anchor.pollCursor.split(".");
        const request = yield* signAuthenticatedRequest(
          infrastructure.routerOrigin,
          "/v1/messages:poll",
          firstRecipient,
          { pollCursor: `plc_${encodedHeader}.${remainingSegments.join(".")}` },
        );
        expect(yield* observeHttpRequest(request), header).toStrictEqual({
          status: OK_STATUS,
          contentType: JSON_CONTENT_TYPE,
          body: JSON.stringify(CURSOR_INVALID_RESULT),
        });
      }),
    { concurrency: 1, discard: true },
  );

const assertCursorInvalidation = (
  sentMessage: SentMessage,
  firstRecipient: RegisteredAgent,
  currentRouterInstanceId: BatchResult["routerInstanceId"],
  runRouter: RouterRunner,
) =>
  Effect.gen(function* () {
    const invalidatedCursor = yield* runRouter(
      Router.poll({
        request: {
          pollCursor: sentMessage.firstRecipientDelivery.pollCursor,
        },
        callerAgentId: firstRecipient.identityCard.agentId,
        signingAuthority: firstRecipient.signingAuthority,
      }),
    );
    expect(invalidatedCursor).toStrictEqual(CURSOR_INVALID_RESULT);
    const newAnchor = requireBatch(
      yield* runRouter(
        Router.poll({
          request: {},
          callerAgentId: firstRecipient.identityCard.agentId,
          signingAuthority: firstRecipient.signingAuthority,
        }),
      ),
    );
    expect(newAnchor.routerInstanceId).toBe(currentRouterInstanceId);
    expect(newAnchor.signedMessages).toHaveLength(0);
  });

const assertCachedCallerSurvivesRegistryOutage = (
  infrastructure: Infrastructure,
  agents: RegisteredAgents,
  currentRouterInstanceId: BatchResult["routerInstanceId"],
  runRouter: RouterRunner,
) =>
  Effect.gen(function* () {
    yield* stopProcess(infrastructure.registryProcess);

    const cachedPoll = requireBatch(
      yield* runRouter(
        Router.poll({
          request: {},
          callerAgentId: agents.sender.identityCard.agentId,
          signingAuthority: agents.sender.signingAuthority,
        }),
      ),
    );
    expect(cachedPoll.routerInstanceId).toBe(currentRouterInstanceId);

    const signedMessage = yield* signMessage(agents);
    const cachedSend = yield* sendInitial(
      signedMessage,
      agents.sender,
      currentRouterInstanceId,
      runRouter,
    );
    expect(cachedSend.accepted.kind).toBe(ACCEPTED_KIND);

    const uncachedRequest = yield* signAuthenticatedRequest(
      infrastructure.routerOrigin,
      "/v1/messages:poll",
      agents.secondRecipient,
      {},
    );
    expectHttpObservation(
      yield* observeHttpRequest(uncachedRequest),
      UNAVAILABLE_STATUS,
      UNAVAILABLE_BODY,
    );
  });

const assertRestartBehavior = (
  infrastructure: Infrastructure,
  agents: RegisteredAgents,
  sentMessage: SentMessage,
  restartedRouter: RouterRunner,
) =>
  Effect.gen(function* () {
    const currentRouterInstanceId = yield* assertRestartFence(
      sentMessage,
      agents.sender,
      restartedRouter,
    );
    yield* assertRetryIndexReset(
      sentMessage,
      agents.sender,
      currentRouterInstanceId,
      restartedRouter,
    );
    yield* assertMalformedMessageRemainsFenced(
      infrastructure,
      sentMessage,
      agents.sender,
      currentRouterInstanceId,
    );
    yield* assertCursorInvalidation(
      sentMessage,
      agents.firstRecipient,
      currentRouterInstanceId,
      restartedRouter,
    );
    yield* assertCachedCallerSurvivesRegistryOutage(
      infrastructure,
      agents,
      currentRouterInstanceId,
      restartedRouter,
    );
  });

const runEndToEndBehavior = Effect.gen(function* () {
  const infrastructure = yield* startInfrastructure;
  const firstRouterProcess = yield* startRouter(infrastructure);
  const runRegistry = makeRegistryRunner(
    infrastructure.registryOrigin,
    infrastructure.registrySignerPublicKey,
  );
  const firstRouter = makeRouterRunner(infrastructure.routerOrigin);
  const agents = yield* registerAgents(runRegistry);
  yield* assertAgentLookups(agents, runRegistry);
  const anchors = yield* anchorFeeds(agents, firstRouter);
  yield* assertNoncanonicalCursorHeaders(
    infrastructure,
    agents.firstRecipient,
    anchors.firstRecipient,
  );
  yield* assertInvalidSignedMessageShapes(
    infrastructure,
    agents.sender,
    anchors.firstRecipient.routerInstanceId,
  );
  const sentMessage = yield* sendAndReceive(agents, anchors, firstRouter);
  yield* assertNonceCapacityWithoutEviction(infrastructure, agents.sender);

  yield* stopProcess(firstRouterProcess);
  yield* startRouter(infrastructure);
  const restartedRouter = makeRouterRunner(infrastructure.routerOrigin);
  yield* assertRestartBehavior(
    infrastructure,
    agents,
    sentMessage,
    restartedRouter,
  );
}).pipe(
  Effect.scoped,
  Effect.provide(NodeFileSystem.layer),
  Effect.provide(NodeHttpClient.layer),
);

it(
  "registers identities and preserves opaque multicast behavior across Router restart",
  processTrace(runEndToEndBehavior),
  120_000,
);

it(
  "returns the live versions from the real Router through the public client",
  processTrace(
    Effect.gen(function* () {
      const infrastructure = yield* startInfrastructure;
      yield* startRouter(infrastructure);
      const { signingAuthority, identityCard } = yield* registerAgent(
        "version-mismatch",
        makeRegistryRunner(
          infrastructure.registryOrigin,
          infrastructure.registrySignerPublicKey,
        ),
      );
      const error = yield* Router.poll({
        request: {},
        callerAgentId: identityCard.agentId,
        signingAuthority,
      }).pipe(
        Effect.provide(
          Router.layer({
            origin: infrastructure.routerOrigin,
            sendTimeout: Duration.seconds(5),
            pollTimeout: Duration.seconds(5),
          }),
        ),
        Effect.provideService(WireVersions, outsideWireWindow),
        Effect.flip,
      );
      expect(error).toMatchObject({
        _tag: "VersionMismatchError",
        supported: LIVE_WIRE_VERSIONS,
      });
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeFileSystem.layer),
      Effect.provide(NodeHttpClient.layer),
    ),
  ),
  60_000,
);

it(
  "reports a Registry version refusal through the closed public client error",
  processTrace(
    Effect.gen(function* () {
      const infrastructure = yield* startRegistryInfrastructure;
      const runRegistry = makeRegistryRunner(
        infrastructure.registryOrigin,
        infrastructure.registrySignerPublicKey,
      );
      const error = yield* runRegistry(
        Registry.lookup({
          agentName: Schema.decodeUnknownSync(AgentName)("version-mismatch"),
        }),
      ).pipe(
        Effect.provideService(WireVersions, outsideWireWindow),
        Effect.flip,
      );
      expect(error).toMatchObject({
        _tag: "RegistryError",
        reason: "http-envelope",
      });
    }).pipe(
      Effect.scoped,
      Effect.provide(NodeFileSystem.layer),
      Effect.provide(NodeHttpClient.layer),
    ),
  ),
  60_000,
);

/**
 * Another listener already holds the port PGlite is told to use.
 *
 * Value: protects=startPgliteRegistry fails with PGlite's own bind error, before any Registry starts, when another listener holds the PGlite port; fails_when=PGlite readiness accepts any listener on its port again; why_new=every other suite boots PGlite on a free port, where a connect probe and PGlite's listening line agree; seam=none; no_claim=how ports are reserved, how the Registry reports storage failures, or whether pglite-server exits on a bind error (0.1.4 logs it and exits later).
 */
const occupiedPgliteStartup = Effect.gen(function* () {
  const unreadyPhase = /process (exited|timed out) before listening/;
  const bindError = "EADDRINUSE";
  const { registryPrivateKeyPath } = yield* makeRegistryIdentity;
  const holder = yield* acquireLoopbackServer(() => Effect.void);
  const registryPort = yield* reservePort;
  const failure = yield* startPgliteRegistry({
    postgresqlPort: holder.port,
    registryPort,
    registryKeyPath: registryPrivateKeyPath,
    admissionCredential: ADMISSION_CREDENTIAL,
  }).pipe(Effect.flip);
  expect(failure.message).toMatch(unreadyPhase);
  expect(failure.message).toContain(bindError);
}).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer));

it(
  "reports PGlite's bind failure when another listener holds its port",
  processTrace(occupiedPgliteStartup),
  60_000,
);

/** Every concurrently registered daemon identity resolves through the Router. */
const concurrentRegistration = Effect.gen(function* () {
  const infrastructure = yield* acquireProcessInfrastructure;
  const fileSystem = yield* FileSystem.FileSystem;
  const runRouter = makeRouterRunner(infrastructure.routerOrigin);
  const fixtures = yield* acquireDaemonProcesses(infrastructure);
  yield* Effect.forEach(
    fixtures,
    (fixture) =>
      Effect.gen(function* () {
        const management = yield* acquireDaemonManagementClient(fixture);
        const registered = yield* management.register(
          makeRegistrationRequest(fixture),
        );
        const identityCard = registered;
        expect(identityCard.agentName).toBe(fixture.agentName);
        const privateKey = yield* fileSystem.readFileString(
          fixture.agentPrivateKeyFile,
        );
        const signingAuthority = yield* makeSigningAuthority(privateKey);
        const result = yield* runRouter(
          Router.poll({
            request: {},
            callerAgentId: identityCard.agentId,
            signingAuthority,
          }),
        );
        expect(result.kind).toBe(BATCH_KIND);
      }),
    { concurrency: CONCURRENT_DAEMONS, discard: true },
  );
}).pipe(
  Effect.scoped,
  Effect.provide(NodeFileSystem.layer),
  Effect.provide(NodeHttpClient.layer),
);

it(
  `registers ${CONCURRENT_DAEMONS} daemons concurrently and resolves every identity through the Router`,
  processTrace(concurrentRegistration),
  180_000,
);
