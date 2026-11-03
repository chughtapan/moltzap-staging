/** @file Black-box Registry binary tests for framing, contracts, bounds, and shutdown. */

import * as Reactivity from "@effect/experimental/Reactivity";
import { FileSystem, HttpClient, HttpClientRequest } from "@effect/platform";
import { NodeFileSystem, NodeHttpClient } from "@effect/platform-node";
import { PgClient } from "@effect/sql-pg";
import {
  AgentIdentityCard,
  AgentName,
  AgentSigningAuthority,
  type VerifiedAgentIdentityCard,
  WIRE_VERSION,
} from "@moltzap/social-harness";
import { registryContract } from "@moltzap/social-harness/internal";
import { Registry } from "@moltzap/social-harness/service";
import { Duration, Effect, Redacted, Ref, Schema } from "effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import {
  callRegister,
  canConnect,
  LOOPBACK_HOST,
  makeCapturedRegistryRunner,
  makeOperationId,
  makePrincipalId,
  makePrivateKeyPem,
  makeRegistryRunner,
  makeSigningAuthority,
  makeVersionedBootstrapRequest,
  managedProcess,
  PGLITE_BINARY,
  prepareAgent,
  processTestError,
  type RawHttpResponse,
  rawSocketRequest,
  registerAgent,
  type RegisteredAgent,
  REGISTRY_BINARY,
  registryEnvironment,
  type RegistryRunner,
  requireCapturedRequest,
  reservePort,
  type RunningProcess,
  sendRawRequest,
  startReadyRegistry,
  stopProcess,
  waitForExit,
  waitForHealthStatus,
  waitForPgliteListening,
} from "./registry-fixture.js";
import { contentDigest, encodeCanonicalJson } from "./signed-requests.js";

const REGISTERED_KIND = "registered";
const NOT_FOUND_KIND = "not_found";
const IDEMPOTENCY_CONFLICT_KIND = "idempotency_conflict";
const KEY_ALREADY_REGISTERED_KIND = "key_already_registered";
const NAME_TAKEN_KIND = "name_taken";
const FOUND_KIND = "found";
const INCOMPATIBLE_STORAGE_REASON = "incompatible";
const OK_STATUS = 200;
const NO_CONTENT_STATUS = 204;
const BAD_REQUEST_STATUS = 400;
const UNAUTHORIZED_STATUS = 401;
const NOT_FOUND_STATUS = 404;
const METHOD_NOT_ALLOWED_STATUS = 405;
const VERSION_MISMATCH_STATUS = 412;
const PAYLOAD_TOO_LARGE_STATUS = 413;
const UNSUPPORTED_MEDIA_TYPE_STATUS = 415;
const OVERLOADED_STATUS = 429;
const MALFORMED_BODY = '{"error":"malformed"}';
const AUTHENTICATION_FAILED_BODY = '{"error":"authentication_failed"}';
const NOT_FOUND_BODY = '{"error":"not_found"}';
const METHOD_NOT_ALLOWED_BODY = '{"error":"method_not_allowed"}';
const VERSION_MISMATCH_BODY = JSON.stringify({
  error: "version_mismatch",
  supported: [WIRE_VERSION],
});
const PAYLOAD_TOO_LARGE_BODY = '{"error":"payload_too_large"}';
const UNSUPPORTED_MEDIA_TYPE_BODY = '{"error":"unsupported_media_type"}';
const OVERLOADED_BODY = '{"error":"overloaded"}';
const EMPTY_BODY = "";
const OVERFLOW_BODY_BYTES = 4_096;
const IDENTITY_POINT_COORDINATE = "AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const IDENTITY_POINT_KEY_ID =
  "urn:ietf:params:oauth:jwk-thumbprint:sha-256:eV9frzBXPTP92MWWMpoFOh0WI_kJLvGlhcNs15APU_s";
const UNIVERSAL_SIGNATURE_BYTES = Buffer.from(
  `01${"00".repeat(63)}`,
  "hex",
).toString("base64");
const UNIVERSAL_SIGNATURE = `sharness=:${UNIVERSAL_SIGNATURE_BYTES}:`;
const expectStartupFailure = (process: RunningProcess) =>
  Effect.gen(function* () {
    const exited = yield* Effect.raceFirst(
      waitForExit(process).pipe(Effect.as(true)),
      Effect.sleep(Duration.seconds(10)).pipe(Effect.as(false)),
    );
    if (!exited) {
      return yield* Effect.fail(
        processTestError(`Registry did not reject startup\n${process.logs()}`),
      );
    }
    expect(process.child.exitCode).not.toBe(0);
  });

const expectExactCard = (
  actual: VerifiedAgentIdentityCard,
  expected: VerifiedAgentIdentityCard,
) =>
  Effect.gen(function* () {
    const [actualEncoded, expectedEncoded] = yield* Effect.all(
      [
        Schema.encode(AgentIdentityCard)(actual),
        Schema.encode(AgentIdentityCard)(expected),
      ] as const,
      { concurrency: 2 },
    );
    expect(actualEncoded).toStrictEqual(expectedEncoded);
  });

const assertRegistrationPrecedence = (
  first: RegisteredAgent,
  runRegistry: RegistryRunner,
) =>
  Effect.gen(function* () {
    const repeated = yield* callRegister(runRegistry, first);
    expect(repeated.kind).toBe(REGISTERED_KIND);
    if (repeated.kind === REGISTERED_KIND) {
      yield* expectExactCard(repeated.identityCard, first.identityCard);
    }

    const changedRequest = {
      ...first.request,
      agentName: Schema.decodeUnknownSync(AgentName)("changed-name"),
    };
    const conflict = yield* callRegister(runRegistry, {
      request: changedRequest,
      signingAuthority: first.signingAuthority,
    });
    expect(conflict.kind).toBe(IDEMPOTENCY_CONFLICT_KIND);

    const sameKey = yield* callRegister(runRegistry, {
      request: {
        ...first.request,
        operationId: makeOperationId(),
      },
      signingAuthority: first.signingAuthority,
    });
    expect(sameKey.kind).toBe(KEY_ALREADY_REGISTERED_KIND);

    const otherKey = yield* prepareAgent("temporary-name");
    const sameName = yield* callRegister(runRegistry, {
      ...otherKey,
      request: {
        ...otherKey.request,
        agentName: first.request.agentName,
      },
    });
    expect(sameName.kind).toBe(NAME_TAKEN_KIND);
    expect(
      (yield* runRegistry(
        Registry.lookup({ agentName: otherKey.request.agentName }),
      )).kind,
    ).toBe(NOT_FOUND_KIND);
  });

const assertWrongAdmission = (runRegistry: RegistryRunner) =>
  Effect.gen(function* () {
    const prepared = yield* prepareAgent("wrong-admission");
    const rejected = yield* callRegister(
      runRegistry,
      prepared,
      "wrong-admission-credential",
    ).pipe(
      Effect.as(false),
      Effect.catchTag("RegistryError", (error) =>
        error.reason === "admission-refused"
          ? Effect.succeed(true)
          : Effect.fail(error),
      ),
    );
    expect(rejected).toBe(true);
    expect(
      (yield* runRegistry(
        Registry.lookup({ agentName: prepared.request.agentName }),
      )).kind,
    ).toBe(NOT_FOUND_KIND);
  });

const assertLookup = (
  agent: RegisteredAgent,
  selector: Parameters<typeof Registry.lookup>[0],
  runRegistry: RegistryRunner,
) =>
  Effect.gen(function* () {
    const lookup = yield* runRegistry(Registry.lookup(selector));
    expect(lookup.kind).toBe(FOUND_KIND);
    if (lookup.kind === FOUND_KIND) {
      yield* expectExactCard(lookup.identityCard, agent.identityCard);
    }
  });

const executeRequest = (request: HttpClientRequest.HttpClientRequest) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.execute(request);
    return {
      status: response.status,
      body: yield* response.text,
    };
  });

const makeJsonRequest = (origin: URL, path: string, body: string) =>
  HttpClientRequest.post(new URL(path, origin)).pipe(
    HttpClientRequest.bodyUint8Array(
      new TextEncoder().encode(body),
      "application/json",
    ),
    HttpClientRequest.setHeader("sharness-version", WIRE_VERSION),
  );

const assertBoundaryResponse = (
  response: RawHttpResponse,
  expected: RawHttpResponse,
): void => {
  expect(response).toStrictEqual(expected);
};

type SharedEnvelopeCase = Readonly<{
  name: string;
  request: Omit<Parameters<typeof sendRawRequest>[0], "port">;
  expected: RawHttpResponse;
}>;

const sharedEnvelopeCases: readonly SharedEnvelopeCase[] = [
  {
    name: "health",
    request: { method: "GET", path: "/healthz" },
    expected: { status: NO_CONTENT_STATUS, body: EMPTY_BODY },
  },
  {
    name: "query route",
    request: { method: "GET", path: "/healthz?probe=1" },
    expected: { status: NOT_FOUND_STATUS, body: NOT_FOUND_BODY },
  },
  {
    name: "wrong method",
    request: { method: "POST", path: "/healthz" },
    expected: {
      status: METHOD_NOT_ALLOWED_STATUS,
      body: METHOD_NOT_ALLOWED_BODY,
    },
  },
  {
    name: "duplicate framing",
    request: {
      method: "POST",
      path: "/v1/identities:lookup",
      headers: [
        ["content-type", "application/json"],
        ["sharness-version", WIRE_VERSION],
        ["sharness-version", WIRE_VERSION],
      ],
      body: "{}",
    },
    expected: { status: BAD_REQUEST_STATUS, body: MALFORMED_BODY },
  },
  {
    name: "unsupported media",
    request: {
      method: "POST",
      path: "/v1/identities:lookup",
      headers: [
        ["content-type", "text/plain"],
        ["sharness-version", WIRE_VERSION],
      ],
      body: "{}",
    },
    expected: {
      status: UNSUPPORTED_MEDIA_TYPE_STATUS,
      body: UNSUPPORTED_MEDIA_TYPE_BODY,
    },
  },
  {
    name: "declared size",
    request: {
      method: "POST",
      path: "/v1/identities:register",
      headers: [
        ["content-type", "application/json"],
        ["content-length", "4096"],
        ["sharness-version", WIRE_VERSION],
      ],
    },
    expected: {
      status: PAYLOAD_TOO_LARGE_STATUS,
      body: PAYLOAD_TOO_LARGE_BODY,
    },
  },
];

const assertReceivedOverflow = (port: number) => {
  const body = "x".repeat(OVERFLOW_BODY_BYTES);
  const request = [
    "POST /v1/identities:register HTTP/1.1",
    `Host: ${LOOPBACK_HOST}:${port}`,
    "Content-Type: application/json",
    `Sharness-Version: ${WIRE_VERSION}`,
    "Transfer-Encoding: chunked",
    "Connection: close",
    "",
    OVERFLOW_BODY_BYTES.toString(16),
    body,
    "0",
    "",
    "",
  ].join("\r\n");
  return rawSocketRequest(port, "received-size request", request).pipe(
    Effect.map((response) => {
      assertBoundaryResponse(response, {
        status: PAYLOAD_TOO_LARGE_STATUS,
        body: PAYLOAD_TOO_LARGE_BODY,
      });
      return response;
    }),
  );
};

const assertSharedHttpEnvelopes = (port: number) =>
  Effect.gen(function* () {
    yield* Effect.forEach(
      sharedEnvelopeCases,
      (scenario) =>
        sendRawRequest({
          port,
          description: scenario.name,
          ...scenario.request,
        }).pipe(
          Effect.map((response) => {
            expect(response, scenario.name).toStrictEqual(scenario.expected);
            return response;
          }),
        ),
      { concurrency: 1, discard: true },
    );
    yield* assertReceivedOverflow(port);
  });

const assertBootstrapVersionPrecedence = (
  origin: URL,
  runRegistry: RegistryRunner,
) =>
  Effect.gen(function* () {
    const prepared = yield* prepareAgent("version-precedence");
    const request = yield* makeVersionedBootstrapRequest(
      origin,
      prepared,
      "incompatible",
    );
    assertBoundaryResponse(yield* executeRequest(request), {
      status: VERSION_MISMATCH_STATUS,
      body: VERSION_MISMATCH_BODY,
    });
    expect(
      (yield* runRegistry(
        Registry.lookup({ agentName: prepared.request.agentName }),
      )).kind,
    ).toBe(NOT_FOUND_KIND);
    assertBoundaryResponse(yield* executeRequest(request), {
      status: UNAUTHORIZED_STATUS,
      body: AUTHENTICATION_FAILED_BODY,
    });
    expect(
      (yield* runRegistry(
        Registry.lookup({ agentName: prepared.request.agentName }),
      )).kind,
    ).toBe(NOT_FOUND_KIND);
  });

const assertRetiredAdmissionSchemeRejected = (
  origin: URL,
  runRegistry: RegistryRunner,
) =>
  Effect.gen(function* () {
    const prepared = yield* prepareAgent("retired-admission-scheme");
    const request = yield* makeVersionedBootstrapRequest(
      origin,
      prepared,
      WIRE_VERSION,
      "MoltZap-Admission",
    );
    assertBoundaryResponse(yield* executeRequest(request), {
      status: UNAUTHORIZED_STATUS,
      body: AUTHENTICATION_FAILED_BODY,
    });
    expect(
      (yield* runRegistry(
        Registry.lookup({ agentName: prepared.request.agentName }),
      )).kind,
    ).toBe(NOT_FOUND_KIND);
  });

const makeIdentityPointForgery = (origin: URL) =>
  Effect.gen(function* () {
    const prepared = yield* prepareAgent("identity-point-forgery");
    const encoded = yield* Schema.encode(
      registryContract.RegistryRegisterRequest,
    )(prepared.request);
    const bodyBytes = yield* encodeCanonicalJson({
      request: {
        ...encoded,
        publicKey: {
          crv: "Ed25519",
          kty: "OKP",
          x: IDENTITY_POINT_COORDINATE,
        },
      },
    });
    const signed = yield* makeVersionedBootstrapRequest(
      origin,
      prepared,
      WIRE_VERSION,
    );
    const signatureInput = signed.headers["signature-input"];
    if (signatureInput === undefined) {
      return yield* Effect.fail(
        processTestError("signed request omitted Signature-Input"),
      );
    }
    const forged = signed.pipe(
      HttpClientRequest.bodyUint8Array(bodyBytes, "application/json"),
      HttpClientRequest.setHeader("content-digest", contentDigest(bodyBytes)),
      HttpClientRequest.setHeader(
        "signature-input",
        signatureInput.replace(
          /keyid="[^"]+"/u,
          `keyid="${IDENTITY_POINT_KEY_ID}"`,
        ),
      ),
      HttpClientRequest.setHeader("signature", UNIVERSAL_SIGNATURE),
    );
    return { forged, agentName: prepared.request.agentName };
  });

const assertIdentityPointForgeryRejected = (
  origin: URL,
  runRegistry: RegistryRunner,
) =>
  Effect.gen(function* () {
    const { forged, agentName } = yield* makeIdentityPointForgery(origin);
    assertBoundaryResponse(yield* executeRequest(forged), {
      status: UNAUTHORIZED_STATUS,
      body: AUTHENTICATION_FAILED_BODY,
    });
    expect((yield* runRegistry(Registry.lookup({ agentName }))).kind).toBe(
      NOT_FOUND_KIND,
    );
  });

const assertPublicAuthenticationFraming = (origin: URL) =>
  Effect.gen(function* () {
    const authenticationFraming = makeJsonRequest(
      origin,
      "/v1/identities:lookup",
      "not-json",
    ).pipe(
      HttpClientRequest.setHeader(
        "authorization",
        "Sharness-Admission ignored",
      ),
      HttpClientRequest.setHeader("sharness-version", "incompatible"),
    );
    assertBoundaryResponse(yield* executeRequest(authenticationFraming), {
      status: BAD_REQUEST_STATUS,
      body: MALFORMED_BODY,
    });
  });

const assertPublicRequestBoundaries = (origin: URL) =>
  Effect.gen(function* () {
    const maximumLookup = JSON.stringify({
      agentName: "a".repeat(32),
    });
    assertBoundaryResponse(
      yield* executeRequest(
        makeJsonRequest(origin, "/v1/identities:lookup", maximumLookup),
      ),
      { status: OK_STATUS, body: '{"kind":"not_found"}' },
    );
    assertBoundaryResponse(
      yield* executeRequest(
        makeJsonRequest(origin, "/v1/identities:lookup", `${maximumLookup} `),
      ),
      { status: PAYLOAD_TOO_LARGE_STATUS, body: PAYLOAD_TOO_LARGE_BODY },
    );
  });

const assertRejectedRepresentations = (origin: URL, first: RegisteredAgent) =>
  Effect.gen(function* () {
    const unknownLookup = JSON.stringify({
      agentName: first.request.agentName,
      unknown: true,
    });
    const noncanonicalLookup = `{ "agentName":"${first.request.agentName}"}`;
    for (const body of [unknownLookup, noncanonicalLookup]) {
      assertBoundaryResponse(
        yield* executeRequest(
          makeJsonRequest(origin, "/v1/identities:lookup", body),
        ),
        { status: BAD_REQUEST_STATUS, body: MALFORMED_BODY },
      );
    }
    const malformedKey = JSON.stringify({
      request: {
        agentName: "malformed-key",
        operationId: makeOperationId(),
        principalId: makePrincipalId(),
        publicKey: { crv: "Ed25519", kty: "OKP", x: "bad" },
      },
    });
    assertBoundaryResponse(
      yield* executeRequest(
        makeJsonRequest(origin, "/v1/identities:register", malformedKey),
      ),
      { status: BAD_REQUEST_STATUS, body: MALFORMED_BODY },
    );
  });

const registerAtMaximumBody = (
  origin: URL,
  registrySignerPublicKey: ReturnType<typeof AgentSigningAuthority.publicKey>,
) =>
  Effect.gen(function* () {
    const captured = yield* makeCapturedRegistryRunner(
      origin,
      registrySignerPublicKey,
    );
    const prepared = yield* prepareAgent("m".repeat(32));
    const result = yield* callRegister(captured.run, prepared);
    expect(result.kind).toBe(REGISTERED_KIND);
    const signedRequest = yield* Ref.get(captured.capturedRequest).pipe(
      Effect.flatMap(requireCapturedRequest),
    );
    const wrongProof = signedRequest.pipe(
      HttpClientRequest.setHeader("signature", "sig1=:AAAA:"),
    );
    assertBoundaryResponse(yield* executeRequest(wrongProof), {
      status: UNAUTHORIZED_STATUS,
      body: AUTHENTICATION_FAILED_BODY,
    });
    const maximumBody = JSON.stringify({
      request: {
        agentName: prepared.request.agentName,
        operationId: prepared.request.operationId,
        principalId: prepared.request.principalId,
        publicKey: prepared.request.publicKey,
      },
    });
    assertBoundaryResponse(
      yield* executeRequest(
        makeJsonRequest(origin, "/v1/identities:register", `${maximumBody} `),
      ),
      { status: PAYLOAD_TOO_LARGE_STATUS, body: PAYLOAD_TOO_LARGE_BODY },
    );
  });

const assertConcurrentIdempotency = (runRegistry: RegistryRunner) =>
  Effect.gen(function* () {
    const idempotent = yield* prepareAgent("concurrent-idempotent");
    const repeated = yield* Effect.all(
      [
        callRegister(runRegistry, idempotent),
        callRegister(runRegistry, idempotent),
      ] as const,
      { concurrency: 2 },
    );
    expect(
      repeated.filter((result) => result.kind === REGISTERED_KIND),
    ).toHaveLength(2);
    if (
      repeated[0].kind === REGISTERED_KIND &&
      repeated[1].kind === REGISTERED_KIND
    ) {
      yield* expectExactCard(
        repeated[0].identityCard,
        repeated[1].identityCard,
      );
    }
  });

const assertConcurrentKey = (runRegistry: RegistryRunner) =>
  Effect.gen(function* () {
    const sameKey = yield* prepareAgent("concurrent-key");
    const keyResults = yield* Effect.all(
      [
        callRegister(runRegistry, sameKey),
        callRegister(runRegistry, {
          ...sameKey,
          request: {
            ...sameKey.request,
            operationId: makeOperationId(),
          },
        }),
      ] as const,
      { concurrency: 2 },
    );
    expect(
      keyResults.filter((result) => result.kind === REGISTERED_KIND),
    ).toHaveLength(1);
    expect(
      keyResults.filter(
        (result) => result.kind === KEY_ALREADY_REGISTERED_KIND,
      ),
    ).toHaveLength(1);
  });

const assertConcurrentName = (runRegistry: RegistryRunner) =>
  Effect.gen(function* () {
    const [firstName, secondName] = yield* Effect.all(
      [prepareAgent("concurrent-name"), prepareAgent("temporary-race")],
      { concurrency: 2 },
    );
    const nameResults = yield* Effect.all(
      [
        callRegister(runRegistry, firstName),
        callRegister(runRegistry, {
          ...secondName,
          request: {
            ...secondName.request,
            agentName: firstName.request.agentName,
          },
        }),
      ] as const,
      { concurrency: 2 },
    );
    expect(
      nameResults.filter((result) => result.kind === REGISTERED_KIND),
    ).toHaveLength(1);
    expect(
      nameResults.filter((result) => result.kind === NAME_TAKEN_KIND),
    ).toHaveLength(1);
  });

const assertConcurrentRegistration = (runRegistry: RegistryRunner) =>
  Effect.gen(function* () {
    yield* assertConcurrentIdempotency(runRegistry);
    yield* assertConcurrentKey(runRegistry);
    yield* assertConcurrentName(runRegistry);
  });

const exerciseFirstRun = (
  origin: URL,
  registrySignerPublicKey: ReturnType<typeof AgentSigningAuthority.publicKey>,
  registryPort: number,
) =>
  Effect.gen(function* () {
    const captured = yield* makeCapturedRegistryRunner(
      origin,
      registrySignerPublicKey,
    );
    const first = yield* registerAgent("first-agent", captured.run);
    const runRegistry = makeRegistryRunner(origin, registrySignerPublicKey);
    yield* assertRegistrationPrecedence(first, runRegistry);
    yield* assertWrongAdmission(runRegistry);
    yield* assertIdentityPointForgeryRejected(origin, runRegistry);
    yield* assertBootstrapVersionPrecedence(origin, runRegistry);
    yield* assertRetiredAdmissionSchemeRejected(origin, runRegistry);
    const second = yield* registerAgent("second-agent", runRegistry);
    const third = yield* registerAgent("third-agent", runRegistry);
    const fourth = yield* registerAgent("fourth-agent", runRegistry);
    yield* assertLookup(
      second,
      { agentName: second.request.agentName },
      runRegistry,
    );
    yield* assertLookup(
      third,
      { agentId: third.identityCard.agentId },
      runRegistry,
    );
    yield* assertLookup(
      fourth,
      { agentId: fourth.identityCard.agentId },
      runRegistry,
    );
    yield* assertPublicAuthenticationFraming(origin);
    yield* assertPublicRequestBoundaries(origin);
    yield* assertRejectedRepresentations(origin, first);
    yield* assertConcurrentRegistration(runRegistry);
    yield* registerAtMaximumBody(origin, registrySignerPublicKey);
    yield* assertSharedHttpEnvelopes(registryPort);
    const request = yield* Ref.get(captured.capturedRequest).pipe(
      Effect.flatMap(requireCapturedRequest),
    );
    return { first, request };
  });

const assertReplayRejected = (request: HttpClientRequest.HttpClientRequest) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const response = yield* client.execute(request);
    expect(response.status).toBe(UNAUTHORIZED_STATUS);
    expect(yield* response.text).toBe(AUTHENTICATION_FAILED_BODY);
  });

const exerciseRestart = (input: {
  readonly origin: URL;
  readonly environment: Readonly<Record<string, string>>;
  readonly registrySignerPublicKey: ReturnType<
    typeof AgentSigningAuthority.publicKey
  >;
  readonly first: RegisteredAgent;
  readonly request: HttpClientRequest.HttpClientRequest;
}) =>
  Effect.gen(function* () {
    const registry = yield* startReadyRegistry(input.environment, input.origin);
    yield* assertReplayRejected(input.request);
    const runRegistry = makeRegistryRunner(
      input.origin,
      input.registrySignerPublicKey,
    );
    const lookup = yield* runRegistry(
      Registry.lookup({ agentId: input.first.identityCard.agentId }),
    );
    expect(lookup.kind).toBe(FOUND_KIND);
    if (lookup.kind === FOUND_KIND) {
      yield* expectExactCard(lookup.identityCard, input.first.identityCard);
    }
    const repeated = yield* callRegister(runRegistry, input.first);
    expect(repeated.kind).toBe(REGISTERED_KIND);
    if (repeated.kind === REGISTERED_KIND) {
      yield* expectExactCard(repeated.identityCard, input.first.identityCard);
    }
    yield* stopProcess(registry);
  });

const assertNonceRetention = (input: {
  readonly origin: URL;
  readonly environment: Readonly<Record<string, string>>;
}) =>
  Effect.gen(function* () {
    const registry = yield* startReadyRegistry(
      {
        ...input.environment,
        SHARNESS_REGISTRY_LIVE_NONCE_CAPACITY: "1",
      },
      input.origin,
    );
    const [first, second] = yield* Effect.all(
      [prepareAgent("nonce-retained"), prepareAgent("nonce-refused")],
      { concurrency: 2 },
    );
    const [firstRequest, secondRequest] = yield* Effect.all(
      [
        makeVersionedBootstrapRequest(input.origin, first, WIRE_VERSION),
        makeVersionedBootstrapRequest(input.origin, second, WIRE_VERSION),
      ] as const,
      { concurrency: 2 },
    );
    expect((yield* executeRequest(firstRequest)).status).toBe(OK_STATUS);
    assertBoundaryResponse(yield* executeRequest(secondRequest), {
      status: OVERLOADED_STATUS,
      body: OVERLOADED_BODY,
    });
    assertBoundaryResponse(yield* executeRequest(firstRequest), {
      status: UNAUTHORIZED_STATUS,
      body: AUTHENTICATION_FAILED_BODY,
    });
    yield* stopProcess(registry);
  });

const assertStartupRejection = (
  environment: Readonly<Record<string, string>>,
) =>
  Effect.gen(function* () {
    const process = yield* managedProcess(REGISTRY_BINARY, [], environment);
    yield* expectStartupFailure(process);
  });

const setupRegistryInfrastructure = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const temporaryDirectory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "sharness-registry-binary-",
  });
  const registryKeyPath = join(temporaryDirectory, "registry-key.pem");
  const wrongKeyPath = join(temporaryDirectory, "wrong-key.pem");
  const privateKeyPem = makePrivateKeyPem();
  yield* Effect.all(
    [
      fileSystem.writeFileString(registryKeyPath, privateKeyPem, {
        mode: 0o600,
      }),
      fileSystem.writeFileString(wrongKeyPath, makePrivateKeyPem(), {
        mode: 0o600,
      }),
    ],
    { concurrency: 2, discard: true },
  );
  const signingAuthority = yield* makeSigningAuthority(privateKeyPem);
  const registrySignerPublicKey =
    AgentSigningAuthority.publicKey(signingAuthority);
  const [postgresqlPort, registryPort] = yield* Effect.all(
    [reservePort, reservePort] as const,
    { concurrency: 2 },
  );
  const postgresql = yield* managedProcess(PGLITE_BINARY, [
    "--db=memory://",
    `--port=${postgresqlPort}`,
    `--host=${LOOPBACK_HOST}`,
    "--max-connections=20",
  ]);
  yield* waitForPgliteListening(postgresql, postgresqlPort);
  const postgresqlUrl = `postgresql://postgres:postgres@${LOOPBACK_HOST}:${postgresqlPort}/postgres`;
  const origin = new URL(`http://${LOOPBACK_HOST}:${registryPort}`);
  const environment = registryEnvironment({
    port: registryPort,
    postgresqlUrl,
    registryKeyPath,
    overrides: { SHARNESS_REGISTRY_SQL_POOL_SIZE: "1" },
  });
  return {
    environment,
    origin,
    postgresqlUrl,
    registryPort,
    registrySignerPublicKey,
    wrongKeyPath,
  };
});

const assertStartupContracts = (input: {
  readonly environment: Readonly<Record<string, string>>;
  readonly wrongKeyPath: string;
}) =>
  Effect.gen(function* () {
    yield* assertStartupRejection({
      ...input.environment,
      SHARNESS_REGISTRY_ADMISSION_CREDENTIAL: "short",
    });
    yield* assertStartupRejection({
      ...input.environment,
      SHARNESS_REGISTRY_SIGNING_PRIVATE_KEY_PATH: input.wrongKeyPath,
    });
  });

const registryBinaryBehavior = Effect.gen(function* () {
  const infrastructure = yield* setupRegistryInfrastructure;
  const registry = yield* startReadyRegistry(
    infrastructure.environment,
    infrastructure.origin,
  );
  const firstRun = yield* exerciseFirstRun(
    infrastructure.origin,
    infrastructure.registrySignerPublicKey,
    infrastructure.registryPort,
  );
  yield* stopProcess(registry);
  expect(registry.logs()).toContain(
    `sharness-registry listening on ${infrastructure.origin.origin}`,
  );
  expect(registry.logs()).toMatch(/\(WIRE_VERSION \S+\)/u);
  expect(
    registry.logs().match(/sharness-registry listening on/gu),
  ).toHaveLength(1);
  expect(yield* canConnect(infrastructure.registryPort)).toBe(false);

  yield* exerciseRestart({
    origin: infrastructure.origin,
    environment: infrastructure.environment,
    registrySignerPublicKey: infrastructure.registrySignerPublicKey,
    first: firstRun.first,
    request: firstRun.request,
  });

  const sql = yield* PgClient.make({
    url: Redacted.make(infrastructure.postgresqlUrl),
    maxConnections: 2,
    connectTimeout: Duration.seconds(5),
    applicationName: "sharness-registry-acceptance",
  }).pipe(Effect.provide(Reactivity.layer));
  yield* sql`DELETE FROM sharness_registry_nonces`;
  yield* assertNonceRetention({
    origin: infrastructure.origin,
    environment: infrastructure.environment,
  });
  yield* assertStartupContracts({
    environment: infrastructure.environment,
    wrongKeyPath: infrastructure.wrongKeyPath,
  });
}).pipe(
  Effect.scoped,
  Effect.provide(NodeFileSystem.layer),
  Effect.provide(NodeHttpClient.layer),
);

// @agent-code-guard/regression-only: These process scenarios cover first boot, restart, and refusal of occupied schemas, including quoted-name case collisions.
it("preserves Registry contracts across first boot, concurrency, and restart", () => {
  expect.hasAssertions();
  return Effect.runPromise(registryBinaryBehavior);
}, 120_000);

it.each(["public", "Registry"])(
  "refuses unrecognized storage in %s without changing its tables",
  (schema) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const infrastructure = yield* setupRegistryInfrastructure;
        const sql = yield* PgClient.make({
          url: Redacted.make(infrastructure.postgresqlUrl),
          maxConnections: 1,
        }).pipe(Effect.provide(Reactivity.layer));
        yield* sql`CREATE SCHEMA IF NOT EXISTS ${sql(schema)}`;
        yield* sql`CREATE SCHEMA registry`;
        yield* sql`SET search_path TO ${sql(schema)}`;
        yield* sql`CREATE TABLE unrecognized_state (retained integer PRIMARY KEY)`;
        yield* sql`INSERT INTO unrecognized_state VALUES (1)`;
        const tables =
          yield* sql`SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN ('public', 'Registry', 'registry') ORDER BY schemaname, tablename`;
        const process = yield* managedProcess(
          REGISTRY_BINARY,
          [],
          infrastructure.environment,
        );
        yield* expectStartupFailure(process);
        expect(process.logs()).toContain(INCOMPATIBLE_STORAGE_REASON);
        expect(
          yield* sql`SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname IN ('public', 'Registry', 'registry') ORDER BY schemaname, tablename`,
        ).toEqual(tables);
        expect(yield* sql`SELECT retained FROM unrecognized_state`).toEqual([
          { retained: 1 },
        ]);
      }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
    ),
  30_000,
);

it(
  "initializes and reopens a quoted Registry schema independently of registry",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const infrastructure = yield* setupRegistryInfrastructure;
        const sql = yield* PgClient.make({
          url: Redacted.make(infrastructure.postgresqlUrl),
          maxConnections: 1,
        }).pipe(Effect.provide(Reactivity.layer));
        yield* sql`CREATE SCHEMA "Registry"`;
        yield* sql`CREATE SCHEMA registry`;
        yield* sql`SET search_path TO "Registry"`;
        const registry = yield* startReadyRegistry(
          infrastructure.environment,
          infrastructure.origin,
        );
        const run = makeRegistryRunner(
          infrastructure.origin,
          infrastructure.registrySignerPublicKey,
        );
        const registered = yield* registerAgent("quoted-schema-agent", run);
        yield* stopProcess(registry);
        yield* startReadyRegistry(
          infrastructure.environment,
          infrastructure.origin,
        );
        const lookup = yield* run(
          Registry.lookup({ agentId: registered.identityCard.agentId }),
        );
        expect(lookup.kind).toBe(FOUND_KIND);
        if (lookup.kind === FOUND_KIND) {
          yield* expectExactCard(lookup.identityCard, registered.identityCard);
        }
        expect(
          yield* sql`SELECT DISTINCT schemaname FROM pg_catalog.pg_tables WHERE schemaname IN ('public', 'Registry', 'registry') ORDER BY schemaname`,
        ).toEqual([{ schemaname: "Registry" }]);
      }).pipe(
        Effect.scoped,
        Effect.provide(NodeFileSystem.layer),
        Effect.provide(NodeHttpClient.layer),
      ),
    ),
  30_000,
);

/** The external caller has the package links that an installed consumer receives. */
const makeExternalConsumer = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "sharness-registry-consumer-",
  });
  const scopeDirectory = join(directory, "node_modules", "@moltzap");
  yield* fileSystem.makeDirectory(scopeDirectory, { recursive: true });
  yield* fileSystem.symlink(
    fileURLToPath(new URL("../../../packages/registry", import.meta.url)),
    join(scopeDirectory, "registry"),
  );
  yield* fileSystem.symlink(
    fileURLToPath(new URL("../../../packages/social-harness", import.meta.url)),
    join(scopeDirectory, "social-harness"),
  );
  return directory;
});

it(
  "test composition accepts an adjacent registration with explicit process conditions",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const infrastructure = yield* setupRegistryInfrastructure;
        const cwd = yield* makeExternalConsumer;
        const adjacent = `${WIRE_VERSION}-adjacent`;
        const versions = {
          emitted: WIRE_VERSION,
          live: [WIRE_VERSION, adjacent],
          history: [WIRE_VERSION, adjacent],
        };
        const registry = yield* managedProcess(
          process.execPath,
          [
            "--conditions=sharness-test",
            "--conditions=sharness-internal",
            "--input-type=module",
            "-e",
            `import { startRegistry } from "@moltzap/registry/test";
         startRegistry(${JSON.stringify(versions)});`,
          ],
          infrastructure.environment,
          cwd,
        );
        yield* waitForHealthStatus({
          runningProcess: registry,
          origin: infrastructure.origin,
          expectedStatus: NO_CONTENT_STATUS,
        });
        const prepared = yield* prepareAgent("adjacent-test-entry");
        const request = yield* makeVersionedBootstrapRequest(
          infrastructure.origin,
          prepared,
          adjacent,
        );
        const response = yield* executeRequest(request);
        expect(response.status, response.body).toBe(OK_STATUS);
        const result: unknown = JSON.parse(response.body);
        expect(result).toHaveProperty("kind", REGISTERED_KIND);
      }).pipe(
        Effect.scoped,
        Effect.provide(NodeFileSystem.layer),
        Effect.provide(NodeHttpClient.layer),
      ),
    ),
  60_000,
);
