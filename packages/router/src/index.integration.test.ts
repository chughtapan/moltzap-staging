/** @file Black-box Router binary tests for framing, configuration, bounds, and shutdown. */
import { HttpClient } from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { WIRE_VERSION } from "@moltzap/social-harness";
import { Data, Duration, Effect, Option } from "effect";
import { type ChildProcess, spawn } from "node:child_process";
import { createPublicKey, generateKeyPairSync } from "node:crypto";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- Duplicate headers and malformed framing cannot be represented by Effect HttpClient.
import {
  type IncomingHttpHeaders,
  type IncomingMessage,
  request as makeHttpRequest,
} from "node:http";
import { createConnection, createServer, type Socket } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";

const LOOPBACK_HOST = "127.0.0.1";
const NO_CONTENT_STATUS = 204;
const EMPTY_BODY = "";
const STARTUP_TIMEOUT_MS = 30_000;
const SHUTDOWN_TIMEOUT_MS = 5_000;
const POLL_ATTEMPT_TIMEOUT = Duration.seconds(1);
const POLL_INTERVAL_MS = 25;
const RAW_REQUEST_TIMEOUT = Duration.seconds(5);
const JSON_CONTENT_TYPE = "application/json";
const MAXIMUM_POLL_BODY_LENGTH = 423;
const MAXIMUM_SEND_BODY_LENGTH = 471_825;
const OVERSIZED_POLL_BODY_LENGTH = MAXIMUM_POLL_BODY_LENGTH + 1;
const envelopes = Object.freeze({
  malformed: {
    status: 400,
    body: '{"error":"malformed"}',
  },
  notFound: {
    status: 404,
    body: '{"error":"not_found"}',
  },
  methodNotAllowed: {
    status: 405,
    body: '{"error":"method_not_allowed"}',
  },
  payloadTooLarge: {
    status: 413,
    body: '{"error":"payload_too_large"}',
  },
  unsupportedMediaType: {
    status: 415,
    body: '{"error":"unsupported_media_type"}',
  },
  overloaded: {
    status: 429,
    body: '{"error":"overloaded"}',
  },
});
const WORKSPACE_ROOT = fileURLToPath(new URL("../../..", import.meta.url));
const CHILD_PATH = `${dirname(process.execPath)}:/usr/bin:/bin`;
const ROUTER_BINARY = join(
  WORKSPACE_ROOT,
  "packages/router/bin/sharness-router",
);

interface RunningProcess {
  readonly child: ChildProcess;
  readonly logs: () => string;
}

interface RawHttpResponse {
  readonly status: number;
  readonly body: string;
  readonly headers: IncomingHttpHeaders;
}

type CompleteRawRequest = (
  result: Effect.Effect<RawHttpResponse, ProcessTestError>,
) => void;

class ProcessTestError extends Data.TaggedError("ProcessTestError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

const processTestError = (message: string, cause?: unknown): ProcessTestError =>
  new ProcessTestError({ message, cause });

const reservePort = Effect.async<number, ProcessTestError>((resume) => {
  const server = createServer();
  const onError = (error: Error): void => {
    resume(Effect.fail(processTestError("could not reserve a port", error)));
  };
  server.once("error", onError);
  server.listen(0, LOOPBACK_HOST, () => {
    server.removeListener("error", onError);
    const address = server.address();
    if (address === null || typeof address === "string") {
      server.close();
      resume(
        Effect.fail(processTestError("ephemeral listener exposed no port")),
      );
      return;
    }
    server.close((error) => {
      resume(
        error === undefined
          ? Effect.succeed(address.port)
          : Effect.fail(
              processTestError("could not release reserved port", error),
            ),
      );
    });
  });
  return Effect.sync(() => {
    if (server.listening) {
      server.close();
    }
  });
});

const startRouter = (
  port: number,
  registryPort: number,
  registrySignerPublicKey: string,
): RunningProcess => {
  const child = spawn(ROUTER_BINARY, [], {
    cwd: WORKSPACE_ROOT,
    env: {
      PATH: CHILD_PATH,
      SHARNESS_ROUTER_HOST: LOOPBACK_HOST,
      SHARNESS_ROUTER_PORT: String(port),
      SHARNESS_ROUTER_REGISTRY_ORIGIN: `http://${LOOPBACK_HOST}:${registryPort}`,
      SHARNESS_ROUTER_REGISTRY_SIGNER_PUBLIC_KEY: registrySignerPublicKey,
      SHARNESS_ROUTER_REQUEST_CONCURRENCY_LIMIT: "2",
      SHARNESS_ROUTER_HELD_POLL_CAPACITY: "1",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    output += chunk;
  });
  child.stderr?.on("data", (chunk: string) => {
    output += chunk;
  });
  return {
    child,
    logs: () => output,
  };
};

const canConnect = (port: number) =>
  Effect.async<boolean>((resume) => {
    const socket = createConnection({ host: LOOPBACK_HOST, port });
    const finish = (connected: boolean): void => {
      socket.destroy();
      resume(Effect.succeed(connected));
    };
    socket.once("connect", () => {
      finish(true);
    });
    socket.once("error", () => {
      finish(false);
    });
    return Effect.sync(() => {
      socket.destroy();
    });
  }).pipe(
    Effect.timeoutTo({
      duration: POLL_ATTEMPT_TIMEOUT,
      onTimeout: () => false,
      onSuccess: (connected) => connected,
    }),
  );

const receiveRawResponse =
  (requestDescription: string, complete: CompleteRawRequest) =>
  (response: IncomingMessage): void => {
    const chunks: Uint8Array[] = [];
    response.on("data", (chunk: Uint8Array) => {
      chunks.push(chunk);
    });
    response.once("error", (error) => {
      complete(
        Effect.fail(
          processTestError(
            `could not read ${requestDescription} response`,
            error,
          ),
        ),
      );
    });
    response.once("end", () => {
      complete(
        Effect.succeed({
          status: response.statusCode ?? 0,
          body: Buffer.concat(chunks).toString("utf8"),
          headers: response.headers,
        }),
      );
    });
  };

interface RawHttpRequest {
  readonly description: string;
  readonly port: number;
  readonly method: string;
  readonly path: string;
  readonly headers?: Readonly<Record<string, string | readonly string[]>>;
  readonly body?: string;
}

const beginRawHttpRequest = (input: RawHttpRequest) =>
  Effect.async<RawHttpResponse, ProcessTestError>((resume) => {
    let settled = false;
    const complete: CompleteRawRequest = (result) => {
      if (!settled) {
        settled = true;
        resume(result);
      }
    };
    const request = makeHttpRequest(
      {
        agent: false,
        host: LOOPBACK_HOST,
        port: input.port,
        method: input.method,
        path: input.path,
        headers: {
          connection: "close",
          ...input.headers,
        },
      },
      receiveRawResponse(input.description, complete),
    );
    request.once("error", (error) => {
      complete(
        Effect.fail(
          processTestError(
            `${input.description} failed: ${String(error)}`,
            error,
          ),
        ),
      );
    });
    if (input.body !== undefined) {
      request.write(input.body);
    }
    request.end();
    return Effect.sync(() => {
      request.destroy();
    });
  });

const rawHttpRequest = (input: RawHttpRequest) =>
  beginRawHttpRequest(input).pipe(
    Effect.timeoutFail({
      duration: RAW_REQUEST_TIMEOUT,
      onTimeout: () => processTestError("raw HTTP request timed out"),
    }),
  );

const parseRawSocketResponse = (
  description: string,
  bytes: Uint8Array,
): Effect.Effect<RawHttpResponse, ProcessTestError> => {
  const response = Buffer.from(bytes).toString("utf8");
  const separator = response.indexOf("\r\n\r\n");
  if (separator < 0) {
    return Effect.fail(
      processTestError(`${description} returned no HTTP header block`),
    );
  }
  const headerLines = response.slice(0, separator).split("\r\n");
  const statusLine = headerLines[0] ?? "";
  const statusMatch = /^HTTP\/1\.[01] (\d{3}) /.exec(statusLine);
  if (statusMatch?.[1] === undefined) {
    return Effect.fail(
      processTestError(`${description} returned an invalid status line`),
    );
  }
  const contentTypeLine = headerLines.slice(1).find((line) => {
    return line.toLowerCase().startsWith("content-type:");
  });
  const contentType = contentTypeLine?.slice("content-type:".length).trim();
  return Effect.succeed({
    status: Number(statusMatch[1]),
    body: response.slice(separator + 4),
    headers: contentType === undefined ? {} : { "content-type": contentType },
  });
};

const beginRawSocketRequest = (
  port: number,
  description: string,
  request: string,
) =>
  Effect.async<RawHttpResponse, ProcessTestError>((resume) => {
    const chunks: Uint8Array[] = [];
    const socket = createConnection({ host: LOOPBACK_HOST, port });
    socket.on("data", (chunk: Uint8Array) => {
      chunks.push(chunk);
    });
    socket.once("connect", () => {
      socket.end(request);
    });
    socket.once("error", (error) => {
      resume(
        Effect.fail(
          processTestError(`${description} failed: ${String(error)}`, error),
        ),
      );
    });
    socket.once("end", () => {
      resume(parseRawSocketResponse(description, Buffer.concat(chunks)));
    });
    return Effect.sync(() => {
      socket.destroy();
    });
  });

const rawSocketRequest = (port: number, description: string, request: string) =>
  beginRawSocketRequest(port, description, request).pipe(
    Effect.timeoutFail({
      duration: RAW_REQUEST_TIMEOUT,
      onTimeout: () => processTestError("raw socket request timed out"),
    }),
  );

const chunkedPollRequest = (
  body: string,
  connection: "close" | "keep-alive",
  complete: boolean,
): string => {
  const lines = [
    "POST /v1/messages:poll HTTP/1.1",
    `Host: ${LOOPBACK_HOST}`,
    `Connection: ${connection}`,
    `Content-Type: ${JSON_CONTENT_TYPE}`,
    `Sharness-Version: ${WIRE_VERSION}`,
    "Transfer-Encoding: chunked",
    "",
    body.length.toString(16),
    body,
  ];
  if (complete) {
    lines.push("0", "", "");
  } else {
    lines.push("");
  }
  return lines.join("\r\n");
};

const openSlowChunkedPoll = (
  port: number,
): Effect.Effect<Socket, ProcessTestError> =>
  Effect.async<Socket, ProcessTestError>((resume) => {
    const socket = createConnection({ host: LOOPBACK_HOST, port });
    const onError = (error: Error): void => {
      resume(Effect.fail(processTestError("slow chunked poll failed", error)));
    };
    socket.once("error", onError);
    socket.once("connect", () => {
      socket.removeListener("error", onError);
      socket.on("error", () => {
        socket.destroy();
      });
      socket.write(chunkedPollRequest("{", "keep-alive", false), () => {
        resume(Effect.succeed(socket));
      });
    });
    return Effect.sync(() => {
      socket.destroy();
    });
  });

const closeSockets = (sockets: readonly Socket[]) =>
  Effect.sync(() => {
    for (const socket of sockets) {
      socket.destroy();
    }
  });

const expectEnvelope = (
  response: RawHttpResponse,
  expected: Readonly<{ status: number; body: string }>,
): void => {
  expect(response.status).toBe(expected.status);
  expect(response.body).toBe(expected.body);
  expect(response.headers["content-type"]).toBe(JSON_CONTENT_TYPE);
};

const assertStreamingAdmissionBound = (port: number) =>
  Effect.gen(function* () {
    yield* Effect.scoped(
      Effect.gen(function* () {
        const holders = yield* Effect.acquireRelease(
          Effect.all([openSlowChunkedPoll(port), openSlowChunkedPoll(port)], {
            concurrency: 2,
          }),
          closeSockets,
        );
        expect(holders.every((socket) => !socket.destroyed)).toBe(true);
        yield* Effect.sleep(Duration.millis(POLL_INTERVAL_MS * 4));

        const declaredOversize = yield* rawHttpRequest({
          description: "declared oversize while request permits are full",
          port,
          method: "POST",
          path: "/v1/messages:poll",
          headers: {
            "content-length": String(OVERSIZED_POLL_BODY_LENGTH),
            "content-type": JSON_CONTENT_TYPE,
            "sharness-version": WIRE_VERSION,
          },
        });
        expectEnvelope(declaredOversize, envelopes.payloadTooLarge);

        const competingStream = yield* rawSocketRequest(
          port,
          "competing chunked poll",
          chunkedPollRequest("{}", "close", true),
        );
        expectEnvelope(competingStream, envelopes.overloaded);
      }),
    );
    yield* Effect.sleep(Duration.millis(POLL_INTERVAL_MS));

    const streamedOversize = yield* rawSocketRequest(
      port,
      "admitted oversize chunked poll",
      chunkedPollRequest("x".repeat(OVERSIZED_POLL_BODY_LENGTH), "close", true),
    );
    expectEnvelope(streamedOversize, envelopes.payloadTooLarge);
  });

const assertRouteAndFramingEnvelopes = (port: number) =>
  Effect.gen(function* () {
    const query = yield* rawHttpRequest({
      description: "query-bearing health request",
      port,
      method: "GET",
      path: "/healthz?",
    });
    expectEnvelope(query, envelopes.notFound);

    const wrongMethod = yield* rawHttpRequest({
      description: "wrong-method health request",
      port,
      method: "POST",
      path: "/healthz",
    });
    expectEnvelope(wrongMethod, envelopes.methodNotAllowed);

    const duplicateContentType = yield* rawHttpRequest({
      description: "duplicate-content-type poll request",
      port,
      method: "POST",
      path: "/v1/messages:poll",
      headers: {
        "content-type": [JSON_CONTENT_TYPE, JSON_CONTENT_TYPE],
        "sharness-version": WIRE_VERSION,
      },
      body: "{}",
    });
    expectEnvelope(duplicateContentType, envelopes.malformed);

    const duplicateHost = yield* rawSocketRequest(
      port,
      "duplicate-host poll request",
      [
        "POST /v1/messages:poll HTTP/1.1",
        `Host: ${LOOPBACK_HOST}`,
        "Host: duplicate.invalid",
        "Connection: close",
        `Content-Type: ${JSON_CONTENT_TYPE}`,
        `Sharness-Version: ${WIRE_VERSION}`,
        "Content-Length: 2",
        "",
        "{}",
      ].join("\r\n"),
    );
    expectEnvelope(duplicateHost, envelopes.malformed);
  });

const assertReceivedBodyBound = (
  port: number,
  path: string,
  maximumBodyLength: number,
  routeName: string,
) =>
  Effect.gen(function* () {
    const acceptedBySizeGate = yield* rawHttpRequest({
      description: `maximum-size ${routeName} request`,
      port,
      method: "POST",
      path,
      headers: {
        "content-type": JSON_CONTENT_TYPE,
        "sharness-version": WIRE_VERSION,
      },
      body: "x".repeat(maximumBodyLength),
    });
    expectEnvelope(acceptedBySizeGate, envelopes.malformed);

    const rejectedBySizeGate = yield* rawHttpRequest({
      description: `oversize ${routeName} request`,
      port,
      method: "POST",
      path,
      headers: {
        "content-type": JSON_CONTENT_TYPE,
        "sharness-version": WIRE_VERSION,
      },
      body: "x".repeat(maximumBodyLength + 1),
    });
    expectEnvelope(rejectedBySizeGate, envelopes.payloadTooLarge);
  });

const assertMediaAndBodyBoundEnvelopes = (port: number) =>
  Effect.gen(function* () {
    const unsupportedMediaType = yield* rawHttpRequest({
      description: "unsupported-media-type poll request",
      port,
      method: "POST",
      path: "/v1/messages:poll",
      headers: {
        "content-type": "text/plain",
        "sharness-version": WIRE_VERSION,
      },
      body: "{}",
    });
    expectEnvelope(unsupportedMediaType, envelopes.unsupportedMediaType);

    const declaredOversize = yield* rawHttpRequest({
      description: "declared-oversize poll request",
      port,
      method: "POST",
      path: "/v1/messages:poll",
      headers: {
        "content-length": String(OVERSIZED_POLL_BODY_LENGTH),
        "content-type": JSON_CONTENT_TYPE,
        "sharness-version": WIRE_VERSION,
      },
    });
    expectEnvelope(declaredOversize, envelopes.payloadTooLarge);

    yield* assertReceivedBodyBound(
      port,
      "/v1/messages:poll",
      MAXIMUM_POLL_BODY_LENGTH,
      "poll",
    );
    yield* assertReceivedBodyBound(
      port,
      "/v1/messages:send",
      MAXIMUM_SEND_BODY_LENGTH,
      "send",
    );
  });

const assertPreAuthenticationEnvelopes = (port: number) =>
  Effect.all(
    [
      assertRouteAndFramingEnvelopes(port),
      assertMediaAndBodyBoundEnvelopes(port),
    ],
    { concurrency: 1, discard: true },
  );

/**
 * The Router prints its listening line only after binding `origin`'s port,
 * so another process answering on a released reservation cannot pass for it.
 */
const announcedListener = (runningProcess: RunningProcess, origin: URL) =>
  runningProcess
    .logs()
    .includes(`sharness-router listening on ${origin.origin} (`);

const waitForHealth = (runningProcess: RunningProcess, origin: URL) =>
  Effect.gen(function* () {
    const deadline = Date.now() + STARTUP_TIMEOUT_MS;
    const healthUrl = new URL("/healthz", origin);
    while (Date.now() < deadline) {
      if (
        runningProcess.child.exitCode !== null ||
        runningProcess.child.signalCode !== null
      ) {
        return yield* Effect.fail(
          processTestError(
            `Router exited before readiness\n${runningProcess.logs()}`,
          ),
        );
      }
      const response = yield* HttpClient.get(healthUrl).pipe(
        Effect.flatMap((result) =>
          result.text.pipe(
            Effect.map((body) => ({ status: result.status, body })),
          ),
        ),
        Effect.timeout(POLL_ATTEMPT_TIMEOUT),
        Effect.option,
      );
      if (
        Option.isSome(response) &&
        response.value.status === NO_CONTENT_STATUS &&
        announcedListener(runningProcess, origin)
      ) {
        return response.value;
      }
      yield* Effect.sleep(Duration.millis(POLL_INTERVAL_MS));
    }
    return yield* Effect.fail(
      processTestError(`Router did not become ready\n${runningProcess.logs()}`),
    );
  });

const waitForExit = (runningProcess: RunningProcess) => {
  if (
    runningProcess.child.exitCode !== null ||
    runningProcess.child.signalCode !== null
  ) {
    return Effect.void;
  }
  return Effect.async<undefined>((resume) => {
    const onExit = (): void => {
      resume(Effect.succeed(undefined));
    };
    runningProcess.child.once("exit", onExit);
    return Effect.sync(() => {
      runningProcess.child.removeListener("exit", onExit);
    });
  });
};

const stopProcess = (runningProcess: RunningProcess) =>
  Effect.gen(function* () {
    if (
      runningProcess.child.exitCode !== null ||
      runningProcess.child.signalCode !== null
    ) {
      return;
    }
    runningProcess.child.kill("SIGTERM");
    const stopped = yield* Effect.raceFirst(
      waitForExit(runningProcess).pipe(Effect.as(true)),
      Effect.sleep(Duration.millis(SHUTDOWN_TIMEOUT_MS)).pipe(Effect.as(false)),
    );
    if (!stopped) {
      runningProcess.child.kill("SIGKILL");
      yield* waitForExit(runningProcess);
    }
  });

const makeRegistrySignerPublicKey = (): string => {
  const { privateKey } = generateKeyPairSync("ed25519");
  const publicJwk = createPublicKey(privateKey).export({ format: "jwk" });
  if (
    publicJwk.crv !== "Ed25519" ||
    publicJwk.kty !== "OKP" ||
    typeof publicJwk.x !== "string"
  ) {
    throw processTestError("Node did not produce an Ed25519 public JWK");
  }
  return JSON.stringify({
    crv: publicJwk.crv,
    kty: publicJwk.kty,
    x: publicJwk.x,
  });
};

const routerBinaryBehavior = Effect.gen(function* () {
  const routerPort = yield* reservePort;
  const unavailableRegistryPort = yield* reservePort;
  const router = yield* Effect.acquireRelease(
    Effect.sync(() =>
      startRouter(
        routerPort,
        unavailableRegistryPort,
        makeRegistrySignerPublicKey(),
      ),
    ),
    stopProcess,
  );

  const origin = new URL(`http://${LOOPBACK_HOST}:${routerPort}`);
  const health = yield* waitForHealth(router, origin);
  expect(health.status).toBe(NO_CONTENT_STATUS);
  expect(health.body).toBe(EMPTY_BODY);
  yield* assertPreAuthenticationEnvelopes(routerPort);
  yield* assertStreamingAdmissionBound(routerPort);

  yield* stopProcess(router);
  expect(router.logs()).toContain(
    `sharness-router listening on ${origin.origin} (`,
  );
  expect(router.logs()).toContain(`WIRE_VERSION ${WIRE_VERSION}`);
  expect(router.logs()).toMatch(/\(WIRE_VERSION \S+\)/u);
  expect(router.logs().match(/sharness-router listening on/gu)).toHaveLength(1);
  expect(
    router.child.exitCode !== null || router.child.signalCode !== null,
  ).toBe(true);
  expect(yield* canConnect(routerPort)).toBe(false);
}).pipe(Effect.scoped, Effect.provide(NodeHttpClient.layer));

it("runs independently of Registry availability and releases its listener", () => {
  expect.hasAssertions();
  return Effect.runPromise(routerBinaryBehavior);
}, 45_000);
