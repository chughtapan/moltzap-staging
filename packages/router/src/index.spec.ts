/** @file Public Router capability tests across HTTP success and typed failure paths. */
import { it, live as startupIt } from "@effect/vitest";
import {
  AgentId,
  AgentIdentityCard,
  type AgentId as AgentIdValue,
  AgentName,
  AgentSigningAuthority,
  EnvelopeId,
  type EnvelopeId as EnvelopeIdValue,
  PrincipalId,
  SignedMessage,
  type SignedMessage as SignedMessageValue,
  WIRE_VERSION,
} from "@moltzap/social-harness";
import {
  httpErrors,
  routerContract,
  type VerifiedAgentRequest,
} from "@moltzap/social-harness/internal";
import canonicalize from "canonicalize";
import {
  Brand,
  ConfigError,
  ConfigProvider,
  Data,
  Effect,
  Either,
  Encoding,
  Layer,
  Redacted,
  Schema,
} from "effect";
import { calculateJwkThumbprintUri, GeneralSign, importPKCS8 } from "jose";
import { generateKeyPairSync } from "node:crypto";
import {
  createServer as createNetServer,
  type Server as NetServer,
} from "node:net";
import { describe, expect } from "vitest";
import { configurationMessage, loadRouterConfiguration } from "./config.js";
import { makeRouterFeedAtOrder, maximumPrivateOrder } from "./feed.js";
import { RouterServer } from "./index.js";
import { makePollWaiters } from "./poll.js";
import { makeRouterSend } from "./send.js";

/* eslint sonarjs/no-empty-test-file: "off" -- Effect's scoped registrations are invisible to Sonar's direct Vitest-call detector. */
/* eslint-disable max-lines-per-function, agent-code-guard/no-hardcoded-assertion-literals -- Each scenario observes the deep capability at its public or private call boundary. */
const utf8Encoder = new TextEncoder();

const identifierPayload = (seed: number): string =>
  Encoding.encodeBase64Url(new Uint8Array(16).fill(seed));

const makeAgentId = (seed: number): AgentIdValue =>
  Schema.decodeUnknownSync(AgentId)(`agt_${identifierPayload(seed)}`);

const makeEnvelopeId = (seed: number): EnvelopeIdValue =>
  Schema.decodeUnknownSync(EnvelopeId)(`env_${identifierPayload(seed)}`);

const makeRouterInstanceId = (
  seed: number,
): typeof routerContract.RouterInstanceId.Type =>
  Schema.decodeUnknownSync(routerContract.RouterInstanceId)(
    `rti_${identifierPayload(seed)}`,
  );

const canonicalText = (value: unknown): string => {
  const text = canonicalize(value);
  if (text === undefined) {
    throw new Error("test fixture is not canonical JSON");
  }
  return text;
};

const encodedCanonicalJson = (value: unknown): string =>
  Encoding.encodeBase64Url(utf8Encoder.encode(canonicalText(value)));

const makeSignedMessage = (input: {
  readonly senderAgentId: AgentIdValue;
  readonly recipientAgentIds: readonly AgentIdValue[];
  readonly envelopeId: EnvelopeIdValue;
  readonly body: string;
}): SignedMessageValue =>
  Schema.decodeUnknownSync(SignedMessage)({
    payload: encodedCanonicalJson({
      kind: "signedMessage",
      sharnessVersion: WIRE_VERSION,
      senderAgentId: input.senderAgentId,
      agentCardDigest: `acd_${Encoding.encodeBase64Url(new Uint8Array(32).fill(9))}`,
      recipientAgentIds: input.recipientAgentIds,
      envelopeId: input.envelopeId,
      body: Encoding.encodeBase64Url(utf8Encoder.encode(input.body)),
    }),
    signatures: [
      {
        protected: encodedCanonicalJson({
          alg: "Ed25519",
          kid: "urn:test:sender",
          typ: "application/vnd.sharness.signed-message+jws",
        }),
        signature:
          "KDxTl7gpBIOcp3KzWrPaXGOI8uSNov6xWPXKa421caAinNAc-_pYc_kzBuUde6eY0Ayp21se-jZdCvMGLlbYDg",
      },
    ],
  });

const sender = makeAgentId(1);
const recipient = makeAgentId(2);
const registrySignerPublicKey =
  '{"crv":"Ed25519","kty":"OKP","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}';
const requiredConfiguration = new Map([
  ["SHARNESS_ROUTER_PORT", "3000"],
  ["SHARNESS_ROUTER_REGISTRY_ORIGIN", "http://127.0.0.1:3001"],
  ["SHARNESS_ROUTER_REGISTRY_SIGNER_PUBLIC_KEY", registrySignerPublicKey],
]);
const LOOPBACK_HOST = "127.0.0.1";

interface OccupiedListener {
  readonly server: NetServer;
  readonly port: number;
}

class RouterTestFixtureError extends Data.TaggedError(
  "RouterTestFixtureError",
) {}

const effectFails = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<boolean, never, R> =>
  effect.pipe(
    Effect.either,
    Effect.map(
      Either.match({
        onLeft: () => true,
        onRight: () => false,
      }),
    ),
  );

const loadConfigurationFrom = (values: ReadonlyMap<string, string>) =>
  loadRouterConfiguration.pipe(
    Effect.withConfigProvider(ConfigProvider.fromMap(new Map(values))),
  );

const loadConfiguration = (overrides?: ReadonlyMap<string, string>) =>
  loadConfigurationFrom(
    new Map([...requiredConfiguration, ...(overrides ?? [])]),
  );

const makePrivateKeyPem = (): string =>
  generateKeyPairSync("ed25519").privateKey.export({
    format: "pem",
    type: "pkcs8",
  });

const makeSigningAuthority = () => {
  return AgentSigningAuthority.fromPkcs8(Redacted.make(makePrivateKeyPem()));
};

const makeRegisteredSenderFixture = () =>
  Effect.gen(function* () {
    const agentSigningAuthority = yield* makeSigningAuthority();
    const registryPrivateKey = makePrivateKeyPem();
    const registrySigningAuthority = yield* AgentSigningAuthority.fromPkcs8(
      Redacted.make(registryPrivateKey),
    );
    const registrySignerPublicKey = AgentSigningAuthority.publicKey(
      registrySigningAuthority,
    );
    const registryPrivateCryptoKey = yield* Effect.tryPromise({
      try: () => importPKCS8(registryPrivateKey, "Ed25519"),
      catch: () => new RouterTestFixtureError(),
    });
    const registryKeyId = yield* Effect.tryPromise({
      try: () => calculateJwkThumbprintUri(registrySignerPublicKey, "sha256"),
      catch: () => new RouterTestFixtureError(),
    });
    const payload = utf8Encoder.encode(
      canonicalText({
        kind: "agentCard",
        sharnessVersion: WIRE_VERSION,
        agentId: sender,
        principalId: Schema.decodeUnknownSync(PrincipalId)(
          `prn_${identifierPayload(6)}`,
        ),
        agentName: Schema.decodeUnknownSync(AgentName)("registered-sender"),
        publicKey: AgentSigningAuthority.publicKey(agentSigningAuthority),
        issuedAt: "2026-07-30T12:00:00Z",
      }),
    );
    const representation = yield* Effect.tryPromise({
      try: () =>
        new GeneralSign(payload)
          .addSignature(registryPrivateCryptoKey)
          .setProtectedHeader({
            alg: "Ed25519",
            kid: registryKeyId,
            typ: "application/vnd.sharness.agent-card+jws",
          })
          .sign(),
      catch: () => new RouterTestFixtureError(),
    });
    const parsedCard =
      yield* Schema.decodeUnknown(AgentIdentityCard)(representation);
    const identityCard = yield* AgentIdentityCard.verify({
      identityCard: parsedCard,
      registrySignerPublicKey,
    });
    return {
      signingAuthority: agentSigningAuthority,
      verifiedRequest: Brand.nominal<VerifiedAgentRequest>()(
        Object.freeze({
          callerAgentId: sender,
          identityCard,
          request: Object.freeze({}),
        }),
      ),
    };
  });

const acquireOccupiedListener = Effect.async<
  OccupiedListener,
  RouterTestFixtureError
>((resume) => {
  const server = createNetServer();
  const onError = (): void => {
    resume(Effect.fail(new RouterTestFixtureError()));
  };
  server.once("error", onError);
  server.listen(0, LOOPBACK_HOST, () => {
    server.removeListener("error", onError);
    const address = server.address();
    if (address === null || typeof address === "string") {
      server.close();
      resume(Effect.fail(new RouterTestFixtureError()));
      return;
    }
    resume(Effect.succeed({ server, port: address.port }));
  });
  return Effect.sync(() => {
    server.close();
  });
});

const releaseOccupiedListener = (
  occupied: OccupiedListener,
): Effect.Effect<undefined> =>
  Effect.async<undefined>((resume) => {
    occupied.server.close(() => {
      resume(Effect.succeed(undefined));
    });
  });

describe("send exhaustion behavior", () => {
  it.effect(
    "preserves instance, validation, retry, and conflict precedence at the final position",
    () =>
      Effect.gen(function* () {
        const routerInstanceId = makeRouterInstanceId(1);
        const feed = yield* makeRouterFeedAtOrder({
          routerInstanceId,
          retainedMessageCapacity: 2,
          retainedMessageByteCapacity: 2_000_000,
          initialTailOrder: maximumPrivateOrder - 1n,
        });
        const send = makeRouterSend({
          routerInstanceId,
          feed,
          pollWaiters: yield* makePollWaiters(1),
        });
        const senderFixture = yield* makeRegisteredSenderFixture();
        const finalMessage = yield* SignedMessage.sign({
          identityCard: senderFixture.verifiedRequest.identityCard,
          signingAuthority: senderFixture.signingAuthority,
          recipientAgentIds: new Set([recipient]),
          envelopeId: makeEnvelopeId(1),
          body: utf8Encoder.encode("final-position"),
        });
        const finalRequest = {
          expectedRouterInstanceId: routerInstanceId,
          mode: "initial",
          signedMessage: Schema.encodeSync(SignedMessage)(finalMessage),
        } as const;
        const accepted = yield* send(
          finalRequest,
          senderFixture.verifiedRequest,
        );
        expect(accepted.kind).toBe("accepted");
        expect((yield* feed.snapshot).tailOrder).toBe(maximumPrivateOrder);

        expect(
          yield* send(
            {
              ...finalRequest,
              expectedRouterInstanceId: makeRouterInstanceId(2),
              signedMessage: null,
            },
            senderFixture.verifiedRequest,
          ),
        ).toEqual({
          kind: "router_restarted",
          routerInstanceId,
        });
        expect(
          yield* send(
            {
              ...finalRequest,
              signedMessage: Schema.encodeSync(SignedMessage)(
                makeSignedMessage({
                  senderAgentId: sender,
                  recipientAgentIds: [recipient],
                  envelopeId: makeEnvelopeId(2),
                  body: "invalid-signature",
                }),
              ),
            },
            senderFixture.verifiedRequest,
          ),
        ).toEqual({ kind: "message_invalid" });
        expect(
          yield* send(finalRequest, senderFixture.verifiedRequest),
        ).toEqual({ kind: "idempotency_conflict" });
        expect(
          yield* send(
            { ...finalRequest, mode: "retry" },
            senderFixture.verifiedRequest,
          ),
        ).toEqual(accepted);

        const nextMessage = yield* SignedMessage.sign({
          identityCard: senderFixture.verifiedRequest.identityCard,
          signingAuthority: senderFixture.signingAuthority,
          recipientAgentIds: new Set([recipient]),
          envelopeId: makeEnvelopeId(2),
          body: utf8Encoder.encode("past-final-position"),
        });
        const exhausted = yield* send(
          {
            expectedRouterInstanceId: routerInstanceId,
            mode: "initial",
            signedMessage: Schema.encodeSync(SignedMessage)(nextMessage),
          },
          senderFixture.verifiedRequest,
        ).pipe(Effect.either);
        Either.match(exhausted, {
          onLeft: (error) => {
            expect(error).toBeInstanceOf(httpErrors.OverloadedError);
          },
          onRight: () => {
            expect.fail("fresh append succeeded past the final position");
          },
        });
        expect((yield* feed.snapshot).tailOrder).toBe(maximumPrivateOrder);
      }),
  );
});

describe("configuration behavior", () => {
  it.effect("loads only declared values and owns its defaults", () =>
    Effect.gen(function* () {
      const defaults = yield* loadConfiguration(
        new Map([["SHARNESS_ROUTER_UNUSED", "ignored"]]),
      );
      expect(defaults).toMatchObject({
        host: "127.0.0.1",
        port: 3000,
        retainedMessageCapacity: 4_096,
        retainedMessageByteCapacity: 67_108_864,
        pollMessageLimit: 128,
        pollResponseByteLimit: 1_048_576,
        requestConcurrencyLimit: 512,
        heldPollCapacity: 256,
        liveNonceCapacity: 100_000,
        identityCardCacheCapacity: 10_000,
        registryLookupConcurrencyLimit: 32,
        registryLookupTimeoutMs: 5_000,
      });
      expect(defaults.registryOrigin.href).toBe("http://127.0.0.1:3001/");
      expect(defaults.registrySignerPublicKey).toEqual({
        crv: "Ed25519",
        kty: "OKP",
        x: "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo",
      });

      const configured = yield* loadConfiguration(
        new Map([
          ["SHARNESS_ROUTER_HOST", "0.0.0.0"],
          ["SHARNESS_ROUTER_PORT", "3002"],
          ["SHARNESS_ROUTER_REGISTRY_ORIGIN", "https://registry.example"],
          ["SHARNESS_ROUTER_RETAINED_MESSAGE_CAPACITY", "2"],
          ["SHARNESS_ROUTER_RETAINED_MESSAGE_BYTE_CAPACITY", "500000"],
          ["SHARNESS_ROUTER_POLL_MESSAGE_LIMIT", "1"],
          [
            "SHARNESS_ROUTER_POLL_RESPONSE_BYTE_LIMIT",
            String(
              routerContract.routerRepresentationLimits.oneMessageBatchBytes,
            ),
          ],
          ["SHARNESS_ROUTER_REQUEST_CONCURRENCY_LIMIT", "4"],
          ["SHARNESS_ROUTER_HELD_POLL_CAPACITY", "3"],
          ["SHARNESS_ROUTER_LIVE_NONCE_CAPACITY", "5"],
          ["SHARNESS_ROUTER_AGENT_CARD_CACHE_CAPACITY", "6"],
          ["SHARNESS_ROUTER_REGISTRY_LOOKUP_CONCURRENCY_LIMIT", "2"],
          ["SHARNESS_ROUTER_REGISTRY_LOOKUP_TIMEOUT_MS", "7"],
        ]),
      );
      expect(configured).toMatchObject({
        host: "0.0.0.0",
        port: 3002,
        retainedMessageCapacity: 2,
        retainedMessageByteCapacity: 500_000,
        pollMessageLimit: 1,
        pollResponseByteLimit:
          routerContract.routerRepresentationLimits.oneMessageBatchBytes,
        requestConcurrencyLimit: 4,
        heldPollCapacity: 3,
        liveNonceCapacity: 5,
        identityCardCacheCapacity: 6,
        registryLookupConcurrencyLimit: 2,
        registryLookupTimeoutMs: 7,
      });
      expect(configured.registryOrigin.href).toBe("https://registry.example/");
      expect(
        (yield* loadConfiguration(new Map([["SHARNESS_ROUTER_HOST", "::"]])))
          .host,
      ).toBe("::");
      expect(
        yield* effectFails(
          routerContract.calculateRouterRepresentationLimits(
            Number.MAX_SAFE_INTEGER,
          ),
        ),
      ).toBe(true);
    }),
  );

  it.effect("rejects invalid values and inconsistent bounds", () =>
    Effect.gen(function* () {
      const numericKeys = [
        "SHARNESS_ROUTER_PORT",
        "SHARNESS_ROUTER_RETAINED_MESSAGE_CAPACITY",
        "SHARNESS_ROUTER_RETAINED_MESSAGE_BYTE_CAPACITY",
        "SHARNESS_ROUTER_POLL_MESSAGE_LIMIT",
        "SHARNESS_ROUTER_POLL_RESPONSE_BYTE_LIMIT",
        "SHARNESS_ROUTER_REQUEST_CONCURRENCY_LIMIT",
        "SHARNESS_ROUTER_HELD_POLL_CAPACITY",
        "SHARNESS_ROUTER_LIVE_NONCE_CAPACITY",
        "SHARNESS_ROUTER_AGENT_CARD_CACHE_CAPACITY",
        "SHARNESS_ROUTER_REGISTRY_LOOKUP_CONCURRENCY_LIMIT",
        "SHARNESS_ROUTER_REGISTRY_LOOKUP_TIMEOUT_MS",
      ] as const;
      const invalidCases = [
        ...numericKeys.map((key) => [key, new Map([[key, "0"]])] as const),
        [
          "noncanonical integer",
          new Map([["SHARNESS_ROUTER_PORT", "+3000"]]),
        ] as const,
        [
          "fractional integer",
          new Map([["SHARNESS_ROUTER_PORT", "3000.0"]]),
        ] as const,
        [
          "negative integer",
          new Map([["SHARNESS_ROUTER_PORT", "-1"]]),
        ] as const,
        [
          "exponent integer",
          new Map([["SHARNESS_ROUTER_PORT", "3e3"]]),
        ] as const,
        [
          "whitespace integer",
          new Map([["SHARNESS_ROUTER_PORT", " 3000"]]),
        ] as const,
        ["empty bind host", new Map([["SHARNESS_ROUTER_HOST", ""]])] as const,
        [
          "port above range",
          new Map([["SHARNESS_ROUTER_PORT", "65536"]]),
        ] as const,
        [
          "process integer above range",
          new Map([["SHARNESS_ROUTER_LIVE_NONCE_CAPACITY", "2147483648"]]),
        ] as const,
        [
          "origin with route path",
          new Map([
            ["SHARNESS_ROUTER_REGISTRY_ORIGIN", "http://127.0.0.1:3001/path"],
          ]),
        ] as const,
        [
          "origin with user information",
          new Map([
            ["SHARNESS_ROUTER_REGISTRY_ORIGIN", "http://user@127.0.0.1:3001"],
          ]),
        ] as const,
        [
          "origin with query",
          new Map([
            [
              "SHARNESS_ROUTER_REGISTRY_ORIGIN",
              "http://127.0.0.1:3001?lookup=1",
            ],
          ]),
        ] as const,
        [
          "origin with fragment",
          new Map([
            ["SHARNESS_ROUTER_REGISTRY_ORIGIN", "http://127.0.0.1:3001#lookup"],
          ]),
        ] as const,
        [
          "noncanonical signer key",
          new Map([
            [
              "SHARNESS_ROUTER_REGISTRY_SIGNER_PUBLIC_KEY",
              '{"kty":"OKP","crv":"Ed25519","x":"11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo"}',
            ],
          ]),
        ] as const,
        [
          "retention cannot hold one message",
          new Map([
            [
              "SHARNESS_ROUTER_RETAINED_MESSAGE_BYTE_CAPACITY",
              String(SignedMessage.maximumEncodedByteLength - 1),
            ],
          ]),
        ] as const,
        [
          "response cannot hold one message",
          new Map([
            [
              "SHARNESS_ROUTER_POLL_RESPONSE_BYTE_LIMIT",
              String(
                routerContract.routerRepresentationLimits.oneMessageBatchBytes -
                  1,
              ),
            ],
          ]),
        ] as const,
        [
          "held polls consume every request permit",
          new Map([
            ["SHARNESS_ROUTER_REQUEST_CONCURRENCY_LIMIT", "4"],
            ["SHARNESS_ROUTER_HELD_POLL_CAPACITY", "4"],
          ]),
        ] as const,
      ];
      yield* Effect.forEach(
        invalidCases,
        ([caseName, overrides]) =>
          effectFails(loadConfiguration(overrides)).pipe(
            Effect.map((failed) => {
              expect(failed, caseName).toBe(true);
              return failed;
            }),
          ),
        { concurrency: 1, discard: true },
      );
      yield* Effect.forEach(
        [...requiredConfiguration.keys()],
        (missingKey) => {
          const withoutRequiredValue = new Map(requiredConfiguration);
          withoutRequiredValue.delete(missingKey);
          return effectFails(loadConfigurationFrom(withoutRequiredValue)).pipe(
            Effect.map((failed) => {
              expect(failed, missingKey).toBe(true);
              return failed;
            }),
          );
        },
        { concurrency: 1, discard: true },
      );
    }),
  );

  it.scoped("reports configuration and listener startup phases", () =>
    Effect.gen(function* () {
      const configurationOutcome = yield* Layer.build(RouterServer.layer).pipe(
        Effect.withConfigProvider(ConfigProvider.fromMap(new Map())),
        Effect.either,
      );
      Either.match(configurationOutcome, {
        onLeft: (error) => {
          expect(error).toBeInstanceOf(RouterServer.StartupError);
          expect(error.phase).toBe("configuration");
        },
        onRight: () => {
          expect.fail("Router started without required configuration");
        },
      });

      const occupied = yield* Effect.acquireRelease(
        acquireOccupiedListener,
        releaseOccupiedListener,
      );
      const listenerOutcome = yield* Layer.build(RouterServer.layer).pipe(
        Effect.withConfigProvider(
          ConfigProvider.fromMap(
            new Map([
              ...requiredConfiguration,
              ["SHARNESS_ROUTER_HOST", LOOPBACK_HOST],
              ["SHARNESS_ROUTER_PORT", String(occupied.port)],
            ]),
          ),
        ),
        Effect.either,
      );
      Either.match(listenerOutcome, {
        onLeft: (error) => {
          expect(error).toBeInstanceOf(RouterServer.StartupError);
          expect(error.phase).toBe("listener");
        },
        onRight: () => {
          expect.fail("Router acquired an occupied listener");
        },
      });
    }),
  );
});

/* eslint-enable max-lines-per-function, agent-code-guard/no-hardcoded-assertion-literals -- Restore defaults after the process scenarios. */

const registryOriginVariable = "SHARNESS_ROUTER_REGISTRY_ORIGIN";
const rejectedValue =
  "https://operator:private-password@registry.example/private";
const environment = ConfigProvider.fromMap(
  new Map([[registryOriginVariable, rejectedValue]]),
);

startupIt(
  "removes configured values from startup error serialization and causes",
  () =>
    Effect.gen(function* () {
      const error = yield* Layer.build(RouterServer.layer).pipe(
        Effect.scoped,
        Effect.withConfigProvider(environment),
        Effect.flip,
      );
      expect(error).toBeInstanceOf(RouterServer.StartupError);
      expect(error.message).toContain(registryOriginVariable);
      expect(JSON.stringify(error.toJSON())).not.toContain(rejectedValue);
      expect(String(error)).not.toContain(rejectedValue);
      expect(error.cause).toBeUndefined();
    }),
);

startupIt(
  "replaces unrecognized decoder messages with a fixed diagnostic",
  () =>
    Effect.sync(() => {
      expect(
        configurationMessage(ConfigError.InvalidData([], rejectedValue)),
      ).not.toContain(rejectedValue);
    }),
);
