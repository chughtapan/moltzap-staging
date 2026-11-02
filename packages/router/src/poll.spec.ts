/** @file Stateful feed, cursor, polling, and retry invariants for one Router instance. */
import { it as effectIt } from "@effect/vitest";
import {
  AgentId,
  type AgentId as AgentIdValue,
  EnvelopeId,
  type EnvelopeId as EnvelopeIdValue,
  type LiveSignedMessage,
  SignedMessage,
  WIRE_VERSION,
} from "@moltzap/social-harness";
import {
  AgentIdentityCardDigest,
  routerContract,
} from "@moltzap/social-harness/internal";
import canonicalize from "canonicalize";
import { Effect, Either, Encoding, Fiber, Schema } from "effect";
import * as fc from "fast-check";
import { CompactEncrypt } from "jose";
import { describe, expect, it } from "vitest";
import {
  entriesAfter,
  makeRouterFeed,
  makeRouterFeedAtOrder,
  maximumPrivateOrder,
  type RouterFeed,
} from "./feed.js";
import { routerHealthResponse } from "./http.js";
import {
  generatePollCursorKey,
  makePollCursorCodec,
  makePollWaiters,
  makeRouterPoll,
  type PollWaiters,
} from "./poll.js";

/* eslint-disable max-lines-per-function, agent-code-guard/no-hardcoded-assertion-literals -- Effect.gen scenarios keep each state transition and its observations together. */
const utf8Encoder = new TextEncoder();
const structurallyValidSignature =
  "KDxTl7gpBIOcp3KzWrPaXGOI8uSNov6xWPXKa421caAinNAc-_pYc_kzBuUde6eY0Ayp21se-jZdCvMGLlbYDg";

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

const makeDigest = (seed: number) =>
  Schema.decodeUnknownSync(routerContract.SignedMessageDigest)(
    `smd_${Encoding.encodeBase64Url(new Uint8Array(32).fill(seed))}`,
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
}): LiveSignedMessage => {
  const agentCardDigest = Schema.decodeUnknownSync(AgentIdentityCardDigest)(
    `acd_${Encoding.encodeBase64Url(new Uint8Array(32).fill(9))}`,
  );
  return Schema.decodeUnknownSync(SignedMessage.live)({
    payload: encodedCanonicalJson({
      kind: "signedMessage",
      sharnessVersion: WIRE_VERSION,
      senderAgentId: input.senderAgentId,
      agentCardDigest,
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
        signature: structurallyValidSignature,
      },
    ],
  });
};

const makeMaximumSignedMessage = (): LiveSignedMessage =>
  Schema.decodeUnknownSync(SignedMessage.live)({
    payload: encodedCanonicalJson({
      kind: "signedMessage",
      sharnessVersion: WIRE_VERSION,
      senderAgentId: makeAgentId(250),
      agentCardDigest: `acd_${Encoding.encodeBase64Url(
        new Uint8Array(32).fill(9),
      )}`,
      recipientAgentIds: Array.from(Array(128).keys(), (index) =>
        makeAgentId(index),
      ),
      envelopeId: makeEnvelopeId(248),
      body: Encoding.encodeBase64Url(new Uint8Array(262_144)),
    }),
    signatures: [
      {
        protected: encodedCanonicalJson({
          alg: "Ed25519",
          kid: "urn:ietf:params:oauth:jwk-thumbprint:sha-256:" + "A".repeat(43),
          typ: "application/vnd.sharness.signed-message+jws",
        }),
        signature: structurallyValidSignature,
      },
    ],
  });

const encodedMessage = (message: LiveSignedMessage): string =>
  canonicalText(Schema.encodeSync(SignedMessage)(message));

const acceptMessage = (
  feed: RouterFeed,
  message: LiveSignedMessage,
  mode: "initial" | "retry" = "initial",
) =>
  feed.accept({
    mode,
    signedMessage: message,
    encodedMessageJcs: encodedMessage(message),
    encodedByteLength: SignedMessage.encodedByteLength(message),
    recipients: new Set(message.recipientAgentIds),
    senderAgentId: message.senderAgentId,
    envelopeId: message.envelopeId,
    signedMessageDigest: makeDigest(message.envelopeId.charCodeAt(4)),
  });

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

const sender = makeAgentId(1);
const firstRecipient = makeAgentId(2);
const secondRecipient = makeAgentId(3);
const nonRecipient = makeAgentId(4);

describe("poll cursor behavior", () => {
  it("binds caller and instance and detects tampering", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const key = generatePollCursorKey();
        const firstInstance = makeRouterInstanceId(1);
        const secondInstance = makeRouterInstanceId(2);
        const firstCodec = makePollCursorCodec({
          key,
          routerInstanceId: firstInstance,
        });
        const secondCodec = makePollCursorCodec({
          key,
          routerInstanceId: secondInstance,
        });
        const cursor = yield* firstCodec.encrypt({
          agentId: firstRecipient,
          lastScannedOrder: 17n,
        });
        expect(yield* firstCodec.decrypt(cursor, firstRecipient)).toBe(17n);
        expect(
          yield* effectFails(firstCodec.decrypt(cursor, secondRecipient)),
        ).toBe(true);
        expect(
          yield* effectFails(secondCodec.decrypt(cursor, firstRecipient)),
        ).toBe(true);

        const tagStart = cursor.length - 22;
        const current = cursor.charAt(tagStart);
        const replacement = current === "A" ? "B" : "A";
        const tampered = Schema.decodeUnknownSync(routerContract.PollCursor)(
          `${cursor.slice(0, tagStart)}${replacement}${cursor.slice(
            tagStart + 1,
          )}`,
        );
        expect(
          yield* effectFails(firstCodec.decrypt(tampered, firstRecipient)),
        ).toBe(true);
      }),
    ));

  effectIt.effect(
    "rejects authenticated plaintext with a UTF-8 byte-order mark",
    () =>
      Effect.gen(function* () {
        const key = generatePollCursorKey();
        const routerInstanceId = makeRouterInstanceId(1);
        const codec = makePollCursorCodec({
          key,
          routerInstanceId,
        });
        const canonical = utf8Encoder.encode(
          canonicalText({
            agentId: firstRecipient,
            routerInstanceId,
            lastScannedOrder: "17",
          }),
        );
        const markedPlaintext = new Uint8Array(canonical.byteLength + 3);
        markedPlaintext.set([0xef, 0xbb, 0xbf]);
        markedPlaintext.set(canonical, 3);
        const compact = yield* Effect.tryPromise({
          try: () =>
            new CompactEncrypt(markedPlaintext)
              .setProtectedHeader({
                alg: "dir",
                enc: "A256GCM",
                typ: "application/vnd.sharness.poll-cursor+jwe",
              })
              .encrypt(key),
          catch: () => new Error("cursor fixture encryption failed"),
        });
        const cursor = Schema.decodeUnknownSync(routerContract.PollCursor)(
          `plc_${compact}`,
        );
        expect(yield* effectFails(codec.decrypt(cursor, firstRecipient))).toBe(
          true,
        );
      }),
  );

  effectIt.effect(
    "uses fresh IVs and stays within the maximum representation",
    () =>
      Effect.gen(function* () {
        const routerInstanceId = makeRouterInstanceId(1);
        const key = generatePollCursorKey();
        const codec = makePollCursorCodec({
          key,
          routerInstanceId,
        });
        const first = yield* codec.encrypt({
          agentId: firstRecipient,
          lastScannedOrder: maximumPrivateOrder,
        });
        const second = yield* codec.encrypt({
          agentId: firstRecipient,
          lastScannedOrder: maximumPrivateOrder,
        });
        expect(first).not.toBe(second);
        expect(first.length).toBe(routerContract.maximumPollCursorLength);
        expect(first.length).toBe(
          routerContract.pollCursorEncodedLength({
            agentId: firstRecipient,
            routerInstanceId,
            lastScannedOrder: maximumPrivateOrder,
          }),
        );
        expect(
          utf8Encoder.encode(
            canonicalText({
              callerAgentId: firstRecipient,
              request: { pollCursor: first },
            }),
          ).byteLength,
        ).toBe(routerContract.routerRepresentationLimits.pollRequestBodyBytes);

        const overRangeCompact = yield* Effect.tryPromise({
          try: () =>
            new CompactEncrypt(
              utf8Encoder.encode(
                canonicalText({
                  agentId: firstRecipient,
                  routerInstanceId,
                  lastScannedOrder: (maximumPrivateOrder + 1n).toString(10),
                }),
              ),
            )
              .setProtectedHeader({
                alg: "dir",
                enc: "A256GCM",
                typ: "application/vnd.sharness.poll-cursor+jwe",
              })
              .encrypt(key),
          catch: () => new Error("cursor fixture encryption failed"),
        });
        const overRangeCursor = Schema.decodeUnknownSync(
          routerContract.PollCursor,
        )(`plc_${overRangeCompact}`);
        expect(
          yield* effectFails(codec.decrypt(overRangeCursor, firstRecipient)),
        ).toBe(true);
      }),
  );
});

describe("global feed behavior", () => {
  effectIt.effect("keeps one ordered copy for every recipient", () =>
    Effect.gen(function* () {
      const feed = yield* makeRouterFeed({
        routerInstanceId: makeRouterInstanceId(1),
        retainedMessageCapacity: 4,
        retainedMessageByteCapacity: 2_000_000,
      });
      const message = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient, secondRecipient],
        envelopeId: makeEnvelopeId(1),
        body: "one-copy",
      });
      yield* acceptMessage(feed, message);
      const snapshot = yield* feed.snapshot;
      expect(snapshot.entries).toHaveLength(1);
      expect(snapshot.entries[0]?.order).toBe(1n);
      expect(snapshot.entries[0]?.recipients).toEqual(
        new Set([firstRecipient, secondRecipient]),
      );
      expect(entriesAfter(snapshot, 0n)).toEqual(snapshot.entries);
    }),
  );

  effectIt.effect("couples eviction to retry identity", () =>
    Effect.gen(function* () {
      const routerInstanceId = makeRouterInstanceId(1);
      const feed = yield* makeRouterFeed({
        routerInstanceId,
        retainedMessageCapacity: 2,
        retainedMessageByteCapacity: 2_000_000,
      });
      const firstMessage = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient],
        envelopeId: makeEnvelopeId(1),
        body: "message-1",
      });
      const secondMessage = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient],
        envelopeId: makeEnvelopeId(2),
        body: "message-2",
      });
      const thirdMessage = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient],
        envelopeId: makeEnvelopeId(3),
        body: "message-3",
      });
      yield* acceptMessage(feed, firstMessage);
      const secondAccepted = yield* acceptMessage(feed, secondMessage);
      yield* acceptMessage(feed, thirdMessage);
      const snapshot = yield* feed.snapshot;

      expect(snapshot.entries.map((entry) => entry.order)).toEqual([2n, 3n]);
      expect(snapshot.greatestEvictedOrder).toBe(1n);
      expect(yield* acceptMessage(feed, firstMessage, "retry")).toEqual({
        kind: "result",
        result: { kind: "retry_identity_unknown" },
      });
      expect(secondAccepted).toMatchObject({
        kind: "result",
        result: { kind: "accepted" },
      });
      if (secondAccepted.kind !== "result") {
        return;
      }
      expect(yield* acceptMessage(feed, secondMessage, "retry")).toEqual({
        kind: "result",
        result: secondAccepted.result,
      });
      expect(yield* acceptMessage(feed, secondMessage)).toEqual({
        kind: "result",
        result: { kind: "idempotency_conflict" },
      });
    }),
  );

  effectIt.effect("appends an initial under an evicted identity again", () =>
    Effect.gen(function* () {
      const feed = yield* makeRouterFeed({
        routerInstanceId: makeRouterInstanceId(1),
        retainedMessageCapacity: 1,
        retainedMessageByteCapacity: 2_000_000,
      });
      const resent = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient],
        envelopeId: makeEnvelopeId(1),
        body: "resent",
      });
      const evictor = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient],
        envelopeId: makeEnvelopeId(2),
        body: "evictor",
      });
      yield* acceptMessage(feed, resent);
      yield* acceptMessage(feed, evictor);

      expect(yield* acceptMessage(feed, resent)).toMatchObject({
        kind: "result",
        result: { kind: "accepted" },
      });
      const snapshot = yield* feed.snapshot;
      expect(
        snapshot.entries.map((entry) => [entry.order, entry.envelopeId]),
      ).toEqual([[3n, resent.envelopeId]]);
    }),
  );

  effectIt.effect("rejects changed bytes for a retained retry identity", () =>
    Effect.gen(function* () {
      const feed = yield* makeRouterFeed({
        routerInstanceId: makeRouterInstanceId(1),
        retainedMessageCapacity: 2,
        retainedMessageByteCapacity: 2_000_000,
      });
      const envelopeId = makeEnvelopeId(8);
      const original = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient],
        envelopeId,
        body: "original",
      });
      const changed = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient],
        envelopeId,
        body: "changed",
      });
      yield* acceptMessage(feed, original);
      expect(yield* acceptMessage(feed, changed, "retry")).toEqual({
        kind: "result",
        result: { kind: "idempotency_conflict" },
      });
    }),
  );

  it("serializes concurrent acceptance into unique positions", () =>
    fc.assert(
      fc.asyncProperty(
        fc.uniqueArray(fc.integer({ min: 1, max: 240 }), {
          minLength: 2,
          maxLength: 32,
        }),
        (seeds) =>
          Effect.runPromise(
            Effect.gen(function* () {
              const feed = yield* makeRouterFeed({
                routerInstanceId: makeRouterInstanceId(1),
                retainedMessageCapacity: seeds.length,
                retainedMessageByteCapacity: 2_000_000,
              });
              const messages = seeds.map((seed) =>
                makeSignedMessage({
                  senderAgentId: sender,
                  recipientAgentIds: [firstRecipient, secondRecipient],
                  envelopeId: makeEnvelopeId(seed),
                  body: `concurrent-${seed}`,
                }),
              );
              const outcomes = yield* Effect.all(
                messages.map((message) => acceptMessage(feed, message)),
                { concurrency: seeds.length },
              );
              expect(
                outcomes.every(
                  (outcome) =>
                    outcome.kind === "result" &&
                    outcome.result.kind === "accepted",
                ),
              ).toBe(true);
              const snapshot = yield* feed.snapshot;
              const orders = snapshot.entries.map((entry) => entry.order);
              expect(new Set(orders).size).toBe(messages.length);
              expect(orders).toEqual(
                Array.from(Array(messages.length).keys(), (index) =>
                  BigInt(index + 1),
                ),
              );
            }),
          ),
      ),
      { numRuns: 25 },
    ));

  effectIt.effect(
    "keeps retries and polls usable after the final position",
    () =>
      Effect.gen(function* () {
        const routerInstanceId = makeRouterInstanceId(1);
        const feedInput = {
          routerInstanceId,
          retainedMessageCapacity: 2,
          retainedMessageByteCapacity: 2_000_000,
        };
        expect(
          yield* effectFails(
            makeRouterFeedAtOrder({
              ...feedInput,
              initialTailOrder: -1n,
            }),
          ),
        ).toBe(true);
        expect(
          yield* effectFails(
            makeRouterFeedAtOrder({
              ...feedInput,
              initialTailOrder: maximumPrivateOrder + 1n,
            }),
          ),
        ).toBe(true);
        const feed = yield* makeRouterFeedAtOrder({
          ...feedInput,
          initialTailOrder: maximumPrivateOrder - 1n,
        });
        const cursorCodec = makePollCursorCodec({
          key: generatePollCursorKey(),
          routerInstanceId,
        });
        const poll = makeRouterPoll({
          routerInstanceId,
          feed,
          pollWaiters: yield* makePollWaiters(1),
          cursorCodec,
          pollMessageLimit: 2,
          pollResponseByteLimit: 1_048_576,
        });
        const beforeFinalPosition = yield* cursorCodec.encrypt({
          agentId: firstRecipient,
          lastScannedOrder: maximumPrivateOrder - 1n,
        });
        const finalMessage = makeSignedMessage({
          senderAgentId: sender,
          recipientAgentIds: [firstRecipient],
          envelopeId: makeEnvelopeId(1),
          body: "final-position",
        });
        const accepted = yield* acceptMessage(feed, finalMessage);
        expect((yield* feed.snapshot).tailOrder).toBe(maximumPrivateOrder);
        expect(yield* feed.freshAppendReady).toBe(false);
        const healthResponse = yield* routerHealthResponse(feed);
        expect(healthResponse.status).toBe(503);
        expect(healthResponse.body._tag).toBe("Empty");
        if (accepted.kind !== "result") {
          return;
        }
        expect(yield* acceptMessage(feed, finalMessage, "retry")).toEqual({
          kind: "result",
          result: accepted.result,
        });

        const refused = yield* acceptMessage(
          feed,
          makeSignedMessage({
            senderAgentId: sender,
            recipientAgentIds: [firstRecipient],
            envelopeId: makeEnvelopeId(2),
            body: "past-final-position",
          }),
        );
        expect(refused).toEqual({ kind: "overloaded" });
        expect((yield* feed.snapshot).tailOrder).toBe(maximumPrivateOrder);

        const continued = yield* poll(
          { pollCursor: beforeFinalPosition },
          { callerAgentId: firstRecipient },
        );
        expect(continued.kind).toBe("batch");
        if (continued.kind === "batch") {
          expect(continued.signedMessages).toEqual([finalMessage]);
        }
        const anchor = yield* poll({}, { callerAgentId: firstRecipient });
        expect(anchor.kind).toBe("batch");
        if (anchor.kind === "batch") {
          expect(anchor.signedMessages).toEqual([]);
          expect(
            yield* cursorCodec.decrypt(anchor.pollCursor, firstRecipient),
          ).toBe(maximumPrivateOrder);
        }
      }),
  );
});

describe("representation bounds", () => {
  effectIt.effect("matches actual Schema, JCS, and JWE encodings", () =>
    Effect.gen(function* () {
      const routerInstanceId = makeRouterInstanceId(255);
      const maximumMessage = makeMaximumSignedMessage();
      const cursor = yield* makePollCursorCodec({
        key: generatePollCursorKey(),
        routerInstanceId,
      }).encrypt({
        agentId: makeAgentId(255),
        lastScannedOrder: maximumPrivateOrder,
      });
      const sendRequest = yield* Schema.encode(
        routerContract.RouterSendRequest,
      )({
        expectedRouterInstanceId: routerInstanceId,
        mode: "initial",
        signedMessage: maximumMessage,
      });
      const pollResult = yield* Schema.encode(routerContract.RouterPollResult)({
        kind: "batch",
        routerInstanceId,
        signedMessages: [maximumMessage],
        pollCursor: cursor,
      });

      expect(SignedMessage.encodedByteLength(maximumMessage)).toBe(
        SignedMessage.maximumEncodedByteLength,
      );
      expect(
        utf8Encoder.encode(
          canonicalText({
            callerAgentId: makeAgentId(255),
            request: sendRequest,
          }),
        ).byteLength,
      ).toBe(routerContract.routerRepresentationLimits.sendRequestBodyBytes);
      expect(cursor.length).toBe(routerContract.maximumPollCursorLength);
      expect(
        utf8Encoder.encode(
          canonicalText({
            callerAgentId: makeAgentId(255),
            request: { pollCursor: cursor },
          }),
        ).byteLength,
      ).toBe(routerContract.routerRepresentationLimits.pollRequestBodyBytes);
      expect(utf8Encoder.encode(canonicalText(pollResult)).byteLength).toBe(
        routerContract.routerRepresentationLimits.oneMessageBatchBytes,
      );
    }),
  );
});

// @agent-code-guard/regression-only: each scenario fixes a distinct cursor progression boundary over deterministic feed state
describe("poll behavior", () => {
  effectIt.effect(
    "anchors at the current tail and returns only later messages",
    () =>
      Effect.gen(function* () {
        const routerInstanceId = makeRouterInstanceId(1);
        const feed = yield* makeRouterFeed({
          routerInstanceId,
          retainedMessageCapacity: 8,
          retainedMessageByteCapacity: 2_000_000,
        });
        const pollWaiters = yield* makePollWaiters(4);
        const cursorCodec = makePollCursorCodec({
          key: generatePollCursorKey(),
          routerInstanceId,
        });
        const poll = makeRouterPoll({
          routerInstanceId,
          feed,
          pollWaiters,
          cursorCodec,
          pollMessageLimit: 8,
          pollResponseByteLimit: 1_048_576,
        });
        const historical = makeSignedMessage({
          senderAgentId: sender,
          recipientAgentIds: [firstRecipient],
          envelopeId: makeEnvelopeId(1),
          body: "historical",
        });
        yield* acceptMessage(feed, historical);
        const anchor = yield* poll({}, { callerAgentId: firstRecipient });
        expect(anchor.kind).toBe("batch");
        if (anchor.kind !== "batch") {
          return;
        }
        expect(anchor.signedMessages).toEqual([]);
        expect(
          yield* cursorCodec.decrypt(anchor.pollCursor, firstRecipient),
        ).toBe(1n);

        const live = makeSignedMessage({
          senderAgentId: sender,
          recipientAgentIds: [firstRecipient],
          envelopeId: makeEnvelopeId(2),
          body: "live",
        });
        yield* acceptMessage(feed, live);
        const delivered = yield* poll(
          { pollCursor: anchor.pollCursor },
          { callerAgentId: firstRecipient },
        );
        expect(delivered.kind).toBe("batch");
        if (delivered.kind === "batch") {
          expect(delivered.signedMessages).toEqual([live]);
        }
      }),
  );

  effectIt.effect("reports a gap and rejects a future private order", () =>
    Effect.gen(function* () {
      const routerInstanceId = makeRouterInstanceId(1);
      const feed = yield* makeRouterFeed({
        routerInstanceId,
        retainedMessageCapacity: 1,
        retainedMessageByteCapacity: 2_000_000,
      });
      const pollWaiters = yield* makePollWaiters(4);
      const cursorCodec = makePollCursorCodec({
        key: generatePollCursorKey(),
        routerInstanceId,
      });
      const poll = makeRouterPoll({
        routerInstanceId,
        feed,
        pollWaiters,
        cursorCodec,
        pollMessageLimit: 8,
        pollResponseByteLimit: 1_048_576,
      });
      const beforeFeed = yield* cursorCodec.encrypt({
        agentId: firstRecipient,
        lastScannedOrder: 0n,
      });
      for (const seed of [1, 2]) {
        yield* acceptMessage(
          feed,
          makeSignedMessage({
            senderAgentId: sender,
            recipientAgentIds: [firstRecipient],
            envelopeId: makeEnvelopeId(seed),
            body: `message-${seed}`,
          }),
        );
      }
      expect(
        yield* poll(
          { pollCursor: beforeFeed },
          { callerAgentId: firstRecipient },
        ),
      ).toEqual({ kind: "feed_gap", routerInstanceId });

      const future = yield* cursorCodec.encrypt({
        agentId: firstRecipient,
        lastScannedOrder: 3n,
      });
      expect(
        yield* poll({ pollCursor: future }, { callerAgentId: firstRecipient }),
      ).toEqual({ kind: "cursor_invalid" });
    }),
  );

  effectIt.effect("keeps the first scan frontier across a held rescan", () =>
    Effect.gen(function* () {
      const routerInstanceId = makeRouterInstanceId(1);
      const feed = yield* makeRouterFeed({
        routerInstanceId,
        retainedMessageCapacity: 1,
        retainedMessageByteCapacity: 2_000_000,
      });
      const cursorCodec = makePollCursorCodec({
        key: generatePollCursorKey(),
        routerInstanceId,
      });
      let holdCount = 0;
      const pollWaiters: PollWaiters = {
        awaitSignal: () =>
          Effect.gen(function* () {
            holdCount += 1;
            yield* acceptMessage(
              feed,
              makeSignedMessage({
                senderAgentId: sender,
                recipientAgentIds: [nonRecipient],
                envelopeId: makeEnvelopeId(2),
                body: "second-unrelated",
              }),
            );
          }),
        notify: () => Effect.void,
        activeCount: Effect.succeed(0),
      };
      const poll = makeRouterPoll({
        routerInstanceId,
        feed,
        pollWaiters,
        cursorCodec,
        pollMessageLimit: 8,
        pollResponseByteLimit: 1_048_576,
      });
      const beforeFeed = yield* cursorCodec.encrypt({
        agentId: firstRecipient,
        lastScannedOrder: 0n,
      });
      yield* acceptMessage(
        feed,
        makeSignedMessage({
          senderAgentId: sender,
          recipientAgentIds: [nonRecipient],
          envelopeId: makeEnvelopeId(1),
          body: "first-unrelated",
        }),
      );
      const advanced = yield* poll(
        { pollCursor: beforeFeed },
        { callerAgentId: firstRecipient },
      );
      expect(holdCount).toBe(1);
      expect(advanced.kind).toBe("batch");
      if (advanced.kind === "batch") {
        expect(advanced.signedMessages).toEqual([]);
        expect(
          yield* cursorCodec.decrypt(advanced.pollCursor, firstRecipient),
        ).toBe(2n);
      }
      expect(
        yield* poll(
          { pollCursor: beforeFeed },
          { callerAgentId: firstRecipient },
        ),
      ).toEqual({ kind: "feed_gap", routerInstanceId });
    }),
  );

  effectIt.effect(
    "returns a bounded prefix without skipping addressed data",
    () =>
      Effect.gen(function* () {
        const routerInstanceId = makeRouterInstanceId(1);
        const feed = yield* makeRouterFeed({
          routerInstanceId,
          retainedMessageCapacity: 8,
          retainedMessageByteCapacity: 2_000_000,
        });
        const cursorCodec = makePollCursorCodec({
          key: generatePollCursorKey(),
          routerInstanceId,
        });
        const poll = makeRouterPoll({
          routerInstanceId,
          feed,
          pollWaiters: yield* makePollWaiters(4),
          cursorCodec,
          pollMessageLimit: 1,
          pollResponseByteLimit: 1_048_576,
        });
        const anchor = yield* cursorCodec.encrypt({
          agentId: firstRecipient,
          lastScannedOrder: 0n,
        });
        const first = makeSignedMessage({
          senderAgentId: sender,
          recipientAgentIds: [firstRecipient],
          envelopeId: makeEnvelopeId(1),
          body: "first",
        });
        const second = makeSignedMessage({
          senderAgentId: sender,
          recipientAgentIds: [firstRecipient],
          envelopeId: makeEnvelopeId(2),
          body: "second",
        });
        yield* acceptMessage(feed, first);
        yield* acceptMessage(feed, second);

        const firstBatch = yield* poll(
          { pollCursor: anchor },
          { callerAgentId: firstRecipient },
        );
        expect(firstBatch.kind).toBe("batch");
        if (firstBatch.kind !== "batch") {
          return;
        }
        expect(firstBatch.signedMessages).toEqual([first]);
        const secondBatch = yield* poll(
          { pollCursor: firstBatch.pollCursor },
          { callerAgentId: firstRecipient },
        );
        expect(secondBatch.kind).toBe("batch");
        if (secondBatch.kind === "batch") {
          expect(secondBatch.signedMessages).toEqual([second]);
        }
      }),
  );

  effectIt.effect("advances past unrelated entries before addressed data", () =>
    Effect.gen(function* () {
      const routerInstanceId = makeRouterInstanceId(1);
      const feed = yield* makeRouterFeed({
        routerInstanceId,
        retainedMessageCapacity: 8,
        retainedMessageByteCapacity: 2_000_000,
      });
      const cursorCodec = makePollCursorCodec({
        key: generatePollCursorKey(),
        routerInstanceId,
      });
      const poll = makeRouterPoll({
        routerInstanceId,
        feed,
        pollWaiters: yield* makePollWaiters(4),
        cursorCodec,
        pollMessageLimit: 8,
        pollResponseByteLimit: 1_048_576,
      });
      const anchor = yield* cursorCodec.encrypt({
        agentId: firstRecipient,
        lastScannedOrder: 0n,
      });
      const unrelated = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [nonRecipient],
        envelopeId: makeEnvelopeId(1),
        body: "unrelated",
      });
      const addressed = makeSignedMessage({
        senderAgentId: sender,
        recipientAgentIds: [firstRecipient],
        envelopeId: makeEnvelopeId(2),
        body: "addressed",
      });
      yield* acceptMessage(feed, unrelated);
      yield* acceptMessage(feed, addressed);
      const batch = yield* poll(
        { pollCursor: anchor },
        { callerAgentId: firstRecipient },
      );
      expect(batch.kind).toBe("batch");
      if (batch.kind === "batch") {
        expect(batch.signedMessages).toEqual([addressed]);
        expect(
          yield* cursorCodec.decrypt(batch.pollCursor, firstRecipient),
        ).toBe(2n);
      }
    }),
  );
});

describe("held poll behavior", () => {
  effectIt.effect(
    "enforces one waiter per agent and cleans up interruption",
    () =>
      Effect.gen(function* () {
        const pollWaiters = yield* makePollWaiters(2);
        const waiter = yield* Effect.fork(
          pollWaiters.awaitSignal(firstRecipient, Effect.succeed(false)),
        );
        yield* Effect.yieldNow();
        expect(yield* pollWaiters.activeCount).toBe(1);
        expect(
          yield* effectFails(
            pollWaiters.awaitSignal(firstRecipient, Effect.succeed(false)),
          ),
        ).toBe(true);
        yield* Fiber.interrupt(waiter);
        expect(yield* pollWaiters.activeCount).toBe(0);
      }),
  );

  effectIt.effect("wakes only addressed waiters and releases their slots", () =>
    Effect.gen(function* () {
      const pollWaiters = yield* makePollWaiters(2);
      const first = yield* Effect.fork(
        pollWaiters.awaitSignal(firstRecipient, Effect.succeed(false)),
      );
      const second = yield* Effect.fork(
        pollWaiters.awaitSignal(secondRecipient, Effect.succeed(false)),
      );
      yield* Effect.yieldNow();
      expect(yield* pollWaiters.activeCount).toBe(2);
      yield* pollWaiters.notify(new Set([firstRecipient]));
      yield* Fiber.join(first);
      expect(yield* pollWaiters.activeCount).toBe(1);
      yield* Fiber.interrupt(second);
      expect(yield* pollWaiters.activeCount).toBe(0);
    }),
  );
});

/* eslint-enable max-lines-per-function, agent-code-guard/no-hardcoded-assertion-literals -- Restore production defaults after the scenario suite. */
