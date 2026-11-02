/** @file Deterministic Identity and Router fixtures for Router worker tests. */

import { type Context, Effect, Encoding, Layer, Ref, Schema } from "effect";
import { generateKeyPairSync } from "node:crypto";
import type {
  AgentId as AgentIdValue,
  VerifiedAgentIdentityCard,
} from "../identity/agent.js";
import type { AgentSigningAuthority as AgentSigningAuthorityValue } from "../identity/keys.js";
import type { RegistryLookupResult } from "../identity/registry/contract.js";
import {
  PollCursor,
  Router,
  type RouterConnectionError,
  RouterInstanceId,
  type RouterPollResult,
  type RouterSendRequest,
  type RouterSendResult,
} from "../communication/router/api.js";
import {
  EnvelopeId,
  type LiveSignedMessage,
  SignedMessage,
} from "../identity/envelope/signed-message.js";
import { Registry } from "../identity/registry/index.js";
import {
  identifier,
  issueTestCard,
  makeTestAuthority,
} from "./agent-card-fixtures.js";

/** Payload decoded by the scripted worker callback. */
export interface TestPayload {
  readonly text: string;
}

interface SendCall {
  readonly request: RouterSendRequest;
}

/** A scripted send answer computed from the request it receives. */
export type ScriptedSendAnswer = (
  request: RouterSendRequest,
) => Effect.Effect<RouterSendResult, RouterConnectionError>;

type ScriptedSend = RouterSendResult | ScriptedSendAnswer;

interface ScriptedRouter {
  readonly polls: Ref.Ref<RouterPollResult[]>;
  readonly sends: Ref.Ref<ScriptedSend[]>;
  /** Reject supplied cursors until an omitted-cursor poll, after invalidation or restart. */
  readonly cursorInvalid: Ref.Ref<boolean>;
  readonly sendCalls: Ref.Ref<SendCall[]>;
  readonly fallbackPoll?: Effect.Effect<RouterPollResult>;
}

interface SignMessageInput {
  readonly card: VerifiedAgentIdentityCard;
  readonly authority: AgentSigningAuthorityValue;
  readonly recipient: AgentIdValue;
  readonly id: number;
  readonly body: string;
}

interface ScriptedRouterInput {
  readonly polls: readonly RouterPollResult[];
  readonly sends?: readonly ScriptedSend[];
  readonly fallbackPoll?: Effect.Effect<RouterPollResult>;
}

/** Identity material used by one Router worker test endpoint. */
export interface Fixture {
  readonly localCard: VerifiedAgentIdentityCard;
  readonly localAuthority: AgentSigningAuthorityValue;
}

/**
 * Construct a deterministic test EnvelopeId.
 * @param byte Repeated identifier byte.
 * @returns Decoded EnvelopeId.
 */
const envelopeId = (byte: number) =>
  Schema.decodeUnknownSync(EnvelopeId)(identifier("env_", byte));

/**
 * Construct a deterministic test RouterInstanceId.
 * @param byte Repeated identifier byte.
 * @returns Decoded RouterInstanceId.
 */
export const routerInstanceId = (byte: number) =>
  Schema.decodeUnknownSync(RouterInstanceId)(identifier("rti_", byte));

/**
 * Construct a deterministic authenticated test cursor.
 * @param byte Repeated cursor byte.
 * @returns Decoded PollCursor.
 */
export const pollCursor = (byte: number) =>
  Schema.decodeUnknownSync(PollCursor)(
    `plc_eyJhbGciOiJkaXIiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiYXBwbGljYXRpb24vdm5kLnNoYXJuZXNzLnBvbGwtY3Vyc29yK2p3ZSJ9..${Encoding.encodeBase64Url(new Uint8Array(12).fill(byte))}.${Encoding.encodeBase64Url(new Uint8Array(120).fill(byte))}.${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`,
  );

/**
 * Build verified agent material with deterministic protocol identifiers.
 * @param byte Repeated AgentId and PrincipalId byte.
 * @param name Canonical agent name.
 * @returns Verified card and its signing authority.
 */
export const makeIdentityFixture = (byte: number, name: string) =>
  Effect.gen(function* () {
    const localAuthority = yield* makeTestAuthority();
    const localCard = yield* issueTestCard({
      byte,
      name,
      authority: localAuthority,
      registryKeys: generateKeyPairSync("ed25519"),
    });
    return {
      localCard,
      localAuthority,
    } satisfies Fixture;
  }).pipe(Effect.withSpan("makeIdentityFixture"));

/** Default local identity fixture shared by Router worker scenarios. */
export const makeFixture = makeIdentityFixture(1, "worker-local");

/**
 * Sign one deterministic Router worker test envelope.
 * @param input Identity, recipient, identifier byte, and body.
 * @returns Signed test envelope.
 */
export const signMessage = (
  input: SignMessageInput,
): Effect.Effect<LiveSignedMessage> =>
  SignedMessage.sign({
    identityCard: input.card,
    signingAuthority: input.authority,
    recipientAgentIds: new Set([input.recipient]),
    envelopeId: envelopeId(input.id),
    body: new TextEncoder().encode(input.body),
  }).pipe(Effect.orDie, Effect.withSpan("signMessage"));

/** The General JWS fields of an encoded SignedMessage. */
const envelopeRepresentation = Schema.Struct({
  payload: Schema.String,
  signatures: Schema.Tuple(
    Schema.Struct({ protected: Schema.String, signature: Schema.String }),
  ),
});

/**
 * Flip one bit of a message's Ed25519 signature. Its sender, card digest, and
 * key id still match the sender's card, so only the cryptographic check
 * rejects the result.
 * @param message A validly signed message.
 * @returns The same message under a signature its sender never made.
 */
export const corruptSignature = (
  message: SignedMessage,
): Effect.Effect<SignedMessage> =>
  Effect.gen(function* () {
    const representation = yield* Schema.encode(SignedMessage)(message).pipe(
      Effect.flatMap(Schema.decodeUnknown(envelopeRepresentation)),
    );
    const [signature] = representation.signatures;
    const bytes = yield* Encoding.decodeBase64Url(signature.signature);
    const corrupted = bytes.map((value, index) =>
      index === 0 ? value ^ 1 : value,
    );
    return yield* Schema.decodeUnknown(SignedMessage)({
      ...representation,
      signatures: [
        { ...signature, signature: Encoding.encodeBase64Url(corrupted) },
      ],
    });
  }).pipe(Effect.orDie, Effect.withSpan("corruptSignature"));

/**
 * Build a Router layer that consumes explicit poll and send results in order.
 * A cursor_invalid poll or router_restarted send makes cursor rejection sticky
 * until an omitted-cursor poll reattaches to the Router's current instance.
 * @param input Ordered poll and send results plus an optional poll fallback.
 * @returns Mutable script evidence and its Router layer.
 */
export const makeScriptedRouter = (input: ScriptedRouterInput) =>
  Effect.gen(function* () {
    const scripted: ScriptedRouter = {
      polls: yield* Ref.make([...input.polls]),
      sends: yield* Ref.make([...(input.sends ?? [])]),
      cursorInvalid: yield* Ref.make(false),
      sendCalls: yield* Ref.make<SendCall[]>([]),
      fallbackPoll: input.fallbackPoll,
    };
    return {
      scripted,
      layer: Layer.succeed(Router, scriptedRouterService(scripted)),
    };
  }).pipe(Effect.withSpan("makeScriptedRouter"));

/**
 * Resolve only the verified cards named by a test.
 * @param cards Cards available to Registry lookup.
 * @returns Registry test layer.
 */
export const registryLayer = (cards: readonly VerifiedAgentIdentityCard[]) =>
  Layer.succeed(Registry, {
    lookup: (request): Effect.Effect<RegistryLookupResult> => {
      const found = cards.find((card) =>
        "agentId" in request
          ? card.agentId === request.agentId
          : card.agentName === request.agentName,
      );
      return Effect.succeed(
        found === undefined
          ? { kind: "not_found" as const }
          : { kind: "found" as const, identityCard: found },
      );
    },
    register: () => Effect.succeed({ kind: "idempotency_conflict" }),
  });

/** Registry layer that proves pinned cards make live lookup unnecessary. */
export const unavailableRegistryLayer = Layer.succeed(Registry, {
  lookup: () => Effect.die("Registry must not be queried for a pinned sender"),
  register: () => Effect.die("Registry unavailable"),
});

/**
 * Build an empty Router poll batch at a deterministic cursor.
 * @param instance Router instance for the batch.
 * @param cursor Cursor committed by the batch.
 * @returns Empty poll result.
 */
export const emptyBatch = (
  instance: ReturnType<typeof routerInstanceId>,
  cursor: ReturnType<typeof pollCursor>,
): RouterPollResult => ({
  kind: "batch",
  routerInstanceId: instance,
  signedMessages: [],
  pollCursor: cursor,
});

/**
 * Build a Router poll batch carrying the supplied envelopes.
 * @param instance Router instance for the batch.
 * @param cursor Cursor committed by the batch.
 * @param signedMessages Ordered envelopes in the batch.
 * @returns Populated poll result.
 */
export const batch = (
  instance: ReturnType<typeof routerInstanceId>,
  cursor: ReturnType<typeof pollCursor>,
  signedMessages: readonly LiveSignedMessage[],
): RouterPollResult => ({
  kind: "batch",
  routerInstanceId: instance,
  signedMessages,
  pollCursor: cursor,
});

function scriptedRouterService(
  scripted: ScriptedRouter,
): Context.Tag.Service<typeof Router> {
  return {
    poll: scriptedPoll(scripted),
    send: scriptedSend(scripted),
  };
}

function scriptedPoll(
  scripted: ScriptedRouter,
): Context.Tag.Service<typeof Router>["poll"] {
  return (call) =>
    Effect.gen(function* () {
      if (
        call.request.pollCursor !== undefined &&
        (yield* Ref.get(scripted.cursorInvalid))
      ) {
        return { kind: "cursor_invalid" };
      }
      yield* Ref.set(scripted.cursorInvalid, false);
      const result = yield* Ref.modify(scripted.polls, (results) => {
        const [head, ...tail] = results;
        return [head, tail] as const;
      });
      yield* Ref.set(scripted.cursorInvalid, result?.kind === "cursor_invalid");
      return (
        result ??
        (scripted.fallbackPoll === undefined
          ? yield* Effect.die("poll script exhausted")
          : yield* scripted.fallbackPoll)
      );
    });
}

function scriptedSend(
  scripted: ScriptedRouter,
): Context.Tag.Service<typeof Router>["send"] {
  return (call) =>
    Effect.gen(function* () {
      yield* Ref.update(scripted.sendCalls, (calls) => [
        ...calls,
        { request: call.request },
      ]);
      const step = yield* Ref.modify(scripted.sends, (steps) => {
        const [head, ...tail] = steps;
        return [head, tail] as const;
      });
      if (step === undefined) {
        return yield* Effect.die("send script exhausted");
      }
      const result = yield* typeof step === "function"
        ? step(call.request)
        : Effect.succeed(step);
      if (result.kind === "router_restarted") {
        yield* Ref.set(scripted.cursorInvalid, true);
      }
      return result;
    });
}
