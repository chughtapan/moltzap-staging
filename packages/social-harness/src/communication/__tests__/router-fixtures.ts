/** @file Router batches and invalid signatures for communication tests. */

import { Effect, Encoding, Schema } from "effect";
import { identifier } from "../../__tests__/agents.js";
import {
  type LiveSignedMessage,
  SignedMessage,
} from "../../identity/message-security.js";
import {
  PollCursor,
  RouterInstanceId,
  type RouterPollResult,
} from "../router/client.js";

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
