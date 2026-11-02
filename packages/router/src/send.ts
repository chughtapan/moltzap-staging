/** @file Verified opaque-message acceptance into the volatile feed. */
import {
  type LiveSignedMessage,
  SignedMessage,
  type VerifiedSignedMessage,
} from "@moltzap/social-harness";
import {
  httpErrors,
  routerContract,
  type VerifiedAgentRequest,
} from "@moltzap/social-harness/internal";
import canonicalize from "canonicalize";
import { Effect, Encoding, Schema } from "effect";
import type { RouterFeed } from "./feed.js";
import type { PollWaiters } from "./poll.js";

/* eslint-disable @typescript-eslint/naming-convention -- Closed HTTP error constructors retain their class names when bound from the service capability. */
const { InternalServerError, OverloadedError } = httpErrors;
/* eslint-enable @typescript-eslint/naming-convention -- Restore variable naming after binding error constructors. */

/** Accepts a message only when its signature belongs to the authenticated caller. */
export type RouterSend = (
  request: typeof routerContract.RawRouterSendRequest.Type,
  verifiedRequest: VerifiedAgentRequest,
) => Effect.Effect<
  typeof routerContract.RouterSendResult.Type,
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.InternalServerError>
>;

interface SendDependencies {
  readonly routerInstanceId: typeof routerContract.RouterInstanceId.Type;
  readonly feed: RouterFeed;
  readonly pollWaiters: PollWaiters;
}

/** Retains verified messages and wakes polls waiting for their recipients. */
export const makeRouterSend =
  (input: SendDependencies): RouterSend =>
  (request, verifiedRequest) =>
    handleSend(input, request, verifiedRequest);

const messageInvalid: typeof routerContract.RouterSendResult.Type = {
  kind: "message_invalid",
};
const utf8Encoder = new TextEncoder();

const encodedLengthIsValid = (
  message: LiveSignedMessage & VerifiedSignedMessage,
  encodedMessageJcs: string,
): boolean => {
  const encodedByteLength = SignedMessage.encodedByteLength(message);
  return (
    encodedByteLength === utf8Encoder.encode(encodedMessageJcs).byteLength &&
    encodedByteLength <= SignedMessage.maximumEncodedByteLength
  );
};

const retainMessage = (
  input: SendDependencies,
  mode: (typeof routerContract.RawRouterSendRequest.Type)["mode"],
  message: LiveSignedMessage & VerifiedSignedMessage,
): Effect.Effect<
  typeof routerContract.RouterSendResult.Type,
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.InternalServerError>
> =>
  Effect.gen(function* () {
    const encodedMessageJcs = yield* encodeMessage(message);
    if (!encodedLengthIsValid(message, encodedMessageJcs)) {
      return messageInvalid;
    }
    const signedMessageDigest = yield* digestMessage(encodedMessageJcs);
    const recipients = new Set(message.recipientAgentIds);
    const accepted = yield* input.feed.accept({
      mode,
      signedMessage: message,
      encodedMessageJcs,
      encodedByteLength: SignedMessage.encodedByteLength(message),
      recipients,
      senderAgentId: message.senderAgentId,
      envelopeId: message.envelopeId,
      signedMessageDigest,
    });
    if (accepted.kind === "overloaded") {
      return yield* Effect.fail(new OverloadedError());
    }
    if (accepted.acceptedRecipients !== undefined) {
      yield* input.pollWaiters.notify(accepted.acceptedRecipients);
    }
    return accepted.result;
  });

const handleDecodedMessage = (
  input: SendDependencies,
  request: typeof routerContract.RawRouterSendRequest.Type,
  verifiedRequest: VerifiedAgentRequest,
  signedMessage: LiveSignedMessage,
): Effect.Effect<
  typeof routerContract.RouterSendResult.Type,
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.InternalServerError>
> => {
  if (signedMessage.senderAgentId !== verifiedRequest.callerAgentId) {
    return Effect.succeed(messageInvalid);
  }
  return SignedMessage.verify({
    signedMessage,
    identityCard: verifiedRequest.identityCard,
  }).pipe(
    Effect.matchEffect({
      onFailure: () => Effect.succeed(messageInvalid),
      onSuccess: (message) => retainMessage(input, request.mode, message),
    }),
  );
};

const handleCurrentInstance = (
  input: SendDependencies,
  request: typeof routerContract.RawRouterSendRequest.Type,
  verifiedRequest: VerifiedAgentRequest,
): Effect.Effect<
  typeof routerContract.RouterSendResult.Type,
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.InternalServerError>
> =>
  Schema.decodeUnknown(SignedMessage.live)(request.signedMessage, {
    exact: true,
    onExcessProperty: "error",
  }).pipe(
    Effect.matchEffect({
      onFailure: () => Effect.succeed(messageInvalid),
      onSuccess: (signedMessage) =>
        handleDecodedMessage(input, request, verifiedRequest, signedMessage),
    }),
  );

function handleSend(
  input: SendDependencies,
  request: typeof routerContract.RawRouterSendRequest.Type,
  verifiedRequest: VerifiedAgentRequest,
): Effect.Effect<
  typeof routerContract.RouterSendResult.Type,
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.InternalServerError>
> {
  return request.expectedRouterInstanceId !== input.routerInstanceId
    ? Effect.succeed({
        kind: "router_restarted",
        routerInstanceId: input.routerInstanceId,
      })
    : handleCurrentInstance(input, request, verifiedRequest);
}

function encodeMessage(
  signedMessage: LiveSignedMessage & VerifiedSignedMessage,
): Effect.Effect<string, InstanceType<typeof httpErrors.InternalServerError>> {
  return Schema.encode(SignedMessage)(signedMessage).pipe(
    Effect.catchTag("ParseError", () => Effect.fail(new InternalServerError())),
    Effect.flatMap((encoded) => {
      const jcs = canonicalize(encoded);
      return jcs === undefined
        ? Effect.fail(new InternalServerError())
        : Effect.succeed(jcs);
    }),
  );
}

function digestMessage(
  encodedMessageJcs: string,
): Effect.Effect<
  typeof routerContract.SignedMessageDigest.Type,
  InstanceType<typeof httpErrors.InternalServerError>
> {
  return Effect.tryPromise({
    try: () =>
      globalThis.crypto.subtle.digest(
        "SHA-256",
        utf8Encoder.encode(encodedMessageJcs),
      ),
    catch: () => new InternalServerError(),
  }).pipe(
    Effect.map(
      (digest) => `smd_${Encoding.encodeBase64Url(new Uint8Array(digest))}`,
    ),
    Effect.flatMap(Schema.decodeUnknown(routerContract.SignedMessageDigest)),
    Effect.catchTag("ParseError", () => Effect.fail(new InternalServerError())),
  );
}
