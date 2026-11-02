/** @file Registry-owned bootstrap admission before durable registration. */

import type { HttpServerRequest } from "@effect/platform";
import { AgentPublicKey } from "@moltzap/social-harness";
import {
  httpErrors,
  registryContract,
  type VerifiedHttpSignature,
  verifyBootstrapRequest,
} from "@moltzap/social-harness/internal";
import { Effect, Redacted, Schema } from "effect";
import { createHash, timingSafeEqual } from "node:crypto";
import type { RegistryStorage } from "./storage.js";

/* eslint-disable @typescript-eslint/naming-convention -- Closed HTTP error constructors retain their class names when bound from the service capability. */
const { AuthenticationFailedError, MalformedRequestError, OverloadedError } =
  httpErrors;
/* eslint-enable @typescript-eslint/naming-convention -- Restore variable naming after binding error constructors. */

const registrationPublicKeyRepresentation = Schema.Struct({
  crv: Schema.Literal("Ed25519"),
  kty: Schema.Literal("OKP"),
  x: Schema.String.pipe(
    Schema.filter((value) => {
      const bytes = Buffer.from(value, "base64url");
      return bytes.byteLength === 32 && bytes.toString("base64url") === value;
    }),
  ),
});

const minimumRegistrationBody = Schema.Struct({
  request: Schema.Struct({
    publicKey: registrationPublicKeyRepresentation,
  }),
});

const admissionValue =
  /^Sharness-Admission ((?=.{8,512}$)[A-Za-z0-9\-._~+/]+=*)$/;

type BootstrapRegistrationInput = Readonly<{
  httpRequest: HttpServerRequest.HttpServerRequest;
  bodyBytes: Uint8Array;
  admissionCredential: Redacted.Redacted;
  nonceCapacity: number;
  storage: RegistryStorage;
}>;

/**
 * Verifies the bootstrap credential and signed request, then durably claims
 * its replay nonce before validating the complete registration body.
 */
export function verifyBootstrapRegistration(
  input: BootstrapRegistrationInput,
): Effect.Effect<
  typeof registryContract.RegistryRegisterRequest.Type,
  | InstanceType<typeof httpErrors.MalformedRequestError>
  | InstanceType<typeof httpErrors.AuthenticationFailedError>
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.VersionMismatchError>
  | InstanceType<typeof httpErrors.UnavailableError>
  | InstanceType<typeof httpErrors.InternalServerError>
> {
  return Effect.gen(function* () {
    const { parsedBody } = yield* verifyBootstrapRequest({
      httpRequest: input.httpRequest,
      bodyBytes: input.bodyBytes,
      admitPublicKey: (body) => admitPublicKey(input, body),
      claimNonce: (signature) => claimNonce(input, signature),
    });
    const complete = yield* malformedRequest(
      Schema.decodeUnknown(registryContract.registrationBody)(parsedBody, {
        exact: true,
        onExcessProperty: "error",
      }),
    );
    return complete.request;
  }).pipe(Effect.withSpan("verifyBootstrapRegistration"));
}

function admitPublicKey(
  input: BootstrapRegistrationInput,
  parsedBody: unknown,
) {
  return Effect.gen(function* () {
    const minimum = yield* malformedRequest(
      Schema.decodeUnknown(minimumRegistrationBody)(parsedBody),
    );
    yield* requireAdmissionCredential(input);
    return yield* authenticatePublicKey(minimum.request.publicKey);
  });
}

function malformedRequest<A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, InstanceType<typeof httpErrors.MalformedRequestError>, R> {
  return effect.pipe(
    // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- Canonical JSON and closed request-schema failures share one exact public malformed envelope.
    Effect.mapError(() => new MalformedRequestError()),
  );
}

function requireAdmissionCredential(
  input: BootstrapRegistrationInput,
): Effect.Effect<
  void,
  InstanceType<typeof httpErrors.AuthenticationFailedError>
> {
  const credential = readAdmissionCredential(
    input.httpRequest.headers.authorization,
  );
  return credential !== undefined &&
    sameCredential(credential, input.admissionCredential)
    ? Effect.void
    : Effect.fail(new AuthenticationFailedError());
}

function authenticatePublicKey(
  candidate: typeof registrationPublicKeyRepresentation.Type,
): Effect.Effect<
  AgentPublicKey,
  InstanceType<typeof httpErrors.AuthenticationFailedError>
> {
  return Schema.decodeUnknown(AgentPublicKey)(candidate).pipe(
    // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- A syntactically valid but cryptographically unusable verification key is an authentication failure.
    Effect.mapError(() => new AuthenticationFailedError()),
  );
}

function claimNonce(
  input: BootstrapRegistrationInput,
  signature: VerifiedHttpSignature,
): Effect.Effect<
  void,
  | InstanceType<typeof httpErrors.AuthenticationFailedError>
  | InstanceType<typeof httpErrors.OverloadedError>
  | InstanceType<typeof httpErrors.UnavailableError>
  | InstanceType<typeof httpErrors.InternalServerError>
> {
  return Effect.gen(function* () {
    const claim = yield* input.storage.claimRegistrationNonce({
      nonce: signature.nonce,
      expires: signature.expires,
      now: signature.verifiedAt,
      capacity: input.nonceCapacity,
    });
    if (claim === "replayed") {
      return yield* new AuthenticationFailedError();
    }
    if (claim === "full") {
      return yield* new OverloadedError();
    }
  });
}

function readAdmissionCredential(authorization?: string): string | undefined {
  if (authorization === undefined) {
    return undefined;
  }
  const match = admissionValue.exec(authorization);
  if (match === null) {
    return undefined;
  }
  const credential = match[1];
  return typeof credential === "string" ? credential : undefined;
}

function sameCredential(
  received: string,
  configured: Redacted.Redacted,
): boolean {
  const receivedDigest = createHash("sha256").update(received).digest();
  const configuredDigest = createHash("sha256")
    .update(Redacted.value(configured))
    .digest();
  return timingSafeEqual(receivedDigest, configuredDigest);
}
