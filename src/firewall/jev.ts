/** @file Jev's bounded-choice HTTP adapter with no credential-bearing redirects. */

import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "@effect/platform";
import { Effect, type Redacted, Schema, Stream } from "effect";
import {
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  DecisionResponse,
} from "./check.js";

/** Jev's model and the key sent as the bearer. */
interface JevOptions {
  readonly apiKey: Redacted.Redacted;
  readonly model: string;
}

const jevVendorName = "typesafe";
const jevUrl = "https://api.typesafe.ai/v1/systemone";
/** The most bytes of a response body that are read. */
const maximumResponseBytes = 1_048_576;
const wireResponse = Schema.Struct({
  model: Schema.NonEmptyTrimmedString,
  answers: Schema.Record({
    key: Schema.NonEmptyTrimmedString,
    value: Schema.Struct({
      type: Schema.Literal("choice"),
      choice: Schema.NonEmptyTrimmedString,
      probabilities: Schema.Record({
        key: Schema.NonEmptyTrimmedString,
        value: Schema.Number.pipe(Schema.between(0, 1)),
      }),
      confidence: Schema.optional(Schema.Number.pipe(Schema.between(0, 1))),
    }),
  }),
  usage: Schema.Record({
    key: Schema.NonEmptyTrimmedString,
    value: Schema.NonNegativeInt,
  }),
});

const prepareJevRequest = Effect.fn("MessagePolicy.Jev.prepareRequest")(
  function* (request: DecisionRequest, options: JevOptions) {
    return yield* HttpClientRequest.post(jevUrl).pipe(
      HttpClientRequest.bearerToken(options.apiKey),
      HttpClientRequest.bodyJson({
        model: options.model,
        state: request.state,
        questions: Object.fromEntries(
          Object.entries(request.questions).map(([id, question]) => [
            id,
            {
              type: "choice",
              instructions: question.instructions,
              criteria: question.criteria,
            },
          ]),
        ),
      }),
      Effect.catchTag("HttpBodyError", () =>
        Effect.fail(new DecisionProviderError({ reason: "transport" })),
      ),
    );
  },
);

const readJevBody = Effect.fn("MessagePolicy.Jev.readBody")(function* (
  response: HttpClientResponse.HttpClientResponse,
) {
  const decoder = new TextDecoder();
  const body = yield* response.stream.pipe(
    Stream.runFoldEffect({ bytes: 0, text: "" }, (current, chunk) => {
      const bytes = current.bytes + chunk.byteLength;
      return bytes > maximumResponseBytes
        ? Effect.fail(new DecisionProviderError({ reason: "budget" }))
        : Effect.succeed({
            bytes,
            text: current.text + decoder.decode(chunk, { stream: true }),
          });
    }),
    Effect.map((current) => current.text + decoder.decode()),
    Effect.catchTag("ResponseError", (error) =>
      error.reason === "EmptyBody"
        ? Effect.succeed("")
        : Effect.fail(new DecisionProviderError({ reason: "transport" })),
    ),
  );
  return body;
});

const parseJevResponse = Effect.fn("MessagePolicy.Jev.parseResponse")(
  function* (response: HttpClientResponse.HttpClientResponse) {
    const body = yield* readJevBody(response);
    if (response.status < 200 || response.status >= 300) {
      return yield* new DecisionProviderError({
        reason: "http",
        status: response.status,
      });
    }
    const wire = yield* Schema.decodeUnknown(Schema.parseJson(wireResponse), {
      onExcessProperty: "error",
    })(body).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new DecisionProviderError({ reason: "invalid-response" })),
      ),
    );
    return new DecisionResponse({
      answers: Object.fromEntries(
        Object.entries(wire.answers).map(([id, answer]) => [
          id,
          {
            choice: answer.choice,
            probabilities: answer.probabilities,
          },
        ]),
      ),
    });
  },
);

const requestJev = Effect.fn("MessagePolicy.Jev.decide")(function* (
  request: DecisionRequest,
  options: JevOptions,
) {
  const client = yield* HttpClient.HttpClient;
  const httpRequest = yield* prepareJevRequest(request, options);
  const response = yield* client.execute(httpRequest).pipe(
    Effect.catchTags({
      RequestError: () =>
        Effect.fail(new DecisionProviderError({ reason: "transport" })),
      ResponseError: () =>
        Effect.fail(new DecisionProviderError({ reason: "transport" })),
    }),
  );
  return yield* parseJevResponse(response);
});

/**
 * Uses a pinned model. The caller owns retries and timeouts. Calls remain
 * interruptible while consuming response bodies. Credentials stay out of
 * serialized diagnostics.
 */
export function makeJevDecisionProvider(options: JevOptions): DecisionProvider {
  const configuration = Object.freeze({ ...options });
  return {
    provider: jevVendorName,
    model: configuration.model,
    decide: (request) =>
      requestJev(request, configuration).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.RequestInit, {
          redirect: "error",
        }),
      ),
  };
}
