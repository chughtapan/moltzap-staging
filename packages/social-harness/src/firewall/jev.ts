/** @file Jev's bounded-choice HTTP adapter with no credential-bearing redirects. */

import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "@effect/platform";
import { Effect, Option, Redacted, Schema, Stream } from "effect";
import {
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  DecisionResponse,
} from "./check.js";

const origin = "https://api.typesafe.ai/v1/systemone";
const providerName = "typesafe";
const maximumChoices = 255;
/** Bounds response bodies retained in audits. */
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

/** The model and usage of a body that parses that far, whatever else it carries. */
const reportedUsage = Schema.decodeUnknownOption(
  Schema.parseJson(
    Schema.Struct({
      model: wireResponse.fields.model,
      usage: wireResponse.fields.usage,
    }),
  ),
);

const prepareJevRequest = Effect.fn("MessagePolicy.Jev.prepareRequest")(
  function* (
    input: DecisionRequest,
    options: { readonly apiKey: Redacted.Redacted; readonly model: string },
  ) {
    return yield* HttpClientRequest.post(origin).pipe(
      HttpClientRequest.bearerToken(options.apiKey),
      HttpClientRequest.bodyJson({
        model: options.model,
        state: input.state,
        questions: Object.fromEntries(
          Object.entries(input.questions).map(([id, question]) => [
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
  function* (
    response: HttpClientResponse.HttpClientResponse,
    apiKey: Redacted.Redacted,
  ) {
    const raw = yield* readJevBody(response).pipe(
      Effect.flatMap((text) => redactBody(text, apiKey)),
    );
    if (response.status < 200 || response.status >= 300) {
      return yield* new DecisionProviderError({
        reason: "http",
        status: response.status,
        response: raw,
        ...accountingOf(raw),
      });
    }
    const value = yield* Schema.decodeUnknown(Schema.parseJson(wireResponse), {
      onExcessProperty: "error",
    })(raw).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(
          new DecisionProviderError({
            reason: "invalid-response",
            response: raw,
            ...accountingOf(raw),
          }),
        ),
      ),
    );
    return new DecisionResponse({
      provider: providerName,
      model: value.model,
      answers: Object.fromEntries(
        Object.entries(value.answers).map(([id, answer]) => [
          id,
          {
            choice: answer.choice,
            probabilities: answer.probabilities,
          },
        ]),
      ),
      usage: value.usage,
      raw,
    });
  },
);

const requestJev = Effect.fn("MessagePolicy.Jev.decide")(function* (
  input: DecisionRequest,
  options: {
    readonly apiKey: Redacted.Redacted;
    readonly model: string;
  },
) {
  if (
    Redacted.value(options.apiKey).length === 0 ||
    options.model.trim().length === 0 ||
    options.model !== options.model.trim()
  ) {
    return yield* new DecisionProviderError({ reason: "configuration" });
  }
  if (
    Object.values(input.questions).some(
      (question) => Object.keys(question.criteria).length > maximumChoices,
    )
  ) {
    return yield* new DecisionProviderError({ reason: "budget" });
  }
  const client = yield* HttpClient.HttpClient;
  const request = yield* prepareJevRequest(input, options);
  const response = yield* client.execute(request).pipe(
    Effect.catchTags({
      RequestError: () =>
        Effect.fail(new DecisionProviderError({ reason: "transport" })),
      ResponseError: () =>
        Effect.fail(new DecisionProviderError({ reason: "transport" })),
    }),
  );
  return yield* parseJevResponse(response, options.apiKey);
});

/**
 * Uses the fixed TypeSafe origin and a pinned model. The caller owns retries
 * and timeouts. Calls remain interruptible while consuming response bodies.
 * Credentials stay out of serialized diagnostics.
 */
export function makeJevDecisionProvider(options: {
  readonly apiKey: Redacted.Redacted;
  readonly model: string;
}): DecisionProvider {
  const configuration = Object.freeze({ ...options });
  return {
    provider: providerName,
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

/** Retains reported model and usage even when the call's answers fail. */
function accountingOf(raw: string) {
  return Option.match(reportedUsage(raw), {
    onNone: () => ({}),
    onSome: ({ model, usage }) => ({
      accounting: { provider: providerName, model, usage },
    }),
  });
}

/** Decode JSON escapes before scrubbing so encoded credential echoes stay private. */
function redactBody(body: string, apiKey: Redacted.Redacted) {
  return Schema.decodeUnknown(Schema.parseJson(Schema.Unknown))(body).pipe(
    Effect.flatMap(Schema.encode(Schema.parseJson(Schema.Unknown))),
    Effect.catchTag("ParseError", () => Effect.succeed(body)),
    Effect.map((text) => text.replaceAll(Redacted.value(apiKey), "[REDACTED]")),
  );
}
