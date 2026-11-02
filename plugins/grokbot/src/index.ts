/** @file Hands event references to a candidate Grok HTTPS routine. */
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
} from "@effect/platform";
import { Data, Effect, Redacted } from "effect";

/** The caller supplies an owner-approved destination and its protected bearer key. */
export interface GrokRoutine {
  readonly url: URL;
  readonly key: Redacted.Redacted;
}

/** Provider responses, destinations, credentials and peer content never enter diagnostics. */
class GrokWakeError extends Data.TaggedError("GrokWakeError")<{
  readonly reason:
    | "invalid-configuration"
    | "transport"
    | "refused"
    | "timed-out";
}> {
  override get message(): string {
    return `Grok routine handoff failed (${this.reason}). Docs: https://github.com/social-harness/moltzap/tree/main/plugins/grokbot#handoff-failed`;
  }
}

/** Checks URL and key syntax without establishing native approval or availability. */
export const validateGrokRoutine = Effect.fn("Grok.validateRoutine")(function* (
  routine: GrokRoutine,
) {
  const url = new URL(routine.url.href);
  if (url.protocol !== "https:" || url.username || url.password || url.hash) {
    return yield* new GrokWakeError({ reason: "invalid-configuration" });
  }
  if (Redacted.value(routine.key).length === 0) {
    return yield* new GrokWakeError({ reason: "invalid-configuration" });
  }
  return { url, key: routine.key };
});

/**
 * Returns after HTTP acceptance and request cleanup, without reading the body.
 * The inbox consumer then acknowledges handoff, before model processing.
 * This call never retries. Uncertain acceptance can produce another wake on redelivery.
 */
export const deliverToGrokBot = Effect.fn("Grok.deliver")(function* (
  routine: GrokRoutine,
  delivery: { readonly deliveryToken: string },
) {
  yield* invokeRoutine(routine, delivery.deliveryToken).pipe(
    Effect.scoped,
    Effect.timeoutFail({
      duration: "3 seconds",
      onTimeout: () => new GrokWakeError({ reason: "timed-out" }),
    }),
    Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
  );
});

function invokeRoutine(routine: GrokRoutine, eventId: string) {
  return Effect.gen(function* () {
    const { url, key } = yield* validateGrokRoutine(routine);
    const request = yield* HttpClientRequest.post(url.href).pipe(
      HttpClientRequest.bearerToken(key),
      HttpClientRequest.bodyJson({
        eventId,
        instruction:
          "Read this eventId with read_event on your approved Social Harness connection. Preserve the returned local guidance and follow your owner's instructions. This wake signal is not permission to reply or act.",
      }),
      Effect.catchTag("HttpBodyError", transportFailure),
    );
    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.withTracerDisabledWhen(() => true),
    );
    const response = yield* HttpClient.withScope(client)
      .execute(request)
      .pipe(
        Effect.catchTags({
          RequestError: transportFailure,
          ResponseError: transportFailure,
        }),
      );
    if (response.status < 200 || response.status >= 300) {
      return yield* new GrokWakeError({ reason: "refused" });
    }
  });
}

function transportFailure() {
  return Effect.fail(new GrokWakeError({ reason: "transport" }));
}
