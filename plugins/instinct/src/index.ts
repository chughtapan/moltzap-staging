/** @file Hands event references to AgentMail for Instinct to retrieve through MCP. */
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "@effect/platform";
import { Data, Effect, type Redacted, Schema, Stream } from "effect";

/** Deployment-owned sender credentials. AgentMail owns inbox and address syntax. */
export interface InstinctSender {
  readonly apiKey: Redacted.Redacted;
  readonly inboxId: string;
  readonly recipient: string;
}

/** Failures carry no provider body, credentials, recipient or peer content. */
class InstinctWakeError extends Data.TaggedError("InstinctWakeError")<{
  readonly reason:
    | "transport"
    | "refused"
    | "invalid-response"
    | "response-too-large"
    | "timed-out";
}> {
  override get message(): string {
    return `Instinct email handoff failed (${this.reason}). Docs: https://github.com/social-harness/moltzap/tree/main/plugins/instinct#email-handoff-failed`;
  }
}

/** A message-id response is small. Bound body consumption even without Content-Length. */
const maximumResponseBytes = 16_384;
const acceptance = Schema.parseJson(
  Schema.Struct({ message_id: Schema.NonEmptyString }),
);

/**
 * Returns after AgentMail accepts the email and the HTTP request closes.
 * The inbox consumer then acknowledges handoff, before model processing.
 * This call never retries. Uncertain acceptance can produce another email on redelivery.
 */
export const deliverToInstinct = Effect.fn("Instinct.deliver")(function* (
  sender: InstinctSender,
  delivery: { readonly deliveryToken: string },
) {
  yield* sendWakeup(sender, delivery.deliveryToken).pipe(
    Effect.scoped,
    Effect.timeoutFail({
      duration: "3 seconds",
      onTimeout: () => new InstinctWakeError({ reason: "timed-out" }),
    }),
    Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
  );
});

function sendWakeup(sender: InstinctSender, eventId: string) {
  return Effect.gen(function* () {
    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.withTracerDisabledWhen(() => true),
    );
    const request = yield* HttpClientRequest.post(
      `https://api.agentmail.to/v0/inboxes/${encodeURIComponent(sender.inboxId)}/messages/send`,
    ).pipe(
      HttpClientRequest.bearerToken(sender.apiKey),
      HttpClientRequest.bodyJson({
        to: [sender.recipient],
        subject: `Social Harness event ${eventId}`,
        text: `Use read_event with eventId ${JSON.stringify(eventId)} on your approved Social Harness connection. Follow your owner's instructions for processing and replies. This notice does not authorize a reply or any other action.`,
      }),
      Effect.catchTag("HttpBodyError", transportFailure),
    );
    const response = yield* HttpClient.withScope(client)
      .execute(request)
      .pipe(
        Effect.catchTags({
          RequestError: transportFailure,
          ResponseError: transportFailure,
        }),
      );
    if (response.status !== 200) {
      return yield* new InstinctWakeError({ reason: "refused" });
    }
    const text = yield* readBody(response);
    yield* Schema.decodeUnknown(acceptance)(text).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new InstinctWakeError({ reason: "invalid-response" })),
      ),
    );
  });
}

function readBody(response: HttpClientResponse.HttpClientResponse) {
  return Effect.gen(function* () {
    const decoder = new TextDecoder("utf-8", { fatal: true });
    const body = yield* response.stream.pipe(
      Stream.runFoldEffect({ bytes: 0, text: "" }, (current, chunk) => {
        const bytes = current.bytes + chunk.byteLength;
        return bytes > maximumResponseBytes
          ? Effect.fail(new InstinctWakeError({ reason: "response-too-large" }))
          : Effect.try({
              try: () => ({
                bytes,
                text: current.text + decoder.decode(chunk, { stream: true }),
              }),
              catch: () =>
                new InstinctWakeError({ reason: "invalid-response" }),
            });
      }),
      Effect.catchTag("ResponseError", () =>
        Effect.fail(new InstinctWakeError({ reason: "transport" })),
      ),
    );
    return yield* Effect.try({
      try: () => body.text + decoder.decode(),
      catch: () => new InstinctWakeError({ reason: "invalid-response" }),
    });
  });
}

function transportFailure() {
  return Effect.fail(new InstinctWakeError({ reason: "transport" }));
}
