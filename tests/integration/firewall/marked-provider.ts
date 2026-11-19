/**
 * @file A deterministic decision provider endpoint for the integration run,
 * started once before the suites. It decides each check by the text it
 * judges: an incoming text carrying `[in]`, or an outgoing text carrying
 * `[out]`, breaks every rule it is asked about, and a text carrying
 * `[unsure]` is left unsettled, which refuses it outgoing and allows it
 * incoming with guidance.
 */

import type { TestProject } from "vitest/node";
import { HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { Effect, Exit, Schema, Scope } from "effect";
// eslint-disable-next-line agent-code-guard/prefer-effect-platform -- NodeHttpServer owns the provider listener's lifetime.
import { createServer } from "node:http";

declare module "vitest" {
  /** What the global setup hands every suite. */
  interface ProvidedContext {
    /** The endpoint a message policy names as its decision provider. */
    markedProviderEndpoint: string;
  }
}

/** The lifetime of the endpoint's listener. */
const scope = Effect.runSync(Scope.make());

const serve = Effect.gen(function* () {
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: 0,
  });
  if (server.address._tag !== "TcpAddress") {
    return yield* Effect.dieMessage("Expected provider TCP listener");
  }
  yield* server.serve(
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      return yield* HttpServerResponse.json(
        markedResponse(yield* request.text),
      );
    }).pipe(Effect.orDie),
  );
  return `http://127.0.0.1:${String(server.address.port)}/`;
});

/** Serves the endpoint for the run and hands its address to every suite. */
export const setup = (project: TestProject) =>
  Effect.runPromise(
    serve.pipe(
      Scope.extend(scope),
      Effect.tap((endpoint) =>
        Effect.sync(() => {
          project.provide("markedProviderEndpoint", endpoint);
        }),
      ),
      Effect.asVoid,
    ),
  );

/** Stops the endpoint. */
export const teardown = () => Effect.runPromise(Scope.close(scope, Exit.void));

/** The marker an incoming text carries to break the rules it is checked against. */
const INCOMING_BREACH = "[in]";
/** The marker an outgoing text carries to break the rules it is checked against. */
const OUTGOING_BREACH = "[out]";
/** The marker a text carries for its judgment to be left unsettled. */
const UNSURE = "[unsure]";

const decisionRequest = Schema.parseJson(
  Schema.Struct({
    state: Schema.parseJson(
      Schema.Struct({
        direction: Schema.Literal("incoming", "outgoing"),
        candidate: Schema.Struct({ text: Schema.optional(Schema.String) }),
      }),
    ),
    questions: Schema.Record({
      key: Schema.String,
      value: Schema.Struct({
        criteria: Schema.Record({ key: Schema.String, value: Schema.Unknown }),
      }),
    }),
  }),
);

/** Answers every question of a request body by the markers in its candidate's text. */
function markedResponse(body: string) {
  const { state, questions } = Schema.decodeUnknownSync(decisionRequest)(body);
  const candidate = state.candidate.text ?? "";
  const breached = candidate.includes(
    state.direction === "incoming" ? INCOMING_BREACH : OUTGOING_BREACH,
  );
  const selected = breached ? "violation" : "compliant";
  const probability = candidate.includes(UNSURE) ? 0.6 : 1;
  return {
    model: "deterministic",
    usage: {},
    answers: Object.fromEntries(
      Object.entries(questions).map(([id, question]) => [
        id,
        {
          type: "choice",
          choice: selected,
          probabilities: Object.fromEntries(
            Object.keys(question.criteria).map((choice) => [
              choice,
              choice === selected ? probability : 1 - probability,
            ]),
          ),
        },
      ]),
    ),
  };
}
