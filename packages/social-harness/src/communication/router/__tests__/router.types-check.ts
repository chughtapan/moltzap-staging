/**
 * @file
 * Public request and result types derive from their schemas. These canaries
 * keep their exact readonly shapes so a schema edit cannot silently change
 * the published types.
 *
 * Router's exported capability keeps send and poll on their exact request,
 * success, failure, and requirement channels. This prevents a later adapter
 * change from widening either operation to unknown or leaking private server
 * construction through the public layer.
 */

import type { HttpClient } from "@effect/platform";
import type { Duration, Effect, Layer, Types } from "effect";
import type { AgentId } from "../../../identity/agent.js";
import type { LiveSignedMessage } from "../../../identity/envelope/signed-message.js";
import type {
  AgentSigningError,
  AuthenticationFailedError,
  InternalServerError,
  MalformedRequestError,
  MethodNotAllowedError,
  OverloadedError,
  PayloadTooLargeError,
  RouteNotFoundError,
  UnavailableError,
  UnsupportedMediaTypeError,
  VersionMismatchError,
} from "../../../identity/http-signatures.js";
import type { AgentSigningAuthority } from "../../../identity/keys.js";
import type * as RouterPackage from "../api.js";

type Expect<Value extends true> = Value;

type RouterFailure =
  | MalformedRequestError
  | AuthenticationFailedError
  | RouteNotFoundError
  | MethodNotAllowedError
  | VersionMismatchError
  | PayloadTooLargeError
  | UnsupportedMediaTypeError
  | OverloadedError
  | UnavailableError
  | InternalServerError
  | RouterPackage.RouterConnectionError
  | RouterPackage.RouterRequestTimeoutError
  | RouterPackage.RouterInvalidResponseError
  | AgentSigningError;

type SendCall = Readonly<{
  request: RouterPackage.RouterSendRequest;
  callerAgentId: AgentId;
  signingAuthority: AgentSigningAuthority;
}>;
type PollCall = Readonly<{
  request: RouterPackage.RouterPollRequest;
  callerAgentId: AgentId;
  signingAuthority: AgentSigningAuthority;
}>;

type SendRequestShapeIsExact = Expect<
  Types.Equals<
    RouterPackage.RouterSendRequest,
    Readonly<{
      expectedRouterInstanceId: RouterPackage.RouterInstanceId;
      mode: "initial" | "retry";
      signedMessage: LiveSignedMessage;
    }>
  >
>;
type PollRequestShapeIsExact = Expect<
  Types.Equals<
    RouterPackage.RouterPollRequest,
    Readonly<{ pollCursor?: RouterPackage.PollCursor }>
  >
>;
type SendResultShapeIsExact = Expect<
  Types.Equals<
    RouterPackage.RouterSendResult,
    | Readonly<{
        kind: "accepted";
        routerInstanceId: RouterPackage.RouterInstanceId;
        signedMessageDigest: RouterPackage.SignedMessageDigest;
      }>
    | Readonly<{
        kind: "router_restarted";
        routerInstanceId: RouterPackage.RouterInstanceId;
      }>
    | Readonly<{ kind: "message_invalid" }>
    | Readonly<{ kind: "idempotency_conflict" }>
    | Readonly<{ kind: "retry_identity_unknown" }>
  >
>;
type PollResultShapeIsExact = Expect<
  Types.Equals<
    RouterPackage.RouterPollResult,
    | Readonly<{
        kind: "batch";
        routerInstanceId: RouterPackage.RouterInstanceId;
        signedMessages: readonly LiveSignedMessage[];
        pollCursor: RouterPackage.PollCursor;
      }>
    | Readonly<{
        kind: "feed_gap";
        routerInstanceId: RouterPackage.RouterInstanceId;
      }>
    | Readonly<{ kind: "cursor_invalid" }>
  >
>;

type SendEffect = ReturnType<typeof RouterPackage.Router.send>;
type PollEffect = ReturnType<typeof RouterPackage.Router.poll>;
type RouterLayer = ReturnType<typeof RouterPackage.Router.layer>;

type SendInputIsExact = Expect<
  Types.Equals<Parameters<typeof RouterPackage.Router.send>[0], SendCall>
>;
type SendSuccessIsExact = Expect<
  Types.Equals<
    Effect.Effect.Success<SendEffect>,
    RouterPackage.RouterSendResult
  >
>;
type SendFailureIsExact = Expect<
  Types.Equals<Effect.Effect.Error<SendEffect>, RouterFailure>
>;
type SendRequiresOnlyRouter = Expect<
  Types.Equals<Effect.Effect.Context<SendEffect>, RouterPackage.Router>
>;
type PollInputIsExact = Expect<
  Types.Equals<Parameters<typeof RouterPackage.Router.poll>[0], PollCall>
>;
type PollSuccessIsExact = Expect<
  Types.Equals<
    Effect.Effect.Success<PollEffect>,
    RouterPackage.RouterPollResult
  >
>;
type PollFailureIsExact = Expect<
  Types.Equals<Effect.Effect.Error<PollEffect>, RouterFailure>
>;
type PollRequiresOnlyRouter = Expect<
  Types.Equals<Effect.Effect.Context<PollEffect>, RouterPackage.Router>
>;
type LayerInputIsExact = Expect<
  Types.Equals<
    Parameters<typeof RouterPackage.Router.layer>[0],
    Readonly<{
      origin: URL;
      sendTimeout: Duration.Duration;
      pollTimeout: Duration.Duration;
    }>
  >
>;
type LayerProvidesOnlyRouter = Expect<
  Types.Equals<Layer.Layer.Success<RouterLayer>, RouterPackage.Router>
>;
type LayerCannotFail = Expect<
  Types.Equals<Layer.Layer.Error<RouterLayer>, never>
>;
type LayerRequiresOnlyHttpClient = Expect<
  Types.Equals<Layer.Layer.Context<RouterLayer>, HttpClient.HttpClient>
>;

/** Compile-time evidence for the complete public Router capability channels. */
export type RouterCapabilityCanaries = [
  SendRequestShapeIsExact,
  PollRequestShapeIsExact,
  SendResultShapeIsExact,
  PollResultShapeIsExact,
  SendInputIsExact,
  SendSuccessIsExact,
  SendFailureIsExact,
  SendRequiresOnlyRouter,
  PollInputIsExact,
  PollSuccessIsExact,
  PollFailureIsExact,
  PollRequiresOnlyRouter,
  LayerInputIsExact,
  LayerProvidesOnlyRouter,
  LayerCannotFail,
  LayerRequiresOnlyHttpClient,
];
