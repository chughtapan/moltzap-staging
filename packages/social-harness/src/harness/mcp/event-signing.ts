/** @file MCP Events signing and callback policy on the Effect HTTP client. */
import {
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
  HttpIncomingMessage,
} from "@effect/platform";
import { NodeHttpClient } from "@effect/platform-node";
import { Data, Effect, Layer, Option, type Scope } from "effect";
import { createHmac } from "node:crypto";
import { lookup } from "node:dns";
import { BlockList, isIP, type LookupFunction } from "node:net";
import { maximumEventBytes } from "./contracts.js";

/** Closed callback diagnostics, never an upstream body, header or network address. */
export type WebhookFailureReason =
  | "connection_refused"
  | "timeout"
  | "tls_error"
  | "http_4xx"
  | "http_5xx"
  | "challenge_failed";

/** Expected callback failure safe to report in the Events protocol. */
export class WebhookCallbackError extends Data.TaggedError(
  "WebhookCallbackError",
)<{
  readonly reason: WebhookFailureReason;
  readonly discardOccurrence?: boolean;
}> {}

/** One immutable event or verification body, signed separately on each attempt. */
export interface WebhookPost {
  readonly url: string;
  readonly secret: string;
  readonly messageId: string;
  readonly subscriptionId: string;
  readonly body: string;
  readonly timestamp: number;
}

/* eslint-disable sonarjs/no-hardcoded-ip -- These are excluded address ranges for callback SSRF defense, never configured endpoints. */
const blockedV4 = new BlockList();
for (const [address, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.88.99.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] satisfies ReadonlyArray<readonly [string, number]>) {
  blockedV4.addSubnet(address, prefix, "ipv4");
}
const globalV6 = new BlockList();
globalV6.addSubnet("2000::", 3, "ipv6");
const blockedV6 = new BlockList();
blockedV6.addSubnet("2001::", 23, "ipv6");
blockedV6.addSubnet("2001:db8::", 32, "ipv6");
blockedV6.addSubnet("2002::", 16, "ipv6");
blockedV6.addSubnet("3fff::", 20, "ipv6");
/* eslint-enable sonarjs/no-hardcoded-ip -- Restore endpoint address checks outside the exclusion table. */

/**
 * Accepts a resolved IP address only if it is public. Excludes local, mapped,
 * transition, reserved and documentation ranges.
 */
export const isPublicWebhookAddress = (address: string): boolean => {
  const family = isIP(address);
  return family === 4
    ? !blockedV4.check(address, "ipv4")
    : family === 6 &&
        globalV6.check(address, "ipv6") &&
        !blockedV6.check(address, "ipv6");
};

/**
 * Returns a canonical HTTPS URL without userinfo or fragments, or `undefined`
 * when invalid. Verification and delivery validate URLs before connecting.
 */
export const webhookUrl = (value: string): URL | undefined => {
  if (!URL.canParse(value)) {
    return undefined;
  }
  const url = new URL(value);
  if (url.protocol !== "https:") {
    return undefined;
  }
  return url.username === "" && url.password === "" && url.hash === ""
    ? url
    : undefined;
};

/** Requires `whsec_` followed by canonical base64 encoding of 24–64 bytes. */
export const validWebhookSecret = (secret: string): boolean => {
  if (!secret.startsWith("whsec_")) {
    return false;
  }
  const encoded = secret.slice("whsec_".length);
  const bytes = Buffer.from(encoded, "base64");
  return (
    bytes.length >= 24 &&
    bytes.length <= 64 &&
    bytes.toString("base64") === encoded
  );
};

/** Signs retained bytes with a fresh timestamp and includes subscription routing. */
const webhookHeaders = (
  post: WebhookPost,
): Readonly<Record<string, string>> => {
  const timestamp = String(Math.floor(post.timestamp / 1000));
  const signature = createHmac(
    "sha256",
    Buffer.from(post.secret.slice("whsec_".length), "base64"),
  )
    .update(`${post.messageId}.${timestamp}.${post.body}`)
    .digest("base64");
  return {
    "content-type": "application/json",
    "webhook-id": post.messageId,
    "webhook-timestamp": timestamp,
    "webhook-signature": `v1,${signature}`,
    "x-mcp-subscription-id": post.subscriptionId,
  };
};

const callbackFailure = (reason: WebhookFailureReason) =>
  new WebhookCallbackError({ reason });

/** Node's connection-time resolver validates every address before selecting one. */
const publicLookup: LookupFunction = (hostname, options, callback) => {
  lookup(hostname, { all: true, verbatim: true }, (error, addresses) => {
    if (error !== null) {
      callback(new Error("Callback address unavailable"), "", 4);
      return;
    }
    const first = addresses[0];
    if (
      first === undefined ||
      !addresses.every(({ address }) => isPublicWebhookAddress(address))
    ) {
      callback(new Error("Callback address unavailable"), "", 4);
    } else if (options.all === true) {
      callback(null, addresses);
    } else {
      callback(null, first.address, first.family);
    }
  });
};

/**
 * Validates DNS addresses at connection time while preserving Host and TLS names.
 * Each attempt opens a fresh connection. Redirects are disabled. The scope owns
 * sockets and response decoding.
 */
export const webhookHttpClientLayer = NodeHttpClient.layerWithoutAgent.pipe(
  Layer.provide(
    NodeHttpClient.makeAgentLayer({ keepAlive: false, lookup: publicLookup }),
  ),
);

const validatePost = (post: WebhookPost) =>
  Effect.gen(function* () {
    const url = webhookUrl(post.url);
    if (
      url === undefined ||
      !validWebhookSecret(post.secret) ||
      Buffer.byteLength(post.body, "utf8") > maximumEventBytes
    ) {
      return yield* Effect.fail(callbackFailure("connection_refused"));
    }
    const host = url.hostname.replace(/^\[|\]$/gu, "");
    if (isIP(host) !== 0 && !isPublicWebhookAddress(host)) {
      return yield* Effect.fail(callbackFailure("connection_refused"));
    }
    return url;
  });
const transportFailure = (cause: unknown) => {
  if (
    !(cause instanceof Error) ||
    !("code" in cause) ||
    typeof cause.code !== "string"
  ) {
    return callbackFailure("connection_refused");
  }
  return callbackFailure(
    cause.code.includes("TLS") || cause.code.includes("CERT")
      ? "tls_error"
      : "connection_refused",
  );
};

const executeWebhook = (
  post: WebhookPost,
): Effect.Effect<
  HttpClientResponse.HttpClientResponse,
  WebhookCallbackError,
  HttpClient.HttpClient | Scope.Scope
> =>
  Effect.gen(function* () {
    const url = yield* validatePost(post);
    const client = yield* HttpClient.HttpClient;
    const response = yield* HttpClient.withScope(client)
      .execute(
        HttpClientRequest.post(url).pipe(
          HttpClientRequest.bodyText(post.body, "application/json"),
          HttpClientRequest.setHeaders(webhookHeaders(post)),
        ),
      )
      .pipe(Effect.mapError((error) => transportFailure(error.cause)));
    if (response.status < 200 || response.status >= 300) {
      return yield* Effect.fail(
        new WebhookCallbackError({
          reason: response.status >= 500 ? "http_5xx" : "http_4xx",
          discardOccurrence: response.status === 410 || response.status === 413,
        }),
      );
    }
    return response;
  });
const boundedCallback = <A>(
  operation: Effect.Effect<
    A,
    WebhookCallbackError,
    HttpClient.HttpClient | Scope.Scope
  >,
) =>
  Effect.scoped(operation).pipe(
    Effect.timeoutFail({
      duration: "5 seconds",
      onTimeout: () => callbackFailure("timeout"),
    }),
  );

/** Reads bounded challenge-response text and closes the connection on exit. */
export const readWebhookChallenge = (
  post: WebhookPost,
): Effect.Effect<string, WebhookCallbackError, HttpClient.HttpClient> =>
  boundedCallback(
    executeWebhook(post).pipe(
      Effect.flatMap((response) => response.text),
      HttpIncomingMessage.withMaxBodySize(Option.some(4096)),
      Effect.catchTag("ResponseError", () =>
        Effect.fail(callbackFailure("challenge_failed")),
      ),
    ),
  ).pipe(Effect.withSpan("readWebhookChallenge"));

/**
 * Accepts transport delivery at 2xx headers and closes the response.
 * Response bodies cannot change that receipt.
 */
export const sendWebhook = (
  post: WebhookPost,
): Effect.Effect<void, WebhookCallbackError, HttpClient.HttpClient> =>
  boundedCallback(executeWebhook(post).pipe(Effect.asVoid)).pipe(
    Effect.withSpan("sendWebhook"),
  );
