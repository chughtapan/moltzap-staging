/** @file Throwaway: the daemon delivers the inbox through the MCP SDK's Events extension. */

import {
  Client,
  StreamableHTTPClientTransport,
} from "@modelcontextprotocol/client";
import { EventsClientExtension } from "@modelcontextprotocol/client/ext/events";
import { INBOX_PENDING_EVENT } from "@social-harness/social-harness";
import { Effect, Fiber, Queue, Redacted } from "effect";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { expect, it } from "vitest";
import {
  type Daemon,
  processTrace,
  send,
  SERVER_AGENT_OWNER,
  sharedDaemon,
  type SignedIn,
  stopProcess,
} from "./daemon-fixture.js";

const TIMEOUT_MS = 120_000;

/** Reads the signed-in agent's inbox through the host library into a queue. */
const reading = (host: SignedIn) =>
  Effect.gen(function* () {
    const texts = yield* Queue.unbounded<string>();
    const fiber = yield* host.connection
      .consumeInbox((entry) =>
        Queue.offer(
          texts,
          entry.message.content
            .map((part) => (part.type === "text" ? part.text : ""))
            .join(""),
        ),
      )
      .pipe(Effect.forkScoped);
    return { texts, fiber };
  });

/** An SDK client with the Events extension, signed in as `owner`. */
const eventsClient = (
  daemon: Daemon,
  owner: string,
  events: EventsClientExtension,
) =>
  Effect.gen(function* () {
    const bearer = yield* daemon.bearer(owner);
    const client = new Client(
      { name: "sdk-events-spike", version: "0" },
      {
        versionNegotiation: { mode: { pin: "2026-07-28" } },
        extensions: [events],
      },
    );
    yield* Effect.acquireRelease(
      Effect.promise(() =>
        client.connect(
          new StreamableHTTPClientTransport(daemon.endpoint, {
            requestInit: {
              headers: { Authorization: `Bearer ${Redacted.value(bearer)}` },
            },
          }),
        ),
      ),
      () => Effect.promise(() => client.close()),
    );
    return client;
  });

it(
  "delivers a client's message to the server agent's host by push, and a host that returns without a cursor loses nothing",
  processTrace(
    Effect.gen(function* () {
      const daemon = yield* sharedDaemon();
      const running = yield* daemon.start();
      const server = yield* daemon.signInAs(SERVER_AGENT_OWNER);
      const client = yield* daemon.signInAs("bob");

      const first = yield* reading(server);
      yield* send(client, { text: "one" });
      expect(yield* Queue.take(first.texts)).toBe("one");
      console.log("SPIKE push delivered: one");

      yield* Fiber.interrupt(first.fiber);
      yield* send(client, { text: "two while the host is away" });
      yield* stopProcess(running);
      yield* daemon.start();
      yield* send(client, { text: "three after the daemon restarted" });

      const returned = yield* daemon.signInAs(SERVER_AGENT_OWNER);
      const second = yield* reading(returned);
      const received = [
        yield* Queue.take(second.texts),
        yield* Queue.take(second.texts),
      ];
      expect(received).toEqual([
        "two while the host is away",
        "three after the daemon restarted",
      ]);
      console.log(`SPIKE resumed without a cursor: ${JSON.stringify(received)}`);
    }),
  ),
  TIMEOUT_MS,
);

it(
  "lists the inbox event with poll, push and webhook, and delivers by poll and by webhook",
  processTrace(
    Effect.gen(function* () {
      let handle: (request: Request) => Promise<Response> = () =>
        Promise.resolve(new Response(null, { status: 503 }));
      const http = createServer((request, response) => {
        const chunks: Buffer[] = [];
        request.on("data", (chunk: Buffer) => chunks.push(chunk));
        request.on("end", () => {
          void handle(
            new Request(`http://127.0.0.1${request.url ?? "/"}`, {
              method: request.method ?? "POST",
              headers: Object.entries(request.headers).flatMap(
                ([name, value]): Array<[string, string]> =>
                  typeof value === "string" ? [[name, value]] : [],
              ),
              body: Buffer.concat(chunks),
            }),
          ).then(async (answer) => {
            response.writeHead(answer.status);
            response.end(await answer.text());
          });
        });
      });
      yield* Effect.acquireRelease(
        Effect.async<void>((resume) => {
          http.listen(0, "127.0.0.1", () => resume(Effect.void));
        }),
        () => Effect.sync(() => http.close()),
      );
      const { port } = http.address() as AddressInfo;
      const origin = `http://127.0.0.1:${String(port)}`;
      const hooked = new EventsClientExtension({
        webhook: { url: `${origin}/hook` },
      });
      handle = hooked.createWebhookHandler();

      const daemon = yield* sharedDaemon({
        settings: { trustedCallbackOrigins: [origin] },
      });
      yield* daemon.start();
      const client = yield* daemon.signInAs("bob");

      const polling = new EventsClientExtension();
      yield* eventsClient(daemon, SERVER_AGENT_OWNER, polling);
      const listed = yield* Effect.promise(() => polling.list());
      expect(listed.events.map(({ name, delivery }) => ({ name, delivery }))).toEqual([
        { name: INBOX_PENDING_EVENT, delivery: ["poll", "push", "webhook"] },
      ]);
      console.log(`SPIKE events/list: ${JSON.stringify(listed.events.map(({ name, delivery }) => ({ name, delivery })))}`);

      const polled = yield* Effect.promise(() =>
        polling.subscribe({ name: INBOX_PENDING_EVENT, delivery: "poll" }),
      );
      expect(polled.deliveryMode).toBe("poll");
      yield* send(client, { text: "by poll" });
      const pollIterator = polled[Symbol.asyncIterator]();
      const byPoll = yield* Effect.promise(() => pollIterator.next());
      expect(byPoll.value).toMatchObject({
        data: { message: { content: [{ type: "text", text: "by poll" }] } },
      });
      console.log(`SPIKE poll delivered eventId=${String(byPoll.value?.eventId)} cursor=${String(byPoll.value?.cursor)}`);
      yield* Effect.promise(() => pollIterator.next().then(() => undefined, () => undefined)).pipe(Effect.fork);
      yield* Effect.sleep("1500 millis");
      yield* Effect.promise(() => polled.cancel());

      yield* eventsClient(daemon, SERVER_AGENT_OWNER, hooked);
      const webhook = yield* Effect.promise(() =>
        hooked.subscribe({ name: INBOX_PENDING_EVENT }),
      );
      expect(webhook.deliveryMode).toBe("webhook");
      yield* send(client, { text: "by webhook" });
      const webhookIterator = webhook[Symbol.asyncIterator]();
      const byWebhook = yield* Effect.promise(() => webhookIterator.next());
      expect(byWebhook.value).toMatchObject({
        data: { message: { content: [{ type: "text", text: "by webhook" }] } },
      });
      console.log(`SPIKE webhook delivered eventId=${String(byWebhook.value?.eventId)} cursor=${String(byWebhook.value?.cursor)}`);
      yield* Effect.promise(() => webhook.cancel());
    }),
  ),
  TIMEOUT_MS,
);
