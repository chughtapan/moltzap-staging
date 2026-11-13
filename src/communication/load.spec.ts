/** @file A sender burst must not prevent other senders reaching the same recipient. */
import { scopedLive as it } from "@effect/vitest";
import { Clock, Context, Effect, Layer, Queue } from "effect";
import { expect } from "vitest";
import { createTestAgent } from "../__tests__/agents.js";
import { openTestDatabase } from "./__tests__/store-fixtures.js";
import { Message } from "./messages.js";
import { Communication, type Incoming } from "./service.js";

it(
  "delivers a 128-message burst and seven independent messages across 24 identities",
  () =>
    Effect.gen(function* () {
      const db = yield* openTestDatabase();
      const consumers = new Map<string, Effect.Latch>();
      const peers = yield* Effect.forEach(
        Array.from({ length: 24 }).keys(),
        (index) =>
          db.run(
            Effect.gen(function* () {
              const agent = yield* createTestAgent(`peer${index}`);
              const services = yield* Layer.build(
                Communication.layer({ ...agent, consumers }),
              );
              return {
                ...agent,
                service: Context.get(services, Communication),
              };
            }),
          ),
        { concurrency: 1 },
      );
      const recipient = peers[0];
      if (recipient === undefined) {
        return yield* Effect.dieMessage("Missing recipient");
      }
      const received = yield* Queue.unbounded<Incoming>();
      yield* Effect.forEach(
        peers,
        (peer) =>
          peer.service
            .consume((incoming) =>
              peer === recipient
                ? Queue.offer(received, incoming).pipe(Effect.asVoid)
                : Effect.void,
            )
            .pipe(Effect.forkScoped),
        { concurrency: 1 },
      );
      const started = yield* Clock.currentTimeMillis;
      const sent = yield* Effect.forEach(
        peers.slice(1),
        (peer, index) =>
          Effect.forEach(
            Array.from({ length: index < 16 ? 8 : 1 }).keys(),
            (ordinal) =>
              Message.create({
                author: peer.card,
                recipients: [recipient.card],
                content: [{ type: "text", text: `${index}:${ordinal}` }],
              }).pipe(Effect.tap(peer.service.send)),
            { concurrency: 1 },
          ),
        { concurrency: 23 },
      );
      const messages = yield* Queue.takeN(received, sent.flat().length).pipe(
        Effect.timeout("30 seconds"),
      );
      const elapsed = (yield* Clock.currentTimeMillis) - started;
      expect(
        new Set(
          [...messages].flatMap((incoming) =>
            incoming.kind === "message" ? [incoming.message.msgId] : [],
          ),
        ),
      ).toEqual(new Set(sent.flat().map((message) => message.msgId)));
      yield* Effect.logInfo("Delivery burst complete").pipe(
        Effect.annotateLogs({
          messages: sent.flat().length,
          identities: peers.length,
          elapsedMilliseconds: elapsed,
        }),
      );
    }),
  60000,
);
