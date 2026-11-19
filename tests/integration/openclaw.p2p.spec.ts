/** @file The OpenClaw channel plugin carries a group's messages on a P2P daemon. */

import { Effect } from "effect";
import { expect, it } from "vitest";
import { groupAddress, P2P } from "./agent-fixture.p2p.js";
import { nextItem, processTrace, send, textContent } from "./daemon-fixture.js";
import {
  acquireScenario,
  nextTurn,
  startQueuedGateway,
  stopGateway,
} from "./openclaw-fixture.js";

/** What the caller sends its group with the host's agent and a peer. */
const GROUP_TEXT = "hello to the group";

/** What the host's agent sends the group back through the message tool. */
const GROUP_REPLY = "reply to the group through the OpenClaw message tool";

/** A P2P daemon, where the host's agent and the two others are peers. */
const acquirePeers = acquireScenario({ settings: P2P }, (daemon) =>
  daemon.signInAs("openclaw-host"),
);

it(
  "carries a group message in both directions through the OpenClaw message tool",
  processTrace(
    Effect.gen(function* () {
      const hosted = yield* startQueuedGateway(yield* acquirePeers);
      const { caller, peer, target, messageTool } = hosted;
      const { id, address } = yield* send(caller, {
        to: groupAddress([caller, peer, target]),
        text: GROUP_TEXT,
      });
      expect((yield* nextTurn(hosted)).ctxPayload).toMatchObject({
        ChatType: "group",
        From: caller.address,
        BodyForAgent: GROUP_TEXT,
        MessageSid: id,
      });
      yield* nextItem(peer);
      const replied = yield* messageTool("reply", {
        to: address,
        message: GROUP_REPLY,
      });
      expect(replied.details).toMatchObject({ ok: true });
      const reply = {
        kind: "message",
        message: { author: target.agent, content: textContent(GROUP_REPLY) },
      };
      expect(yield* nextItem(caller)).toMatchObject(reply);
      expect(yield* nextItem(peer)).toMatchObject(reply);
      yield* stopGateway(hosted);
    }),
  ),
  300_000,
);
