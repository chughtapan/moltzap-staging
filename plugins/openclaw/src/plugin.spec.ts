/** @file The channel records a send in its recipient's session and refuses to start an account without the host runtime. */

import { live as it } from "@effect/vitest";
import { Effect } from "effect";
import { resolveAgentRoute } from "openclaw/plugin-sdk/routing";
import { expect, it as unit } from "vitest";
import { createChannelPlugin } from "./plugin.js";

unit.each(["shared", "private"] as const)(
  "records a send in the session its recipient's turns use in %s mode",
  (mode) =>
    Effect.gen(function* () {
      const cfg = {
        channels: { sharness: { accounts: [{ id: "primary", mode }] } },
        agents: { entries: { primary: {} } },
        ...(mode === "private"
          ? { session: { dmScope: "per-account-channel-peer" as const } }
          : {}),
      };
      const turnRoute = resolveAgentRoute({
        cfg,
        channel: "sharness",
        accountId: "primary",
        peer: { kind: "direct", id: "bob" },
      });
      const resolve = yield* Effect.fromNullable(
        createChannelPlugin().messaging?.resolveOutboundSessionRoute,
      );
      const sendRoute = yield* Effect.tryPromise({
        try: () =>
          Promise.resolve(
            resolve({
              cfg,
              agentId: turnRoute.agentId,
              accountId: "primary",
              target: "agent:bob",
            }),
          ),
        catch: (cause) => String(cause),
      });
      expect(sendRoute?.sessionKey).toBe(
        mode === "shared" ? turnRoute.mainSessionKey : turnRoute.sessionKey,
      );
    }).pipe(Effect.runPromise),
);

it("refuses account startup without the host runtime before opening MCP", () =>
  Effect.gen(function* () {
    const plugin = createChannelPlugin();
    const start = yield* Effect.fromNullable(plugin.gateway?.startAccount);
    const failure = yield* Effect.tryPromise({
      try: () =>
        Promise.resolve(
          start({
            cfg: { channels: { sharness: { accounts: [{ id: "primary" }] } } },
            accountId: "primary",
            account: { id: "primary" },
            abortSignal: new AbortController().signal,
            runtime: { log: () => {}, error: () => {}, exit: () => {} },
            getStatus: () => ({ accountId: "primary" }),
            setStatus: () => {},
          }),
        ),
      catch: (cause) => String(cause),
    }).pipe(Effect.flip);
    expect(failure).toContain("channel-runtime-unavailable");
  }));
