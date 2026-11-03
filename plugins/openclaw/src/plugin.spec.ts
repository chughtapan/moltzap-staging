/** @file The channel advertises OpenClaw's standard explicitly addressed actions. */

import { live as it } from "@effect/vitest";
import { Effect } from "effect";
import { resolveAgentRoute } from "openclaw/plugin-sdk/routing";
import { afterEach, expect, it as unit, vi } from "vitest";
import { openClawTestMcpCredentialFile } from "../vitest.setup.js";
import { createChannelPlugin } from "./plugin.js";

const channelRuntime = {
  runtimeContexts: {
    register: () => ({ dispose: () => {} }),
    get: () => undefined,
    watch: () => () => {},
  },
  inbound: { buildContext: () => {}, run: () => {} },
  routing: { resolveAgentRoute: () => {} },
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

unit(
  "offers send and reply without Social Harness-specific tool parameters",
  () => {
    expect(
      createChannelPlugin().actions?.describeMessageTool({
        cfg: { channels: { sharness: { accounts: [{ id: "primary" }] } } },
      }),
    ).toEqual({ actions: ["send", "reply"] });
  },
);

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
      const turn = resolveAgentRoute({
        cfg,
        channel: "sharness",
        accountId: "primary",
        peer: { kind: "direct", id: "bob" },
      });
      const resolve = yield* Effect.fromNullable(
        createChannelPlugin().messaging?.resolveOutboundSessionRoute,
      );
      const send = yield* Effect.tryPromise({
        try: () =>
          Promise.resolve(
            resolve({
              cfg,
              agentId: turn.agentId,
              accountId: "primary",
              target: "agent:bob",
            }),
          ),
        catch: (cause) => String(cause),
      });
      expect(send?.sessionKey).toBe(
        mode === "shared" ? turn.mainSessionKey : turn.sessionKey,
      );
    }).pipe(Effect.runPromise),
);

unit.each([
  { value: undefined, requests: 0 },
  { value: "false", requests: 0 },
  { value: "true", requests: 1 },
  { value: "invalid", requests: 0 },
])(
  "uses the host HTTP permission $value before connecting MCP",
  ({ value, requests }) =>
    Effect.gen(function* () {
      vi.stubEnv("SHARNESS_MCP_URL", "http://internal.example/mcp");
      vi.stubEnv("SHARNESS_MCP_CREDENTIAL_FILE", openClawTestMcpCredentialFile);
      vi.stubEnv("SHARNESS_MCP_ALLOW_HTTP", value);
      const fetch = vi
        .spyOn(globalThis, "fetch")
        .mockResolvedValue(new Response(null, { status: 401 }));
      const start = yield* Effect.fromNullable(
        createChannelPlugin().gateway?.startAccount,
      );
      yield* Effect.tryPromise({
        try: () =>
          Promise.resolve(
            start({
              cfg: {
                channels: { sharness: { accounts: [{ id: "primary" }] } },
              },
              accountId: "primary",
              account: { id: "primary" },
              abortSignal: new AbortController().signal,
              runtime: { log: () => {}, error: () => {}, exit: () => {} },
              channelRuntime,
              getStatus: () => ({ accountId: "primary" }),
              setStatus: () => {},
            }),
          ),
        catch: (cause) => String(cause),
      }).pipe(Effect.flip);
      expect(fetch).toHaveBeenCalledTimes(requests);
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
    // eslint-disable-next-line agent-code-guard/no-hardcoded-assertion-literals -- The observable startup failure names missing host runtime support before endpoint credentials are read.
    expect(failure).toContain("channel-runtime-unavailable");
  }));
