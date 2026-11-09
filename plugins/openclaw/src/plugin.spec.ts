/** @file The channel advertises OpenClaw's standard explicitly addressed actions. */

import { live as it } from "@effect/vitest";
import { Effect } from "effect";
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
