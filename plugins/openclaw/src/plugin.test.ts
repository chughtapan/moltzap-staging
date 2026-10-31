/** @file The channel advertises OpenClaw's standard explicitly addressed actions. */

import { live as it } from "@effect/vitest";
import { Effect } from "effect";
import { expect, it as unit } from "vitest";
import { createChannelPlugin } from "./plugin.js";

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
