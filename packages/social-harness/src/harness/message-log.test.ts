/** @file History export writes complete records and stops without interrupting delivery. */

import { FileSystem, Error as PlatformError } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { scopedLive as it } from "@effect/vitest";
import { DateTime, Effect, Schema } from "effect";
import { join } from "node:path";
import { expect } from "vitest";
import { testAgentView } from "../__tests__/agent-card-fixtures.js";
import { Message } from "../communication/index.js";
import { MessageAddressInput } from "../communication/records/values.js";
import { HistoryExportRecord, makeHistoryExport } from "./message-log.js";

const at = DateTime.unsafeMake("2026-09-01T12:00:00.000Z");
const decodeLine = Schema.decodeUnknownSync(
  Schema.parseJson(HistoryExportRecord),
);
const noSpace = "ENOSPC: no space left on device";

function records() {
  return Message.create({
    author: testAgentView("agent:bob"),
    recipients: [testAgentView("agent:alice")],
    content: [{ type: "text", text: "Hello" }],
  }).pipe(
    Effect.map((message): readonly HistoryExportRecord[] => [
      { kind: "inbound", item: { kind: "multicast", message }, at },
      {
        kind: "outbound",
        input: { to: MessageAddressInput.make("agent:bob"), text: "Hi" },
        outcome: { kind: "sent", msgIds: [message.msgId] },
        at,
      },
    ]),
  );
}

function decodeFile(text: string) {
  return text
    .trimEnd()
    .split("\n")
    .map((line) => decodeLine(line));
}

it("appends complete decodable records even when callers write concurrently", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({
      prefix: "sharness-message-log-",
    });
    const path = join(directory, "history.ndjson");
    const sink = yield* makeHistoryExport(path);
    const expected = yield* records();
    yield* Effect.forEach(expected, sink.record, {
      concurrency: 2,
    });
    const text = yield* fs.readFileString(path);
    expect(text.endsWith("\n")).toBe(true);
    expect(decodeFile(text)).toEqual(expect.arrayContaining([...expected]));
    expect(decodeFile(text)).toHaveLength(expected.length);
  }).pipe(Effect.provide(NodeFileSystem.layer)));

it("records one export failure and lets later delivery continue", () =>
  Effect.gen(function* () {
    const writes: string[] = [];
    let fail = true;
    const fs = FileSystem.layerNoop({
      writeFileString: (...[, data]) =>
        Effect.suspend(() => {
          if (fail) {
            fail = false;
            return Effect.fail(
              new PlatformError.SystemError({
                reason: "Unknown",
                module: "FileSystem",
                method: "writeFileString",
                description: noSpace,
              }),
            );
          }
          writes.push(data);
          return Effect.void;
        }),
    });
    const sink = yield* makeHistoryExport(
      "/run/social-harness/history.ndjson",
    ).pipe(Effect.provide(fs));
    for (const record of yield* records()) {
      yield* sink.record(record);
    }
    const failureReason: unknown = expect.stringContaining(noSpace);
    expect(decodeFile(writes.join(""))).toMatchObject([
      { kind: "export-failed", reason: failureReason },
    ]);
  }));
