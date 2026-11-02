/**
 * @file Exercises the NanoClaw image's TCP gateway with real TCP clients and
 * a stand-in for NanoClaw's CLI socket on a temporary Unix socket.
 */
import assert from "node:assert/strict";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { createConnection, createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import { startGateway } from "./gateway.ts";

/** How long the stand-in CLI socket waits before it reads a connection. */
const READ_DELAY_MILLIS = 200;
/**
 * The size of each client's line, in bytes: 8 KiB apart from 128 KiB to
 * 640 KiB. The gateway still holds part of a line when its client closes
 * if the line just overflows what the system buffers on the CLI socket
 * connection, wherever that limit falls in the range.
 */
const LINE_BYTES = [...Array.from({ length: 65 }).keys()].map(
  (index) => (128 + index * 8) * 1024,
);
/** How long the clients' lines may take to arrive before the test compares. */
const DELIVERY_TIMEOUT_MILLIS = 10_000;
const LOOPBACK = "127.0.0.1";

test("every client that writes a line and closes at once is delivered whole while NanoClaw reads slowly", async (t) => {
  const receivedBytes: number[] = [];
  const allReceived = Promise.withResolvers<undefined>();
  const socketPath = await cliSocket(t, async (connection) => {
    await delay(READ_DELAY_MILLIS);
    let bytes = 0;
    connection.on("data", (chunk: Buffer) => {
      bytes += chunk.length;
    });
    connection.on("end", () => {
      receivedBytes.push(bytes);
      if (receivedBytes.length === LINE_BYTES.length) {
        allReceived.resolve(undefined);
      }
    });
  });
  const gateway = await startGateway({ host: LOOPBACK, port: 0 }, socketPath);
  t.after(() => {
    gateway.close();
  });

  const clients = LINE_BYTES.map((bytes) => {
    const client = createConnection(gateway.port, LOOPBACK, () => {
      client.end(`${"x".repeat(bytes - 1)}\n`);
    });
    return client;
  });
  t.after(() => {
    for (const client of clients) {
      client.destroy();
    }
  });
  await Promise.race([
    allReceived.promise,
    delay(DELIVERY_TIMEOUT_MILLIS, undefined, { ref: false }),
  ]);

  assert.deepEqual(receivedBytes.toSorted(byNumber), LINE_BYTES);
});

test(
  "closing the gateway ends a connection that never sent a line",
  { timeout: 10_000 },
  async (t) => {
    const accepted = Promise.withResolvers<Socket>();
    const socketPath = await cliSocket(t, (connection) => {
      connection.resume();
      accepted.resolve(connection);
    });
    const gateway = await startGateway({ host: LOOPBACK, port: 0 }, socketPath);
    const client = createConnection(gateway.port, LOOPBACK);
    t.after(() => client.destroy());
    const connection = await accepted.promise;
    const closed = once(connection, "close");

    gateway.close();

    await closed;
  },
);

/**
 * Listen on a Unix socket in a temporary directory, as NanoClaw's CLI
 * channel does, and return its path.
 */
async function cliSocket(
  t: TestContext,
  onConnection: (connection: Socket) => unknown,
): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), "social-harness-gateway-"));
  const socketPath = join(root, "cli.sock");
  const server = createServer((connection) => {
    onConnection(connection);
  });
  server.listen(socketPath);
  await once(server, "listening");
  t.after(async () => {
    server.close();
    await rm(root, { recursive: true, force: true });
  });
  return socketPath;
}

function byNumber(left: number, right: number): number {
  return left - right;
}
