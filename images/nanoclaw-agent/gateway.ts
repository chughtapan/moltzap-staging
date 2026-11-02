/** @file Exposes NanoClaw's CLI socket over TCP for the principal's CLI channel. */
import { createConnection, createServer, type Socket } from "node:net";

/** The TCP address the host's CLI socket is served on. */
export interface GatewayAddress {
  readonly host: string;
  readonly port: number;
}

/** A listening gateway. */
export interface Gateway {
  /** The port the gateway listens on, which the system picks for port 0. */
  readonly port: number;
  /**
   * Closes the listener and all connections, including idle clients that would
   * otherwise prevent NanoClaw from shutting down.
   */
  close(): undefined;
}

/**
 * Resolves once listening. Each client gets its own CLI socket connection.
 * A client close reaches that socket only after its buffered writes drain.
 */
export function startGateway(
  address: GatewayAddress,
  socketPath: string,
): Promise<Gateway> {
  const open = new Set<Socket>();
  const track = (socket: Socket): void => {
    open.add(socket);
    socket.once("close", () => {
      open.delete(socket);
    });
  };
  const server = createServer((client) => {
    const native = createConnection(socketPath);
    track(client);
    track(native);
    client.pipe(native);
    native.pipe(client);
    const destroy = (): void => {
      client.destroy();
      native.destroy();
    };
    client.on("error", destroy);
    native.on("error", destroy);
  });
  const close = (): undefined => {
    server.close();
    for (const socket of open) {
      socket.destroy();
    }
  };
  return new Promise((resolveGateway, reject) => {
    server.once("error", reject);
    server.listen(address.port, address.host, () => {
      const bound = server.address();
      if (bound === null || typeof bound === "string") {
        reject(new Error("the NanoClaw gateway is not listening on TCP"));
        return;
      }
      resolveGateway({ port: bound.port, close });
    });
  });
}
