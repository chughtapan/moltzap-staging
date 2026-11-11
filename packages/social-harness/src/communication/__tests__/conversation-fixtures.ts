/** @file Three agents with identity cards from one signer, and a database they share. */

import { Effect, Schema } from "effect";
import { generateKeyPairSync } from "node:crypto";
import { issueTestCard, makeTestAuthority } from "../../__tests__/agents.js";
import { AgentPublicKey } from "../../identity/keys.js";
import { openTestDatabase } from "./store-fixtures.js";

/** Alice is initialized in the database; Bob and Carol are ready to join it. */
export const conversationIdentityFixture = Effect.gen(function* () {
  const registryKeys = generateKeyPairSync("ed25519");
  const identity = (byte: number, name: string) =>
    Effect.gen(function* () {
      const authority = yield* makeTestAuthority();
      return {
        authority,
        card: yield* issueTestCard({ byte, name, authority, registryKeys }),
      };
    });
  const local = yield* identity(1, "alice");
  const remote = yield* identity(2, "bob");
  const third = yield* identity(3, "carol");
  const signer = yield* Schema.decodeUnknown(AgentPublicKey)(
    registryKeys.publicKey.export({ format: "jwk" }),
  );
  const { sql, run, store, directory } = yield* openTestDatabase();
  yield* run(store.initializeCommunication(local.card));
  return { local, remote, third, agentKey: directory, sql, signer };
}).pipe(Effect.withSpan("conversationIdentityFixture"));
