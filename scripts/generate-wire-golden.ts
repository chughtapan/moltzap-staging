/**
 * @file Rebuild synthetic retained-wire fixtures with deterministic test-only keys.
 * Regenerate with pnpm nx run workspace:generate-wire-golden.
 */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { Effect, Encoding, Redacted, Schema } from "effect";
import { fileURLToPath } from "node:url";
import {
  AgentCardIssuedAt as agentCardIssuedAt,
  issueAgentCard,
} from "../packages/registry/dist/issuance.js";
import { AgentIdentityCard } from "../packages/social-harness/dist/identity/agent.js";
import {
  EnvelopeId,
  SignedMessage,
} from "../packages/social-harness/dist/identity/envelope/signed-message.js";
import {
  AgentId,
  AgentName,
  PrincipalId,
} from "../packages/social-harness/dist/identity/agent.js";
import {
  AgentSigningAuthority,
  AgentPublicKey,
} from "../packages/social-harness/dist/identity/keys.js";
import { WireVersions } from "../packages/social-harness/dist/identity/wire-version.js";

/** Public deterministic seeds belong only to synthetic golden fixtures. */
function testAuthority(byte: number) {
  const der = Buffer.concat([
    Buffer.from("302e020100300506032b657004220420", "hex"),
    Buffer.alloc(32, byte),
  ]);
  return AgentSigningAuthority.fromPkcs8(
    Redacted.make(
      `-----BEGIN PRIVATE KEY-----\n${der.toString("base64")}\n-----END PRIVATE KEY-----\n`,
    ),
  );
}

const identifier = (prefix: string, byte: number) =>
  `${prefix}${Encoding.encodeBase64Url(new Uint8Array(16).fill(byte))}`;
const retainedVersion = "2099.101.0";

const generate = Effect.gen(function* () {
  const registryAuthority = yield* testAuthority(1);
  const signingAuthority = yield* testAuthority(2);
  const agentCard = yield* issueAgentCard({
    agentId: Schema.decodeUnknownSync(AgentId)(identifier("agt_", 1)),
    principalId: Schema.decodeUnknownSync(PrincipalId)(identifier("prn_", 2)),
    agentName: Schema.decodeUnknownSync(AgentName)("http-signer"),
    publicKey: AgentSigningAuthority.publicKey(signingAuthority),
    issuedAt: Schema.decodeUnknownSync(agentCardIssuedAt)(
      "2026-07-30T12:00:00Z",
    ),
    registrySigningAuthority: registryAuthority,
  });
  const message = yield* SignedMessage.sign({
    agentCard,
    signingAuthority,
    recipientAgentIds: new Set([agentCard.agentId]),
    envelopeId: Schema.decodeUnknownSync(EnvelopeId)(identifier("env_", 2)),
    body: new TextEncoder().encode("retained golden evidence"),
  });
  return {
    provenance:
      "Synthetic signed retained-version fixture using fictional 2099.101.0; not captured from a published release. Regenerate: pnpm nx run workspace:generate-wire-golden.",
    registryKey: yield* Schema.encode(AgentPublicKey)(
      AgentSigningAuthority.publicKey(registryAuthority),
    ),
    card: yield* Schema.encode(AgentIdentityCard)(agentCard),
    message: yield* Schema.encode(SignedMessage)(message),
  };
}).pipe(
  Effect.provideService(WireVersions, {
    emitted: retainedVersion,
    live: [retainedVersion],
    history: [retainedVersion],
  }),
);

await Effect.runPromise(
  generate.pipe(
    Effect.flatMap((golden) =>
      FileSystem.FileSystem.pipe(
        Effect.flatMap((fileSystem) =>
          fileSystem.writeFileString(
            fileURLToPath(
              new URL(
                "../packages/social-harness/src/identity/envelope/__tests__/wire-golden.json",
                import.meta.url,
              ),
            ),
            `${JSON.stringify(golden, null, 2)}\n`,
          ),
        ),
      ),
    ),
    Effect.provide(NodeFileSystem.layer),
  ),
);
