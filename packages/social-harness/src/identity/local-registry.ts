/**
 * @file A Registry kept in the daemon's own database.
 *
 * A Registry gives each agent a unique name and a signed identity card. When
 * every agent belongs to one daemon, that daemon can do this itself and needs
 * no separate Registry service. It signs cards with a key it generates on
 * first use and stores sealed with the database key.
 */
import { SqlClient } from "@effect/sql";
import { Clock, type Context, Effect, Layer, Redacted, Schema } from "effect";
import { generateKeyPairSync, randomBytes } from "node:crypto";
import { openCredential, sealCredential } from "../database/secrets.js";
import { wireVersions } from "../version.js";
import {
  AgentId,
  AgentIdentityCard,
  agentIdentityCardSigning,
  type AgentName,
  type VerifiedAgentIdentityCard,
} from "./agent-cards.js";
import {
  type AgentPublicKey,
  AgentSigningAuthority,
  ed25519PublicKeyThumbprintUri,
} from "./keys.js";
import { Registry, registryContract, RegistryError } from "./registry.js";

const SIGNER_RECORD = "local-registry/signer";

const cardRow = Schema.Struct({ canonicalCard: Schema.Uint8ArrayFromSelf });

const unavailable = () => new RegistryError({ reason: "connection" });
const unreadable = () => new RegistryError({ reason: "invalid-response" });
const unsigned = () => new RegistryError({ reason: "signing" });

/** The key this daemon signs identity cards with, generated the first time it is needed. */
const signingAuthority = (unlockKey: Redacted.Redacted<Uint8Array>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const generated = generateKeyPairSync("ed25519").privateKey.export({
      format: "pem",
      type: "pkcs8",
    });
    const sealed = yield* sealCredential(
      Redacted.make(generated),
      SIGNER_RECORD,
      unlockKey,
    );
    yield* sql`INSERT INTO local_registry_signer(singleton,private_key) VALUES(1,${sealed}) ON CONFLICT(singleton) DO NOTHING`;
    const [saved] = yield* sql<{
      readonly privateKey: string;
    }>`SELECT private_key AS "privateKey" FROM local_registry_signer WHERE singleton=1`;
    if (saved === undefined) {
      return yield* unavailable();
    }
    return yield* openCredential(
      saved.privateKey,
      SIGNER_RECORD,
      unlockKey,
    ).pipe(Effect.flatMap(AgentSigningAuthority.fromPkcs8));
  });

/** A stored card, checked against this Registry's signing key. */
const storedCard = (
  rows: readonly unknown[],
  registrySignerPublicKey: AgentPublicKey,
) =>
  Effect.gen(function* () {
    const [row] = yield* Schema.decodeUnknown(Schema.Array(cardRow))(rows);
    if (row === undefined) {
      return undefined;
    }
    const identityCard =
      yield* registryContract.registryIdentityCardCodec.decode(
        row.canonicalCard,
      );
    return yield* AgentIdentityCard.verify({
      identityCard,
      registrySignerPublicKey,
    });
  }).pipe(Effect.mapError(unreadable));

type RegisterCall = Parameters<
  Context.Tag.Service<typeof Registry>["register"]
>[0];

/** What every operation of one open Registry works with. */
interface Store {
  readonly sql: SqlClient.SqlClient;
  readonly authority: AgentSigningAuthority;
  readonly registrySignerPublicKey: AgentPublicKey;
}

/** Signs and stores the identity card of a new agent. */
const admit = (
  store: Store,
  request: RegisterCall["request"],
  key: { readonly publicKey: AgentPublicKey; readonly thumbprint: string },
) =>
  Effect.gen(function* () {
    const agentId = yield* Schema.decodeUnknown(AgentId)(
      `agt_${randomBytes(16).toString("base64url")}`,
    );
    const now = yield* Clock.currentTimeMillis;
    const issuedAt = yield* Schema.decodeUnknown(
      agentIdentityCardSigning.issuedAt,
    )(
      new Date(Math.floor(now / 1000) * 1000)
        .toISOString()
        .replace(".000Z", "Z"),
    );
    const identityCard = yield* agentIdentityCardSigning.sign(
      {
        kind: "agentCard",
        sharnessVersion: yield* wireVersions.emitted,
        agentId,
        principalId: request.principalId,
        agentName: request.agentName,
        publicKey: key.publicKey,
        issuedAt,
      },
      store.authority,
    );
    const canonicalCard =
      yield* registryContract.registryIdentityCardCodec.encode(identityCard);
    yield* store.sql`INSERT INTO local_registry_agents(agent_id,agent_name,key_thumbprint,canonical_card) VALUES(${agentId},${request.agentName},${key.thumbprint},${canonicalCard})`.pipe(
      Effect.mapError(unavailable),
    );
    return { kind: "registered", identityCard } as const;
  }).pipe(
    Effect.catchTags({
      ParseError: () => Effect.fail(unsigned()),
      CanonicalJsonError: () => Effect.fail(unsigned()),
      AgentIdentityCardVerificationError: () => Effect.fail(unsigned()),
    }),
  );

const cardsNamed = (sql: SqlClient.SqlClient, agentName: AgentName) =>
  sql`SELECT canonical_card AS "canonicalCard" FROM local_registry_agents WHERE agent_name=${agentName}`;

/**
 * Registering again with the same key, name and principal returns the saved
 * card. A key names one agent and a name belongs to one key.
 */
const register = (store: Store, call: RegisterCall) =>
  Effect.gen(function* () {
    const { sql } = store;
    const publicKey = AgentSigningAuthority.publicKey(call.signingAuthority);
    const thumbprint = yield* ed25519PublicKeyThumbprintUri(publicKey).pipe(
      Effect.mapError(unsigned),
    );
    const sameKey = yield* storedCard(
      yield* sql`SELECT canonical_card AS "canonicalCard" FROM local_registry_agents WHERE key_thumbprint=${thumbprint}`,
      store.registrySignerPublicKey,
    );
    if (sameKey !== undefined) {
      return sameKey.agentName === call.request.agentName &&
        sameKey.principalId === call.request.principalId
        ? ({ kind: "registered", identityCard: sameKey } as const)
        : ({ kind: "key_already_registered" } as const);
    }
    const sameName = yield* cardsNamed(sql, call.request.agentName);
    if (sameName.length > 0) {
      return { kind: "name_taken" } as const;
    }
    return yield* admit(store, call.request, { publicKey, thumbprint });
  }).pipe(
    store.sql.withTransaction,
    Effect.catchTag("SqlError", () => Effect.fail(unavailable())),
    Effect.withSpan("LocalRegistry.register"),
  );

const lookup = (
  store: Store,
  request: Parameters<Context.Tag.Service<typeof Registry>["lookup"]>[0],
) =>
  Effect.gen(function* () {
    const rows =
      "agentId" in request
        ? yield* store.sql`SELECT canonical_card AS "canonicalCard" FROM local_registry_agents WHERE agent_id=${request.agentId}`
        : yield* cardsNamed(store.sql, request.agentName);
    const identityCard: VerifiedAgentIdentityCard | undefined =
      yield* storedCard(rows, store.registrySignerPublicKey);
    return identityCard === undefined
      ? ({ kind: "not_found" } as const)
      : ({ kind: "found", identityCard } as const);
  }).pipe(
    Effect.catchTag("SqlError", () => Effect.fail(unavailable())),
    Effect.withSpan("LocalRegistry.lookup"),
  );

/**
 * Opens the daemon's own Registry. Returns the public half of its signing
 * key, which identity cards are checked against, and the Registry service.
 */
export const localRegistry = (unlockKey: Redacted.Redacted<Uint8Array>) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const authority = yield* signingAuthority(unlockKey);
    const registrySignerPublicKey = AgentSigningAuthority.publicKey(authority);
    const store = { sql, authority, registrySignerPublicKey };
    return {
      registrySignerPublicKey,
      layer: Layer.succeed(
        Registry,
        Registry.of({
          register: (call) => register(store, call),
          lookup: (request) => lookup(store, request),
        }),
      ),
    };
  }).pipe(Effect.withSpan("localRegistry"));
