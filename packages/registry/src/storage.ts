/** @file Durable Registry persistence for admission, immutable cards, and idempotent operations. */

import type { SqlError } from "@effect/sql/SqlError";
import { SqlClient } from "@effect/sql/SqlClient";
import {
  AgentId,
  AgentIdentityCard,
  type AgentPublicKey,
  type AgentSigningAuthority,
  type VerifiedAgentIdentityCard,
} from "@moltzap/social-harness";
import {
  agentIdentityCardSigning,
  httpErrors,
  registryContract,
  wireVersions,
} from "@moltzap/social-harness/internal";
import { Clock, Data, Effect, Schema } from "effect";
import { randomBytes } from "node:crypto";

/* eslint-disable @typescript-eslint/naming-convention -- Closed HTTP error constructors retain their class names when bound from the service capability. */
const { InternalServerError, UnavailableError } = httpErrors;
/* eslint-enable @typescript-eslint/naming-convention -- Restore variable naming after binding error constructors. */
const ed25519PublicKeyThumbprintUri = agentIdentityCardSigning.publicKeyId;

/** Storage initialization failed before the Registry became ready. */
export class RegistryStorageInitializationError extends Data.TaggedError(
  "RegistryStorageInitializationError",
)<{
  readonly reason: "incompatible" | "signer-mismatch" | "unavailable";
}> {}

/** Created atomically with the format marker and signer binding in an empty namespace. */
const createRegistrySchema = Effect.gen(function* () {
  const sql = yield* SqlClient;
  yield* sql.unsafe(`
    CREATE TABLE sharness_registry_metadata (
      singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
      signer_thumbprint text NOT NULL
    );
    CREATE TABLE sharness_registry_agents (
      agent_id text PRIMARY KEY,
      principal_id text NOT NULL,
      agent_name text NOT NULL UNIQUE,
      public_key_thumbprint text NOT NULL UNIQUE,
      agent_card_bytes bytea NOT NULL
    );
    CREATE TABLE sharness_registry_operations (
      public_key_thumbprint text NOT NULL,
      operation_id text NOT NULL,
      request_bytes bytea NOT NULL,
      result_bytes bytea NOT NULL,
      PRIMARY KEY (public_key_thumbprint, operation_id)
    );
    CREATE TABLE sharness_registry_nonces (
      nonce text PRIMARY KEY,
      expires_at bigint NOT NULL
    );
    CREATE INDEX sharness_registry_nonces_expiry
      ON sharness_registry_nonces (expires_at);
  `);
}).pipe(Effect.withSpan("createRegistrySchema"));

const LEDGER_TABLE = "sharness_registry_ledger_20261029_3";

type StorageError =
  | InstanceType<typeof httpErrors.UnavailableError>
  | InstanceType<typeof httpErrors.InternalServerError>;
type StorageInput = Readonly<{
  sql: SqlClient;
  registrySignerPublicKey: AgentPublicKey;
  operationTimeoutMilliseconds: number;
}>;
type RegisterInput = Readonly<{
  request: typeof registryContract.RegistryRegisterRequest.Type;
  requestBytes: Uint8Array;
  registrySigningAuthority: AgentSigningAuthority;
}>;
type StoredOperation = Readonly<{
  requestBytes: Uint8Array;
  resultBytes: Uint8Array;
}>;

/** Durable operations needed by Registry admission and request handling. */
export interface RegistryStorage {
  readonly checkReadiness: () => Effect.Effect<void, StorageError>;
  readonly claimRegistrationNonce: (input: {
    readonly nonce: string;
    readonly expires: number;
    readonly now: number;
    readonly capacity: number;
  }) => Effect.Effect<"claimed" | "replayed" | "full", StorageError>;
  readonly register: (
    input: RegisterInput,
  ) => Effect.Effect<
    typeof registryContract.registerResultSchema.Type,
    StorageError
  >;
  readonly lookup: (
    request: typeof registryContract.RegistryLookupRequest.Type,
  ) => Effect.Effect<
    typeof registryContract.lookupResultSchema.Type,
    StorageError
  >;
}

/**
 * Requires exclusive use of the SQL client during initialization. An occupied
 * namespace must have the current format and the configured Registry signer.
 */
export function initializeRegistryStorage(input: {
  readonly sql: SqlClient;
  readonly registrySignerPublicKey: AgentPublicKey;
}): Effect.Effect<void, RegistryStorageInitializationError> {
  return runStorageInitialization(input).pipe(
    Effect.mapError((error) =>
      error instanceof RegistryStorageInitializationError
        ? error
        : new RegistryStorageInitializationError({ reason: "unavailable" }),
    ),
    Effect.catchAllDefect(() =>
      Effect.fail(
        new RegistryStorageInitializationError({ reason: "unavailable" }),
      ),
    ),
    Effect.withSpan("initializeRegistryStorage"),
  );
}

/**
 * Requires an initialized SQL client and its pinned Registry verification key.
 * The configured deadline covers each complete storage operation.
 */
export function makeRegistryStorage(input: StorageInput): RegistryStorage {
  return Object.freeze({
    checkReadiness: makeCheckReadiness(input),
    claimRegistrationNonce: makeClaimRegistrationNonce(input),
    register: makeRegister(input),
    lookup: makeLookup(input),
  });
}

// eslint-disable-next-line @typescript-eslint/unbound-method -- Capturing the intrinsic prevents candidate arrays from replacing the comparison method.
const everyByte = Uint8Array.prototype.every;

/** Compares complete byte sequences, rejecting detached or exotic typed arrays. */
const sameBytes = (left: Uint8Array, right: Uint8Array): boolean => {
  try {
    return (
      everyByte.call(left, (byte, index) => byte === right[index]) &&
      everyByte.call(right, (byte, index) => byte === left[index])
    );
    // eslint-disable-next-line agent-code-guard/bare-catch -- Detached or exotic typed arrays are non-canonical inputs, not defects.
  } catch {
    return false;
  }
};

/** Rejects excess members and absent required fields at identity boundaries. */
const exactStruct = <Fields extends Schema.Struct.Fields>(fields: Fields) =>
  Schema.Struct(fields).annotations({
    parseOptions: { exact: true, onExcessProperty: "error" },
  });

const AGENT_ID_MINT_ATTEMPTS = 8;
const metadataRow = exactStruct({
  signerThumbprint: Schema.String,
});
const ledgerRow = exactStruct({
  migrationId: Schema.Literal(1),
});
const schemaStateRow = exactStruct({
  currentLedger: Schema.Boolean,
  occupied: Schema.Boolean,
});
const cardRow = exactStruct({
  agentCardBytes: Schema.Uint8ArrayFromSelf,
});
const operationRow = exactStruct({
  requestBytes: Schema.Uint8ArrayFromSelf,
  resultBytes: Schema.Uint8ArrayFromSelf,
});
const countRow = exactStruct({
  retained: Schema.Union(Schema.String, Schema.Number),
});
const readinessRow = exactStruct({
  ready: Schema.Literal(1),
});

const internalFailure = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, InstanceType<typeof httpErrors.InternalServerError>, R> =>
  effect.pipe(
    // eslint-disable-next-line agent-code-guard/no-effect-error-coalescing -- Stored bytes and internal cryptographic failures cross one closed, non-diagnostic storage boundary.
    Effect.mapError(() => new InternalServerError()),
  );

const exactRows = <A, I>(
  schema: Schema.Schema<A, I>,
  rows: unknown,
): Effect.Effect<
  readonly A[],
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  internalFailure(
    Schema.decodeUnknown(Schema.Array(schema))(rows, {
      exact: true,
      onExcessProperty: "error",
    }),
  ).pipe(Effect.map((decoded) => Object.freeze([...decoded])));

const withStorageDeadline = <A>(
  effect: Effect.Effect<
    A,
    InstanceType<typeof httpErrors.InternalServerError> | SqlError
  >,
  timeoutMilliseconds: number,
): Effect.Effect<A, StorageError> =>
  effect.pipe(
    Effect.catchTag("SqlError", () => Effect.fail(new UnavailableError())),
    Effect.timeoutFail({
      duration: timeoutMilliseconds,
      onTimeout: () => new UnavailableError(),
    }),
  );

const decodeStoredCard = (
  bytes: Uint8Array,
  registrySignerPublicKey: AgentPublicKey,
): Effect.Effect<
  VerifiedAgentIdentityCard,
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  internalFailure(
    registryContract.registryIdentityCardCodec.decode(bytes).pipe(
      Effect.flatMap((agentCard) =>
        AgentIdentityCard.verify({
          identityCard: agentCard,
          registrySignerPublicKey,
        }),
      ),
    ),
  );

const encodeResult = (
  result: typeof registryContract.registerResultSchema.Type,
): Effect.Effect<
  Uint8Array,
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  internalFailure(
    registryContract.registryRegisterCodec.response.encode(result),
  );

const decodeResult = (
  bytes: Uint8Array,
  registrySignerPublicKey: AgentPublicKey,
): Effect.Effect<
  typeof registryContract.registerResultSchema.Type,
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  internalFailure(
    Effect.gen(function* () {
      const result =
        yield* registryContract.registryRegisterCodec.response.decode(bytes);
      if (result.kind !== "registered") {
        return Object.freeze({ kind: result.kind });
      }
      const agentCard = yield* AgentIdentityCard.verify({
        identityCard: result.agentCard,
        registrySignerPublicKey,
      });
      return Object.freeze({ kind: "registered", agentCard });
    }),
  );

const mintAgentId = (): Effect.Effect<
  AgentId,
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  internalFailure(
    Effect.try({
      try: () => `agt_${randomBytes(16).toString("base64url")}`,
      catch: () => new InternalServerError(),
    }).pipe(Effect.flatMap(Schema.decodeUnknown(AgentId))),
  );

const retainedCount = (
  rows: unknown,
): Effect.Effect<number, InstanceType<typeof httpErrors.InternalServerError>> =>
  exactRows(countRow, rows).pipe(
    Effect.map((decoded) => Number(decoded[0]?.retained ?? 0)),
  );

const mintUnusedAgentId = (
  sql: SqlClient,
): Effect.Effect<
  AgentId,
  SqlError | InstanceType<typeof httpErrors.InternalServerError>
> =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < AGENT_ID_MINT_ATTEMPTS; attempt += 1) {
      const candidate = yield* mintAgentId();
      const rows = yield* sql`
        SELECT COUNT(*)::text AS retained
        FROM sharness_registry_agents
        WHERE agent_id = ${candidate}
      `;
      if ((yield* retainedCount(rows)) === 0) {
        return candidate;
      }
    }
    return yield* new InternalServerError();
  });

const issueTime = (): Effect.Effect<
  typeof agentIdentityCardSigning.issuedAt.Type,
  InstanceType<typeof httpErrors.InternalServerError>
> =>
  internalFailure(
    Clock.currentTimeMillis.pipe(
      Effect.map((milliseconds) =>
        new Date(Math.floor(milliseconds / 1_000) * 1_000)
          .toISOString()
          .replace(".000Z", "Z"),
      ),
      Effect.flatMap(Schema.decodeUnknown(agentIdentityCardSigning.issuedAt)),
    ),
  );

function makeCheckReadiness(
  input: StorageInput,
): RegistryStorage["checkReadiness"] {
  return () =>
    withStorageDeadline(
      input.sql`SELECT 1 AS ready`.pipe(
        Effect.flatMap((rows) => exactRows(readinessRow, rows)),
        Effect.flatMap((rows) =>
          rows.length === 1
            ? Effect.void
            : Effect.fail(new InternalServerError()),
        ),
      ),
      input.operationTimeoutMilliseconds,
    );
}

/** The format marker commits with the current schema in the initialization transaction. */
const createFormatMarker = (sql: SqlClient) =>
  sql`
    CREATE TABLE ${sql(LEDGER_TABLE)} (
      migration_id integer PRIMARY KEY,
      created_at timestamp with time zone NOT NULL DEFAULT now(),
      name text NOT NULL
    )
  `;

/** An initialized schema has exactly one current-format marker. */
function requireCurrentFormat(sql: SqlClient) {
  return Effect.gen(function* () {
    yield* sql`LOCK TABLE ${sql(LEDGER_TABLE)} IN ACCESS EXCLUSIVE MODE`;
    const rows = yield* sql`
      SELECT migration_id AS "migrationId" FROM ${sql(LEDGER_TABLE)}
    `;
    yield* Schema.decodeUnknown(Schema.Tuple(ledgerRow))(rows).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(
          new RegistryStorageInitializationError({ reason: "incompatible" }),
        ),
      ),
    );
  });
}

const bindStorageMetadata = (input: {
  readonly sql: SqlClient;
  readonly signerThumbprint: string;
}) =>
  input.sql.withTransaction(
    Effect.gen(function* () {
      yield* input.sql`
        INSERT INTO sharness_registry_metadata
          (singleton, signer_thumbprint)
        VALUES (true, ${input.signerThumbprint})
        ON CONFLICT (singleton) DO NOTHING
      `;
      const rows = yield* input.sql`
        SELECT
          signer_thumbprint AS "signerThumbprint"
        FROM sharness_registry_metadata
        WHERE singleton = true
        FOR UPDATE
      `;
      const metadata = yield* Schema.decodeUnknown(Schema.Tuple(metadataRow))(
        rows,
        { exact: true, onExcessProperty: "error" },
      );
      if (metadata[0].signerThumbprint !== input.signerThumbprint) {
        return yield* new RegistryStorageInitializationError({
          reason: "signer-mismatch",
        });
      }
    }),
  );

/** Only an empty namespace can initialize. An existing namespace must carry the current format. */
function runStorageInitialization(input: {
  readonly sql: SqlClient;
  readonly registrySignerPublicKey: AgentPublicKey;
}) {
  return Effect.gen(function* () {
    const rows = yield* input.sql`
      SELECT
        COALESCE(bool_or(relname = ${LEDGER_TABLE} AND relkind = 'r'), false) AS "currentLedger",
        count(*) > 0 AS occupied
      FROM pg_catalog.pg_class
      WHERE relnamespace = (
        SELECT oid FROM pg_catalog.pg_namespace
        WHERE nspname = current_schema()
      )
    `;
    const [schema] = yield* Schema.decodeUnknown(Schema.Tuple(schemaStateRow))(
      rows,
    );
    if (schema.occupied && !schema.currentLedger) {
      return yield* new RegistryStorageInitializationError({
        reason: "incompatible",
      });
    }
    const signerThumbprint = yield* ed25519PublicKeyThumbprintUri(
      input.registrySignerPublicKey,
    );
    yield* input.sql.withTransaction(
      Effect.gen(function* () {
        if (schema.occupied) {
          yield* requireCurrentFormat(input.sql);
        } else {
          yield* createRegistrySchema.pipe(
            Effect.provideService(SqlClient, input.sql),
          );
          yield* createFormatMarker(input.sql);
          yield* input.sql`
            INSERT INTO ${input.sql(LEDGER_TABLE)} (migration_id, name)
            VALUES (1, 'registry')
          `;
        }
        yield* bindStorageMetadata({ sql: input.sql, signerThumbprint });
      }),
    );
  });
}

const claimNonceTransaction = (
  sql: SqlClient,
  claim: Parameters<RegistryStorage["claimRegistrationNonce"]>[0],
) =>
  sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        SELECT singleton FROM sharness_registry_metadata
        WHERE singleton = true FOR UPDATE
      `;
      yield* sql`
        DELETE FROM sharness_registry_nonces
        WHERE expires_at < ${claim.now}
      `;
      const existing = yield* sql`
        SELECT COUNT(*)::text AS retained
        FROM sharness_registry_nonces WHERE nonce = ${claim.nonce}
      `;
      if ((yield* retainedCount(existing)) !== 0) {
        return "replayed" as const;
      }
      const retained = yield* sql`
        SELECT COUNT(*)::text AS retained FROM sharness_registry_nonces
      `;
      if ((yield* retainedCount(retained)) >= claim.capacity) {
        return "full" as const;
      }
      yield* sql`
        INSERT INTO sharness_registry_nonces (nonce, expires_at)
        VALUES (${claim.nonce}, ${claim.expires})
      `;
      return "claimed" as const;
    }),
  );

function makeClaimRegistrationNonce(
  input: StorageInput,
): RegistryStorage["claimRegistrationNonce"] {
  return (claim) =>
    withStorageDeadline(
      claimNonceTransaction(input.sql, claim),
      input.operationTimeoutMilliseconds,
    );
}

const findStoredOperation = (
  sql: SqlClient,
  keyThumbprint: string,
  operationId: (typeof registryContract.RegistryRegisterRequest.Type)["operationId"],
): Effect.Effect<
  StoredOperation | undefined,
  SqlError | InstanceType<typeof httpErrors.InternalServerError>
> =>
  sql`
    SELECT
      request_bytes AS "requestBytes",
      result_bytes AS "resultBytes"
    FROM sharness_registry_operations
    WHERE public_key_thumbprint = ${keyThumbprint}
      AND operation_id = ${operationId}
  `.pipe(
    Effect.flatMap((rows) => exactRows(operationRow, rows)),
    Effect.map((rows) => rows[0]),
  );

const keyIsRegistered = (sql: SqlClient, thumbprint: string) =>
  sql`
    SELECT COUNT(*)::text AS retained
    FROM sharness_registry_agents
    WHERE public_key_thumbprint = ${thumbprint}
  `.pipe(
    Effect.flatMap(retainedCount),
    Effect.map((retained) => retained > 0),
  );

const nameIsRegistered = (
  sql: SqlClient,
  agentName: (typeof registryContract.RegistryRegisterRequest.Type)["agentName"],
) =>
  sql`
    SELECT COUNT(*)::text AS retained
    FROM sharness_registry_agents
    WHERE agent_name = ${agentName}
  `.pipe(
    Effect.flatMap(retainedCount),
    Effect.map((retained) => retained > 0),
  );

const insertNewAgent = (
  input: StorageInput,
  call: RegisterInput,
  keyThumbprint: string,
): Effect.Effect<
  typeof registryContract.registerResultSchema.Type,
  SqlError | InstanceType<typeof httpErrors.InternalServerError>
> =>
  Effect.gen(function* () {
    const agentId = yield* mintUnusedAgentId(input.sql);
    const issuedAt = yield* issueTime();
    const agentCard = yield* internalFailure(
      agentIdentityCardSigning.sign(
        {
          kind: "agentCard",
          sharnessVersion: yield* wireVersions.emitted,
          agentId,
          principalId: call.request.principalId,
          agentName: call.request.agentName,
          publicKey: call.request.publicKey,
          issuedAt,
        },
        call.registrySigningAuthority,
      ),
    );
    const agentCardBytes = yield* internalFailure(
      registryContract.registryIdentityCardCodec.encode(agentCard),
    );
    yield* input.sql`
      INSERT INTO sharness_registry_agents
        (agent_id, principal_id, agent_name,
         public_key_thumbprint, agent_card_bytes)
      VALUES
        (${agentId}, ${call.request.principalId},
         ${call.request.agentName}, ${keyThumbprint}, ${agentCardBytes})
    `;
    return Object.freeze({ kind: "registered", agentCard });
  });

const decideRegistration = (
  input: StorageInput,
  call: RegisterInput,
  keyThumbprint: string,
) =>
  Effect.gen(function* () {
    if (yield* keyIsRegistered(input.sql, keyThumbprint)) {
      return Object.freeze({ kind: "key_already_registered" } as const);
    }
    if (yield* nameIsRegistered(input.sql, call.request.agentName)) {
      return Object.freeze({ kind: "name_taken" } as const);
    }
    return yield* insertNewAgent(input, call, keyThumbprint);
  });

const recordOperation = (input: {
  readonly sql: SqlClient;
  readonly keyThumbprint: string;
  readonly operationId: (typeof registryContract.RegistryRegisterRequest.Type)["operationId"];
  readonly requestBytes: Uint8Array;
  readonly resultBytes: Uint8Array;
}) =>
  input.sql`
    INSERT INTO sharness_registry_operations
      (public_key_thumbprint, operation_id, request_bytes, result_bytes)
    VALUES
      (${input.keyThumbprint}, ${input.operationId},
       ${input.requestBytes}, ${input.resultBytes})
  `;

const registerTransaction = (
  input: StorageInput,
  call: RegisterInput,
  keyThumbprint: string,
) =>
  input.sql.withTransaction(
    Effect.gen(function* () {
      yield* input.sql`
        SELECT singleton FROM sharness_registry_metadata
        WHERE singleton = true FOR UPDATE
      `;
      const previous = yield* findStoredOperation(
        input.sql,
        keyThumbprint,
        call.request.operationId,
      );
      if (previous !== undefined) {
        return sameBytes(previous.requestBytes, call.requestBytes)
          ? yield* decodeResult(
              previous.resultBytes,
              input.registrySignerPublicKey,
            )
          : Object.freeze({ kind: "idempotency_conflict" } as const);
      }
      const result = yield* decideRegistration(input, call, keyThumbprint);
      const resultBytes = yield* encodeResult(result);
      yield* recordOperation({
        sql: input.sql,
        keyThumbprint,
        operationId: call.request.operationId,
        requestBytes: call.requestBytes,
        resultBytes,
      });
      return result;
    }),
  );

function makeRegister(input: StorageInput): RegistryStorage["register"] {
  return (call) =>
    withStorageDeadline(
      internalFailure(
        ed25519PublicKeyThumbprintUri(call.request.publicKey),
      ).pipe(
        Effect.flatMap((thumbprint) =>
          registerTransaction(input, call, thumbprint),
        ),
      ),
      input.operationTimeoutMilliseconds,
    );
}

const findCardRows = (
  sql: SqlClient,
  request: typeof registryContract.RegistryLookupRequest.Type,
) =>
  "agentId" in request
    ? sql`
        SELECT agent_card_bytes AS "agentCardBytes"
        FROM sharness_registry_agents
        WHERE agent_id = ${request.agentId}
      `
    : sql`
        SELECT agent_card_bytes AS "agentCardBytes"
        FROM sharness_registry_agents
        WHERE agent_name = ${request.agentName}
      `;

function makeLookup(input: StorageInput): RegistryStorage["lookup"] {
  return (request) =>
    withStorageDeadline(
      Effect.gen(function* () {
        const cards = yield* findCardRows(input.sql, request).pipe(
          Effect.flatMap((rows) => exactRows(cardRow, rows)),
        );
        const card = cards[0];
        if (card === undefined) {
          return Object.freeze({ kind: "not_found" } as const);
        }
        if (cards.length !== 1) {
          return yield* new InternalServerError();
        }
        const agentCard = yield* decodeStoredCard(
          card.agentCardBytes,
          input.registrySignerPublicKey,
        );
        return Object.freeze({ kind: "found", agentCard } as const);
      }),
      input.operationTimeoutMilliseconds,
    );
}
