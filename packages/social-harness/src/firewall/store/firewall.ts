/** @file Retained policy checks and editable rule installations. */

import * as SqlClient from "@effect/sql/SqlClient";
import * as SqlSchema from "@effect/sql/SqlSchema";
import { Effect, Schema, Struct } from "effect";

import type {
  FirewallActivation,
  FirewallAuditEntry,
  FirewallCommit,
  FirewallCompletion,
} from "./types.js";
import {
  EndpointStoreError,
  readBytes,
  readInteger,
  readText,
  requireBytes,
  requireSameBytes,
  requireText,
  storeOperation,
  transaction,
} from "../../communication/store/database/index.js";
import { decodeRuntimeValue } from "../../communication/store/index.js";
import { exactStruct } from "../../encoding/exact.js";
import { AgentView } from "../../identity/cards.js";
import {
  type FirewallCheck,
  firewallCheckSchema,
  Message,
  MessageContext,
} from "../check.js";
import {
  type firewallDecisionsSchema,
  firewallDecisionSummarySchema,
  firewallInstallationSchema,
} from "../config.js";
const ruleIds = Schema.parseJson(Schema.Array(Schema.NonEmptyString));
const activationRow = Schema.Struct({
  sequence: Schema.Int,
  time: Schema.Int,
  activeRevision: Schema.String,
  addedRuleIds: ruleIds,
  removedRuleIds: ruleIds,
});
const completedCheckSchema = exactStruct({
  result: firewallCheckSchema,
  summaries: Schema.Array(firewallDecisionSummarySchema),
});
const savedSummarySchema = Schema.Struct({
  summaries: completedCheckSchema.fields.summaries,
});
const auditDisposition = Schema.Literal("admitted", "withheld");
/** Missing policy means initial rules have never been seeded, distinct from an empty list. */
export function readFirewallPolicy(): Effect.Effect<
  Uint8Array | undefined,
  EndpointStoreError,
  SqlClient.SqlClient
> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    const row = (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT canonical_rules FROM firewall_policy WHERE singleton = 1",
      [],
    ))[0];
    return row === undefined ? undefined : readBytes(row, "canonical_rules");
  }).pipe(storeOperation, Effect.withSpan("readFirewallPolicy"));
}
/** The editable document and its activation either both commit or neither does. */
export function writeFirewallPolicy(
  rules: Uint8Array,
  activation?: FirewallActivation,
): Effect.Effect<void, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    requireBytes(rules);
    yield* transaction(
      Effect.gen(function* () {
        yield* database.unsafe<Readonly<Record<string, unknown>>>(
          `INSERT INTO firewall_policy (singleton, canonical_rules) VALUES (1, ?)
      ON CONFLICT(singleton) DO UPDATE SET canonical_rules = excluded.canonical_rules`,
          [rules],
        );
        if (activation !== undefined) {
          yield* database.unsafe<Readonly<Record<string, unknown>>>(
            `INSERT INTO firewall_activations
        (activated_at, active_revision, added_rule_ids, removed_rule_ids)
        VALUES (?, ?, ?, ?)`,
            [
              activation.time,
              activation.activeRevision,
              Schema.encodeSync(ruleIds)(activation.addedRuleIds),
              Schema.encodeSync(ruleIds)(activation.removedRuleIds),
            ],
          );
        }
      }),
    );
  }).pipe(storeOperation, Effect.withSpan("writeFirewallPolicy"));
}
/** Recent edits preserve activation order when timestamps coincide. */
export function readFirewallActivations(request: { readonly since?: number }) {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const since = request.since ?? 0;
    if (!Number.isSafeInteger(since) || since < 0) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    const query = SqlSchema.findAll({
      Request: Schema.Int,
      Result: activationRow,
      execute: (after) =>
        sql.unsafe(
          "SELECT sequence,activated_at AS time,active_revision AS activeRevision,added_rule_ids AS addedRuleIds,removed_rule_ids AS removedRuleIds FROM firewall_activations WHERE activated_at>=? ORDER BY activated_at DESC,sequence DESC LIMIT 100",
          [after],
        ),
    });
    const rows = yield* query(since).pipe(
      Effect.catchTag("ParseError", () =>
        Effect.fail(new EndpointStoreError({ reason: "corrupt" })),
      ),
    );
    return rows.map(({ sequence, ...activation }) => ({
      sequence,
      activation,
    }));
  }).pipe(storeOperation, Effect.withSpan("readFirewallActivations"));
}
/** Completion time includes checks that started before a caller's previous poll. */
export function readFirewallDecisions(since = 0) {
  return Effect.gen(function* () {
    const database = yield* SqlClient.SqlClient;
    if (!Number.isSafeInteger(since) || since < 0) {
      return yield* new EndpointStoreError({ reason: "invalid-input" });
    }
    return (yield* database.unsafe<Readonly<Record<string, unknown>>>(
      `SELECT rowid AS sequence, check_id, direction,
    canonical_candidate, canonical_input, canonical_result, disposition, decided_at
    FROM firewall_checks WHERE decided_at >= ?
    ORDER BY decided_at DESC, rowid DESC LIMIT 100`,
      [since],
    )).map((row) => ({
      ...auditEntry(row),
      time: readInteger(row, "decided_at"),
    }));
  }).pipe(storeOperation, Effect.withSpan("readFirewallDecisions"));
}
/** Saves one completed assessment; interruption before completion leaves no audit row. */
export function saveCheckDecision(
  completion: FirewallCompletion,
): Effect.Effect<FirewallCommit, EndpointStoreError, SqlClient.SqlClient> {
  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    requireText(completion.checkId);
    requireBytes(completion.canonicalCandidate);
    requireBytes(completion.canonicalInput);
    requireBytes(completion.canonicalResult);
    const disposition = completion.admit ? "admitted" : "withheld";
    const prior = (yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "SELECT direction,canonical_candidate,canonical_input,canonical_result,disposition FROM firewall_checks WHERE check_id=?",
      [completion.checkId],
    ))[0];
    if (prior !== undefined) {
      if (
        readText(prior, "direction") !== completion.direction ||
        readText(prior, "disposition") !== disposition
      ) {
        return yield* new EndpointStoreError({ reason: "conflict" });
      }
      requireSameBytes(
        readBytes(prior, "canonical_candidate"),
        completion.canonicalCandidate,
      );
      requireSameBytes(
        readBytes(prior, "canonical_input"),
        completion.canonicalInput,
      );
      requireSameBytes(
        readBytes(prior, "canonical_result"),
        completion.canonicalResult,
      );
      return disposition;
    }
    yield* sql.unsafe<Readonly<Record<string, unknown>>>(
      "INSERT INTO firewall_checks(check_id,direction,canonical_candidate,canonical_input,canonical_result,disposition,decided_at) VALUES(?,?,?,?,?,?,CAST(unixepoch('subsec')*1000 AS INTEGER))",
      [
        completion.checkId,
        completion.direction,
        completion.canonicalCandidate,
        completion.canonicalInput,
        completion.canonicalResult,
        disposition,
      ],
    );
    return disposition;
  }).pipe(storeOperation, Effect.withSpan("saveCheckDecision"));
}
/**
 * Returns retained decisions without message content, private inputs,
 * diagnostics or rule text. Includes registered advice only for rules that
 * determined the decision.
 */
export function readDecisions(request: { readonly since?: number }) {
  return Effect.gen(function* () {
    const decisions: Array<
      (typeof firewallDecisionsSchema.Type.decisions)[number]
    > = [];
    for (const entry of yield* readFirewallDecisions(request.since)) {
      const { summaries } = yield* decodeRuntimeValue(
        savedSummarySchema,
        entry.canonicalResult,
      );
      for (const summary of summaries) {
        decisions.push({
          ...summary,
          disposition: entry.disposition,
          checkId: entry.checkId,
          time: entry.time,
        });
      }
    }
    return {
      decisions,
      activations: (yield* readFirewallActivations(request)).map(
        (entry) => entry.activation,
      ),
    } satisfies typeof firewallDecisionsSchema.Type;
  }).pipe(Effect.withSpan("readDecisions"));
}
function auditEntry(
  row: Readonly<Record<string, unknown>>,
): FirewallAuditEntry {
  const canonicalResult = readBytes(row, "canonical_result");
  return {
    sequence: readInteger(row, "sequence"),
    checkId: readText(row, "check_id"),
    direction: Schema.decodeUnknownSync(Schema.Literal("incoming", "outgoing"))(
      readText(row, "direction"),
    ),
    canonicalCandidate: readBytes(row, "canonical_candidate"),
    canonicalInput: readBytes(row, "canonical_input"),
    disposition: Schema.decodeUnknownSync(auditDisposition)(
      readText(row, "disposition"),
    ),
    canonicalResult,
  };
}
const firewallAuditInput = exactStruct({
  configurationDigest: Schema.String.pipe(Schema.pattern(/^[a-f0-9]{64}$/u)),
  installation: firewallInstallationSchema,
  context: MessageContext,
  agents: Schema.Array(AgentView),
  message: Schema.NullOr(Message),
});

/**
 * A check as the store retains it: every field except the decision-provider
 * traces, which stay in memory with the code that produced the check.
 */
export const withoutTraces = (check: FirewallCheck) => ({
  ...check,
  checks: check.checks.map((middleware) => Struct.omit(middleware, "trace")),
});
/**
 * Opaque retained bytes are interpreted only by their Client delivery owner.
 * A retained result that holds provider traces decodes without them.
 */
export const decodeFirewallAuditEntry = Effect.fn("decodeFirewallAuditEntry")(
  function* (entry: FirewallAuditEntry) {
    const input = yield* decodeRuntimeValue(
      firewallAuditInput,
      entry.canonicalInput,
    );
    const completed = yield* decodeRuntimeValue(
      completedCheckSchema,
      entry.canonicalResult,
    );
    const result = withoutTraces(completed.result);
    return {
      checkId: entry.checkId,
      direction: entry.direction,
      disposition: entry.disposition,
      candidate: entry.canonicalCandidate,
      input,
      result,
    };
  },
);
