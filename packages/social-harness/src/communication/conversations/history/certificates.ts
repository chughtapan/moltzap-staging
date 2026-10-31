/**
 * @file Certificate assembly: the signer order every certificate requires,
 * which re-anchor shares, and the record envelopes shared by the records the
 * engine builds and the records it restores from the store.
 */

import { Effect, type ParseResult, Schema } from "effect";
import { SignedMessage } from "../../../identity/envelope/signed-message.js";
import {
  type ActionCertifiedRecord,
  type CertifiedRecord,
  compareAgentIds,
  type RecordCore,
  type RecordHash,
  type RouterAnchor,
} from "../../records/index.js";

/** The nonempty encoded signer messages one certificate carries. */
export type CertificateSignatures =
  ActionCertifiedRecord["actionCertificate"]["signatures"];

/**
 * Encode signer messages in decoded-AgentId byte order, which a certificate
 * requires and neither a fold's map nor the store's key order gives.
 * @param messages Verified signer messages, in any order.
 * @returns The encoded messages, or `undefined` when there are none; each
 *   caller reports an empty certificate in its own error.
 */
export function orderedSignatures(
  messages: Iterable<SignedMessage>,
): Effect.Effect<CertificateSignatures | undefined, ParseResult.ParseError> {
  const sorted = [...messages].sort((left, right) =>
    compareAgentIds(left.senderAgentId, right.senderAgentId),
  );
  return Effect.forEach(
    sorted,
    (message) => Schema.encode(SignedMessage)(message),
    { concurrency: 1 },
  ).pipe(
    Effect.map((encoded) => {
      const first = encoded[0];
      return first === undefined ? undefined : [first, ...encoded.slice(1)];
    }),
  );
}

/**
 * Assemble an action-certified record from its verified parts.
 * @param recordCore Record core the action certificate certifies.
 * @param recordHash Hash of `recordCore`.
 * @param routerAnchor Anchor the record core commits to.
 * @param signatures Action signatures in certificate order.
 * @returns The record, whose certificate names the core's action hash.
 */
export function actionCertifiedRecord(
  recordCore: RecordCore,
  recordHash: RecordHash,
  routerAnchor: RouterAnchor,
  signatures: CertificateSignatures,
): ActionCertifiedRecord {
  return {
    sharnessVersion: recordCore.sharnessVersion,
    kind: "action_certified_record",
    recordHash,
    recordCore,
    routerAnchor,
    actionCertificate: {
      sharnessVersion: recordCore.sharnessVersion,
      kind: "action_certificate",
      actionHash: recordCore.actionHash,
      signatures,
    },
  };
}

/**
 * Add a durability certificate without changing the record hash.
 * @param record Action-certified record being completed.
 * @param votes Durability votes in certificate order.
 * @returns The complete certified record.
 */
export function certifiedRecord(
  record: ActionCertifiedRecord,
  votes: CertificateSignatures,
): CertifiedRecord {
  return {
    sharnessVersion: record.recordCore.sharnessVersion,
    kind: "certified_record",
    actionCertifiedRecord: record,
    durabilityCertificate: {
      sharnessVersion: record.recordCore.sharnessVersion,
      kind: "durability_certificate",
      recordHash: record.recordHash,
      votes,
    },
  };
}
