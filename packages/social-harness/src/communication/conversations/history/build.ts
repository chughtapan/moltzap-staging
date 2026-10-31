/**
 * @file Records built from an in-memory fold: the certified records, their
 * store rows and evidence rows, and their retained protocol evidence.
 */

import { Effect } from "effect";
import type { AgentId } from "../../../identity/identifiers.js";
import type {
  ProtocolEvidence,
  StagedRecord,
  CertifiedRecord as StoredCertifiedRecord,
} from "../../store/index.js";
import type {
  EngineActionFold,
  EngineCertifiedHead,
  EngineConversation,
} from "../engine-context.js";
import {
  type ClientRepresentationError,
  encodeCanonical,
  representationFailure,
} from "../../../encoding/canonical-json.js";
import { SignedMessage } from "../../../identity/envelope/signed-message.js";
import {
  type ActionCertifiedRecord,
  type AnchorHash,
  type CertifiedRecord,
  hashAnchor,
  hashRecord,
  type RecordCore,
  RecordCore as RecordCoreSchema,
} from "../../records/index.js";
import {
  actionCertifiedRecord,
  type CertificateSignatures,
  certifiedRecord,
  orderedSignatures,
} from "./certificates.js";

/**
 * The hash of a conversation's current anchor, which its next record binds.
 * A conversation still on its initial anchor holds the anchor body unhashed,
 * and every record since that anchor, the head included, binds its hash.
 * @param conversation The conversation whose anchor is current.
 * @param head The conversation's certified head.
 * @returns The current anchor's hash.
 */
export const anchorHashAtHead = (
  conversation: EngineConversation,
  head: EngineCertifiedHead,
): AnchorHash =>
  conversation.currentAnchor.kind === "initial_anchor_body"
    ? head.record.actionCertifiedRecord.recordCore.anchorHash
    : conversation.currentAnchor.anchorHash;

/**
 * Resolve the immutable Router anchor bound when one action fold is created.
 * @param fold Selected action and its action-specific Router anchor.
 * @returns The canonical anchor hash committed by the record core.
 */
export const recordAnchorHash = (
  fold: EngineActionFold,
): Effect.Effect<AnchorHash, ClientRepresentationError> =>
  fold.action.kind === "FIRST_MESSAGE"
    ? hashAnchor(fold.action.anchor)
    : Effect.succeed(fold.action.anchorHash);

const encodeOrderedEvidence = (
  evidence: ReadonlyMap<AgentId, SignedMessage>,
): Effect.Effect<CertificateSignatures, ClientRepresentationError> =>
  orderedSignatures(evidence.values()).pipe(
    Effect.mapError(representationFailure),
    Effect.flatMap((signatures) =>
      signatures === undefined
        ? Effect.fail(representationFailure())
        : Effect.succeed(signatures),
    ),
  );

/**
 * Convert one verified inner signature into its durable evidence row.
 * @param conversationId Private conversation that owns the evidence.
 * @param kind Evidence statement family retained by the store.
 * @param subjectId Hash named by the evidence statement.
 * @param message Verified self-addressed evidence message.
 * @returns Canonical durable evidence without changing its signer bytes.
 */
export const protocolEvidence = (
  conversationId: string,
  kind: ProtocolEvidence["kind"],
  subjectId: string,
  message: SignedMessage,
): Effect.Effect<ProtocolEvidence, ClientRepresentationError> =>
  encodeCanonical(SignedMessage, message).pipe(
    Effect.map((canonicalEvidence) => ({
      conversationId,
      kind,
      subjectId,
      evidenceKey: message.senderAgentId,
      canonicalEvidence,
    })),
  );

/**
 * Build the action-certified record after its threshold is reached.
 * @param fold In-memory fold containing verified action evidence.
 * @param anchorHash Completed anchor bound by the record core.
 * @returns One evidence-independent record core with its action certificate.
 */
export const makeActionCertifiedRecord = (
  fold: EngineActionFold,
  anchorHash: RecordCore["anchorHash"],
): Effect.Effect<ActionCertifiedRecord, ClientRepresentationError> =>
  Effect.gen(function* () {
    const signatures = yield* encodeOrderedEvidence(fold.actionEvidence);
    const recordCore: RecordCore =
      fold.action.kind === "FIRST_MESSAGE"
        ? {
            sharnessVersion: fold.action.messageIntent.sharnessVersion,
            kind: "record_core",
            membership: fold.conversation.membership.descriptor,
            anchorHash,
            action: fold.action,
            actionHash: fold.actionHash,
          }
        : {
            sharnessVersion: fold.action.messageIntent.sharnessVersion,
            kind: "record_core",
            membershipHash: fold.conversation.membership.hash,
            anchorHash,
            action: fold.action,
            actionHash: fold.actionHash,
          };
    return actionCertifiedRecord(
      recordCore,
      yield* hashRecord(recordCore),
      fold.routerAnchor,
      signatures,
    );
  }).pipe(Effect.withSpan("makeActionCertifiedRecord"));

/**
 * Add a durability certificate without changing the certified record hash.
 * @param record Action-certified core being finalized.
 * @param fold In-memory fold containing verified durability evidence.
 * @returns One complete certified record with mergeable durability votes.
 */
export const makeCertifiedRecord = (
  record: ActionCertifiedRecord,
  fold: EngineActionFold,
): Effect.Effect<CertifiedRecord, ClientRepresentationError> =>
  encodeOrderedEvidence(fold.durabilityEvidence).pipe(
    Effect.map((votes) => certifiedRecord(record, votes)),
  );

/**
 * Convert one verified record core to the store's evidence-free staging row.
 * @param record Verified action-certified record from the protocol fold.
 * @returns Canonical evidence-free row used for atomic staging.
 */
export const stagedRecord = (
  record: ActionCertifiedRecord,
): Effect.Effect<StagedRecord, ClientRepresentationError> => {
  const action = record.recordCore.action;
  return encodeCanonical(RecordCoreSchema, record.recordCore).pipe(
    Effect.map((canonicalRecordCore) => ({
      conversationId: action.conversationId,
      recordHash: record.recordHash,
      ...(action.previousRecordHash === null
        ? {}
        : { previousRecordHash: action.previousRecordHash }),
      membershipHash:
        action.kind === "FIRST_MESSAGE"
          ? action.messageIntent.membershipHash
          : action.membershipHash,
      anchorHash: record.recordCore.anchorHash,
      actionHash: record.recordCore.actionHash,
      authorAgentId: action.messageIntent.authorAgentId,
      msgId: action.messageIntent.msgId,
      canonicalRecordCore,
    })),
  );
};

/**
 * Convert a complete wire record into one complete store promotion row.
 * @param record Complete certified wire record.
 * @param actionSignatures Verified action signatures stored with the record.
 * @param durabilityVotes Verified durability votes stored with the record.
 * @returns Store promotion data with separate action and durability evidence.
 */
export const storedCertifiedRecord = (
  record: CertifiedRecord,
  actionSignatures: Iterable<SignedMessage>,
  durabilityVotes: Iterable<SignedMessage>,
): Effect.Effect<StoredCertifiedRecord, ClientRepresentationError> =>
  Effect.gen(function* () {
    const staged = yield* stagedRecord(record.actionCertifiedRecord);
    const actionEvidence = yield* Effect.forEach(
      actionSignatures,
      (message) =>
        protocolEvidence(
          staged.conversationId,
          "action",
          staged.actionHash,
          message,
        ),
      { concurrency: 1 },
    );
    const durabilityEvidence = yield* Effect.forEach(
      durabilityVotes,
      (message) =>
        protocolEvidence(
          staged.conversationId,
          "durability",
          staged.recordHash,
          message,
        ),
      { concurrency: 1 },
    );
    return { ...staged, actionEvidence, durabilityEvidence };
  }).pipe(Effect.withSpan("storedCertifiedRecord"));
