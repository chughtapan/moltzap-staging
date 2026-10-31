/** @file Cryptographic and cross-field verification for Client protocol values. */

import { Effect, Schema } from "effect";
import type { AgentId } from "../../identity/identifiers.js";
import type { Ed25519PublicKey } from "../../identity/keys.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  equalCanonical,
  representationFailure,
  sameBytes,
} from "../../encoding/canonical-json.js";
import { exactParseOptions } from "../../encoding/exact.js";
import { isStrictlyAscending } from "../../encoding/identifier.js";
import { AgentCard, type VerifiedAgentCard } from "../../identity/cards.js";
import {
  type LiveSignedMessage,
  SignedMessage,
  type SignedMessage as SignedMessageValue,
  type VerifiedSignedMessage,
} from "../../identity/envelope/signed-message.js";
import {
  deriveConversationId,
  deriveEvidenceEnvelopeId,
  hashAction,
  hashAnchor,
  hashMembershipDescriptor,
  hashMessageIntent,
  hashRecord,
  type OuterMembership,
} from "./encoding.js";
import {
  type ActionCertifiedRecord,
  type ActionCore,
  type ActionHash,
  type ActionProposal,
  type AnchorHash,
  type CatchUpAttestationStatement,
  type CatchUpIncomplete,
  type CatchUpPage,
  CatchUpRequest,
  type CatchUpRequest as CatchUpRequestValue,
  type CertifiedRecord,
  type CompletedReanchor,
  EvidenceStatement,
  type EvidenceStatement as EvidenceStatementValue,
  InitialAnchorBody,
  MembershipDescriptor,
  type MembershipDescriptor as MembershipDescriptorValue,
  type MembershipHash,
  ReanchorBody,
  type RecordCore,
  type RecordHash,
} from "./schemas.js";
import { compareAgentIds } from "./values.js";

/* eslint-disable jsdoc/require-jsdoc -- The package-private representation facade documents these closed verification operations. */

const sortedDistinct = (agentIds: readonly AgentId[]): boolean => {
  return isStrictlyAscending(agentIds, compareAgentIds);
};

export interface VerifiedMembership extends OuterMembership {
  readonly descriptor: MembershipDescriptorValue;
  readonly hash: MembershipHash;
}

export const verifyMembershipDescriptor = (
  descriptor: MembershipDescriptorValue,
  registrySignerPublicKey: Ed25519PublicKey,
): Effect.Effect<VerifiedMembership, ClientRepresentationError> =>
  Effect.gen(function* () {
    const verified = yield* Effect.forEach(
      descriptor.members,
      (representation) =>
        Schema.decodeUnknown(AgentCard)(representation, exactParseOptions).pipe(
          Effect.flatMap((agentCard) =>
            AgentCard.verify({ agentCard, registrySignerPublicKey }),
          ),
          Effect.mapError(representationFailure),
        ),
      { concurrency: "inherit" },
    );
    const agentIds = verified.map((card) => card.agentId);
    const agentNames = new Set(verified.map((card) => card.agentName));
    const first = verified[0];
    const second = verified[1];
    if (
      first === undefined ||
      second === undefined ||
      !sortedDistinct(agentIds) ||
      agentNames.size !== verified.length
    ) {
      return yield* representationFailure();
    }
    const memberAgentIds: readonly [AgentId, AgentId, ...AgentId[]] = [
      first.agentId,
      second.agentId,
      ...verified.slice(2).map((card) => card.agentId),
    ];
    const conversationId = yield* deriveConversationId(memberAgentIds);
    if (conversationId !== descriptor.conversationId) {
      return yield* representationFailure();
    }
    const members: OuterMembership["members"] = [
      first,
      second,
      ...verified.slice(2),
    ];
    return {
      descriptor,
      hash: yield* hashMembershipDescriptor(descriptor),
      members,
    };
  }).pipe(Effect.withSpan("verifyMembershipDescriptor"));

export const memberCard = (
  membership: VerifiedMembership,
  agentId: AgentId,
): VerifiedAgentCard | undefined =>
  membership.members.find((card) => card.agentId === agentId);

export const verifyOuterMessage = (input: {
  readonly message: LiveSignedMessage;
  readonly membership: VerifiedMembership;
}): Effect.Effect<
  LiveSignedMessage & VerifiedSignedMessage,
  ClientRepresentationError
> =>
  Effect.gen(function* () {
    const card = memberCard(input.membership, input.message.senderAgentId);
    const expected = input.membership.members.map((member) => member.agentId);
    if (
      card === undefined ||
      input.message.recipientAgentIds.length !== expected.length ||
      input.message.recipientAgentIds.some(
        (recipient, index) => recipient !== expected[index],
      )
    ) {
      return yield* representationFailure();
    }
    const verified = yield* SignedMessage.verify({
      signedMessage: input.message,
      agentCard: card,
    }).pipe(Effect.mapError(representationFailure));
    return verified;
  }).pipe(Effect.withSpan("verifyOuterMessage"));

export interface VerifiedEvidence {
  readonly message: VerifiedSignedMessage;
  readonly statement: EvidenceStatementValue;
}

/**
 * Verify the signature and stable identity of self-addressed evidence. Ingress
 * may supply the statement it decoded from this same body so routing and
 * verification share that decode; stored representations decode here instead.
 */
export const verifyStableEvidence = (input: {
  readonly representation: unknown;
  readonly membership: VerifiedMembership;
  readonly statement?: EvidenceStatementValue;
}): Effect.Effect<VerifiedEvidence, ClientRepresentationError> =>
  Effect.gen(function* () {
    const message = yield* Schema.decodeUnknown(SignedMessage)(
      input.representation,
      exactParseOptions,
    ).pipe(Effect.mapError(representationFailure));
    const card = memberCard(input.membership, message.senderAgentId);
    if (
      card === undefined ||
      message.recipientAgentIds.length !== 1 ||
      message.recipientAgentIds[0] !== message.senderAgentId
    ) {
      return yield* representationFailure();
    }
    const verifiedMessage = yield* SignedMessage.verify({
      signedMessage: message,
      agentCard: card,
    }).pipe(Effect.mapError(representationFailure));
    const statement =
      input.statement ??
      (yield* decodeCanonical(EvidenceStatement, message.body));
    if (
      !sameBytes(
        yield* encodeCanonical(EvidenceStatement, statement),
        message.body,
      )
    ) {
      return yield* representationFailure();
    }
    const expectedEnvelopeId = yield* deriveEvidenceEnvelopeId(statement);
    if (
      statement.signerAgentId !== message.senderAgentId ||
      expectedEnvelopeId !== message.envelopeId
    ) {
      return yield* representationFailure();
    }
    return { message: verifiedMessage, statement };
  }).pipe(Effect.withSpan("verifyStableEvidence"));

/**
 * Verify stable evidence a member delivered inside an outer envelope: the
 * envelope verifies as that member's, addressed to the whole membership, and
 * the evidence it carries verifies against the same membership.
 * @param input The delivery to verify.
 * @param input.outer The member's outer envelope.
 * @param input.evidence The evidence message the envelope carries.
 * @param input.membership Membership both must verify against.
 * @param input.statement Statement already decoded from this evidence body by ingress.
 * @returns The verified evidence message and its statement.
 */
export const verifyDeliveredEvidence = (input: {
  readonly outer: LiveSignedMessage;
  readonly evidence: SignedMessageValue;
  readonly membership: VerifiedMembership;
  readonly statement?: EvidenceStatementValue;
}): Effect.Effect<VerifiedEvidence, ClientRepresentationError> =>
  verifyOuterMessage({
    message: input.outer,
    membership: input.membership,
  }).pipe(
    Effect.zipRight(
      Schema.encode(SignedMessage)(input.evidence).pipe(
        Effect.catchTag("ParseError", () => representationFailure()),
      ),
    ),
    Effect.flatMap((representation) =>
      verifyStableEvidence({
        representation,
        membership: input.membership,
        ...(input.statement === undefined
          ? {}
          : { statement: input.statement }),
      }),
    ),
    Effect.withSpan("verifyDeliveredEvidence"),
  );

type EvidenceKind = EvidenceStatementValue["kind"];

const verifyEvidenceSet = <Kind extends EvidenceKind>(input: {
  readonly representations: readonly unknown[];
  readonly expectedKind: Kind;
  readonly membership: VerifiedMembership;
  /** At least this many distinct member signers, at most one per member; all-member requirements are exact. */
  readonly requiredCount: number;
  readonly statementMatches: (
    statement: Extract<EvidenceStatementValue, { readonly kind: Kind }>,
  ) => Effect.Effect<boolean, ClientRepresentationError>;
}): Effect.Effect<readonly VerifiedEvidence[], ClientRepresentationError> =>
  Effect.gen(function* () {
    const length = input.representations.length;
    if (length > input.membership.members.length) {
      return yield* representationFailure();
    }
    if (length < input.requiredCount) {
      return yield* representationFailure();
    }
    const verified = yield* Effect.forEach(
      input.representations,
      (representation) =>
        verifyStableEvidence({ representation, membership: input.membership }),
      { concurrency: "inherit" },
    );
    const signers = verified.map(({ statement }) => statement.signerAgentId);
    if (!sortedDistinct(signers)) {
      return yield* representationFailure();
    }
    const matchesKind = (
      statement: EvidenceStatementValue,
    ): statement is Extract<EvidenceStatementValue, { readonly kind: Kind }> =>
      statement.kind === input.expectedKind;
    const matches = yield* Effect.forEach(
      verified,
      ({ statement }) =>
        matchesKind(statement)
          ? input.statementMatches(statement)
          : Effect.succeed(false),
      { concurrency: "inherit" },
    );
    if (matches.some((matchesStatement) => !matchesStatement)) {
      return yield* representationFailure();
    }
    return verified;
  });

export const quorumThreshold = (memberCount: number): number =>
  memberCount < 4
    ? memberCount
    : memberCount - Math.floor((memberCount - 1) / 3);

export interface VerifiedActionCore {
  readonly actionHash: ActionHash;
  readonly anchorHash: AnchorHash;
}

const actionReferencesMembership = (input: {
  readonly action: ActionCore;
  readonly membership: VerifiedMembership;
}): boolean => {
  const intent = input.action.messageIntent;
  if (
    input.action.conversationId !==
      input.membership.descriptor.conversationId ||
    intent.conversationId !== input.membership.descriptor.conversationId
  ) {
    return false;
  }
  if (
    intent.membershipHash !== input.membership.hash ||
    memberCard(input.membership, intent.authorAgentId) === undefined
  ) {
    return false;
  }
  return true;
};

export const verifyActionCore = (input: {
  readonly action: ActionCore;
  readonly membership: VerifiedMembership;
}): Effect.Effect<VerifiedActionCore, ClientRepresentationError> =>
  Effect.gen(function* () {
    const action = input.action;
    const intent = action.messageIntent;
    if (!actionReferencesMembership(input)) {
      return yield* representationFailure();
    }
    if ((yield* hashMessageIntent(intent)) !== action.messageIntentHash) {
      return yield* representationFailure();
    }
    if (action.kind === "FIRST_MESSAGE") {
      if (
        !(yield* equalCanonical(
          MembershipDescriptor,
          action.membership,
          input.membership.descriptor,
        )) ||
        action.anchor.conversationId !== action.conversationId ||
        action.anchor.membershipHash !== input.membership.hash
      ) {
        return yield* representationFailure();
      }
      return {
        actionHash: yield* hashAction(action),
        anchorHash: yield* hashAnchor(action.anchor),
      };
    }
    if (action.membershipHash !== input.membership.hash) {
      return yield* representationFailure();
    }
    return {
      actionHash: yield* hashAction(action),
      anchorHash: action.anchorHash,
    };
  }).pipe(Effect.withSpan("verifyActionCore"));

export const verifyActionProposal = (input: {
  readonly proposal: ActionProposal;
  readonly membership: VerifiedMembership;
  readonly outerSenderAgentId: AgentId;
}): Effect.Effect<VerifiedActionCore, ClientRepresentationError> =>
  Effect.gen(function* () {
    const verifiedAction = yield* verifyActionCore({
      action: input.proposal.action,
      membership: input.membership,
    });
    const authorAgentId = input.proposal.action.messageIntent.authorAgentId;
    if (input.outerSenderAgentId !== authorAgentId) {
      return yield* representationFailure();
    }
    return verifiedAction;
  }).pipe(Effect.withSpan("verifyActionProposal"));

/**
 * The caller has already verified `actionHash` against `action`.
 */
const verifyActionCertificateEvidence = (input: {
  readonly actionHash: ActionHash;
  readonly certificate: ActionCertifiedRecord["actionCertificate"];
  readonly action: ActionCore;
  readonly membership: VerifiedMembership;
}): Effect.Effect<ActionHash, ClientRepresentationError> =>
  Effect.gen(function* () {
    if (input.certificate.actionHash !== input.actionHash) {
      return yield* representationFailure();
    }
    const requiredCount =
      input.action.kind === "FIRST_MESSAGE"
        ? input.membership.members.length
        : quorumThreshold(input.membership.members.length);
    const evidence = yield* verifyEvidenceSet({
      representations: input.certificate.signatures,
      expectedKind: "action_signature",
      membership: input.membership,
      requiredCount,
      statementMatches: (statement) =>
        Effect.succeed(statement.actionHash === input.actionHash),
    });
    const authorAgentId = input.action.messageIntent.authorAgentId;
    if (
      !evidence.some(
        ({ statement }) => statement.signerAgentId === authorAgentId,
      )
    ) {
      return yield* representationFailure();
    }
    return input.actionHash;
  }).pipe(Effect.withSpan("verifyActionCertificate"));

export const verifyCompletedReanchor = (input: {
  readonly completed: CompletedReanchor;
  readonly membership: VerifiedMembership;
}): Effect.Effect<AnchorHash, ClientRepresentationError> =>
  Effect.gen(function* () {
    const expectedAnchorHash = yield* hashAnchor(input.completed.reanchor);
    if (
      expectedAnchorHash !== input.completed.anchorHash ||
      input.completed.certificate.anchorHash !== expectedAnchorHash ||
      input.completed.reanchor.membershipHash !== input.membership.hash ||
      input.completed.reanchor.conversationId !==
        input.membership.descriptor.conversationId
    ) {
      return yield* representationFailure();
    }
    yield* verifyEvidenceSet({
      representations: input.completed.certificate.votes,
      expectedKind: "reanchor_vote",
      membership: input.membership,
      requiredCount: quorumThreshold(input.membership.members.length),
      statementMatches: (statement) =>
        equalCanonical(
          ReanchorBody,
          statement.reanchor,
          input.completed.reanchor,
        ).pipe(
          Effect.map(
            (sameReanchor) =>
              sameReanchor && statement.anchorHash === expectedAnchorHash,
          ),
        ),
    });
    return expectedAnchorHash;
  }).pipe(Effect.withSpan("verifyCompletedReanchor"));

/**
 * Check that a record core binds the membership this endpoint holds for its
 * conversation. A FIRST_MESSAGE core carries the descriptor, which must be the held
 * descriptor byte for byte; a MESSAGE core carries only the `MembershipHash`,
 * which must be the held membership's hash. The caller verified the held
 * membership against the Registry when it first took it.
 * @param recordCore The record core to check.
 * @param membership The membership held for the record's conversation.
 * @returns Completion when the core names that membership.
 */
const verifyRecordMembership = (
  recordCore: RecordCore,
  membership: VerifiedMembership,
): Effect.Effect<void, ClientRepresentationError> =>
  ("membership" in recordCore
    ? equalCanonical(
        MembershipDescriptor,
        recordCore.membership,
        membership.descriptor,
      )
    : Effect.succeed(recordCore.membershipHash === membership.hash)
  ).pipe(
    Effect.filterOrFail((matches) => matches, representationFailure),
    Effect.asVoid,
  );

export const verifyRecordCore = (input: {
  readonly recordCore: RecordCore;
  readonly membership: VerifiedMembership;
}): Effect.Effect<RecordHash, ClientRepresentationError> =>
  Effect.gen(function* () {
    const { membership } = input;
    yield* verifyRecordMembership(input.recordCore, membership);
    const verifiedAction = yield* verifyActionCore({
      action: input.recordCore.action,
      membership,
    });
    if (
      input.recordCore.actionHash !== verifiedAction.actionHash ||
      input.recordCore.anchorHash !== verifiedAction.anchorHash
    ) {
      return yield* representationFailure();
    }
    return yield* hashRecord(input.recordCore);
  }).pipe(Effect.withSpan("verifyRecordCore"));

const verifyRecordRouterAnchor = (input: {
  readonly record: ActionCertifiedRecord;
  readonly membership: VerifiedMembership;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    const routerAnchor = input.record.routerAnchor;
    const anchorHash =
      routerAnchor.kind === "initial_anchor_body"
        ? yield* hashAnchor(routerAnchor)
        : yield* verifyCompletedReanchor({
            completed: routerAnchor,
            membership: input.membership,
          });
    if (anchorHash !== input.record.recordCore.anchorHash) {
      return yield* representationFailure();
    }
    if (routerAnchor.kind !== "initial_anchor_body") {
      return;
    }
    if (
      routerAnchor.conversationId !==
        input.membership.descriptor.conversationId ||
      routerAnchor.membershipHash !== input.membership.hash
    ) {
      return yield* representationFailure();
    }
  });

const verifyFirstMessageRouterAnchor = (input: {
  readonly record: ActionCertifiedRecord;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    const action = input.record.recordCore.action;
    if (action.kind !== "FIRST_MESSAGE") {
      return;
    }
    const routerAnchor = input.record.routerAnchor;
    if (routerAnchor.kind !== "initial_anchor_body") {
      return yield* representationFailure();
    }
    if (
      !(yield* equalCanonical(InitialAnchorBody, routerAnchor, action.anchor))
    ) {
      return yield* representationFailure();
    }
  });

export const verifyActionCertifiedRecord = (input: {
  readonly record: ActionCertifiedRecord;
  readonly membership: VerifiedMembership;
}): Effect.Effect<RecordHash, ClientRepresentationError> =>
  Effect.gen(function* () {
    const recordHash = yield* verifyRecordCore({
      recordCore: input.record.recordCore,
      membership: input.membership,
    });
    if (input.record.recordHash !== recordHash) {
      return yield* representationFailure();
    }
    yield* verifyActionCertificateEvidence({
      certificate: input.record.actionCertificate,
      action: input.record.recordCore.action,
      membership: input.membership,
      actionHash: input.record.recordCore.actionHash,
    });
    yield* verifyRecordRouterAnchor({
      record: input.record,
      membership: input.membership,
    });
    yield* verifyFirstMessageRouterAnchor({ record: input.record });
    return recordHash;
  }).pipe(Effect.withSpan("verifyActionCertifiedRecord"));

export const verifyCertifiedRecord = (input: {
  readonly record: CertifiedRecord;
  readonly membership: VerifiedMembership;
}): Effect.Effect<readonly VerifiedEvidence[], ClientRepresentationError> =>
  Effect.gen(function* () {
    const { membership } = input;
    const recordHash = yield* verifyActionCertifiedRecord({
      record: input.record.actionCertifiedRecord,
      membership,
    });
    const certificate = input.record.durabilityCertificate;
    if (certificate.recordHash !== recordHash) {
      return yield* representationFailure();
    }
    return yield* verifyEvidenceSet({
      representations: certificate.votes,
      expectedKind: "durability_vote",
      membership,
      requiredCount: quorumThreshold(membership.members.length),
      statementMatches: (statement) =>
        Effect.succeed(
          statement.conversationId === membership.descriptor.conversationId &&
            statement.membershipHash === membership.hash &&
            statement.recordHash === recordHash,
        ),
    });
  }).pipe(Effect.withSpan("verifyCertifiedRecord"));

const verifyCatchUpRequest = (input: {
  readonly request: CatchUpRequestValue;
  readonly membership: VerifiedMembership;
}): Effect.Effect<void, ClientRepresentationError> =>
  input.request.conversationId === input.membership.descriptor.conversationId &&
  input.request.membershipHash === input.membership.hash &&
  memberCard(input.membership, input.request.requesterAgentId) !== undefined
    ? Effect.void
    : Effect.fail(representationFailure());

const catchUpAttestationMatches = (input: {
  readonly statement: CatchUpAttestationStatement;
  readonly request: CatchUpRequestValue;
  readonly itemKind: CatchUpAttestationStatement["itemKind"];
  readonly itemHash: RecordHash | AnchorHash | null;
  readonly hasMore: boolean;
}): Effect.Effect<boolean, ClientRepresentationError> =>
  equalCanonical(CatchUpRequest, input.statement.request, input.request).pipe(
    Effect.map(
      (sameRequest) =>
        sameRequest &&
        input.statement.itemKind === input.itemKind &&
        input.statement.itemHash === input.itemHash &&
        input.statement.hasMore === input.hasMore,
    ),
  );

const verifyCatchUpAttestation = (input: {
  readonly representation: unknown;
  readonly membership: VerifiedMembership;
  readonly responseSenderAgentId: AgentId;
  readonly request: CatchUpRequestValue;
  readonly itemKind: CatchUpAttestationStatement["itemKind"];
  readonly itemHash: RecordHash | AnchorHash | null;
  readonly hasMore: boolean;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    const evidence = yield* verifyStableEvidence(input);
    if (
      evidence.statement.kind !== "catch_up_attestation" ||
      evidence.statement.signerAgentId !== input.responseSenderAgentId ||
      !(yield* catchUpAttestationMatches({
        statement: evidence.statement,
        request: input.request,
        itemKind: input.itemKind,
        itemHash: input.itemHash,
        hasMore: input.hasMore,
      }))
    ) {
      return yield* representationFailure();
    }
  });

interface VerifiedCatchUpItem {
  readonly itemKind: CatchUpAttestationStatement["itemKind"];
  readonly itemHash: RecordHash | AnchorHash;
}

const certifiedItemExtendsPosition = (input: {
  readonly request: CatchUpRequestValue;
  readonly record: CertifiedRecord;
}): boolean => {
  const action = input.record.actionCertifiedRecord.recordCore.action;
  if (input.request.knownRecordHash === null) {
    return (
      action.kind === "FIRST_MESSAGE" &&
      input.record.actionCertifiedRecord.routerAnchor.kind ===
        "initial_anchor_body"
    );
  }
  return (
    action.kind === "MESSAGE" &&
    action.previousRecordHash === input.request.knownRecordHash &&
    action.anchorHash === input.request.knownAnchorHash
  );
};

const verifyCertifiedCatchUpItem = (input: {
  readonly page: CatchUpPage;
  readonly item: CertifiedRecord;
  readonly membership: VerifiedMembership;
}): Effect.Effect<VerifiedCatchUpItem, ClientRepresentationError> =>
  Effect.gen(function* () {
    if (
      !certifiedItemExtendsPosition({
        request: input.page.request,
        record: input.item,
      })
    ) {
      return yield* representationFailure();
    }
    yield* verifyCertifiedRecord({
      record: input.item,
      membership: input.membership,
    });
    return {
      itemKind: "certified_record",
      itemHash: input.item.actionCertifiedRecord.recordHash,
    };
  });

const verifyReanchorCatchUpItem = (input: {
  readonly page: CatchUpPage;
  readonly item: CompletedReanchor;
  readonly membership: VerifiedMembership;
}): Effect.Effect<VerifiedCatchUpItem, ClientRepresentationError> =>
  Effect.gen(function* () {
    if (
      input.page.request.knownRecordHash === null ||
      input.item.reanchor.selectedRecordHash !==
        input.page.request.knownRecordHash ||
      input.item.reanchor.previousAnchorHash !==
        input.page.request.knownAnchorHash
    ) {
      return yield* representationFailure();
    }
    yield* verifyCompletedReanchor({
      completed: input.item,
      membership: input.membership,
    });
    return {
      itemKind: "completed_reanchor",
      itemHash: input.item.anchorHash,
    };
  });

const verifyCatchUpItem = (input: {
  readonly page: CatchUpPage;
  readonly membership: VerifiedMembership;
}): Effect.Effect<VerifiedCatchUpItem, ClientRepresentationError> => {
  const item = input.page.item;
  return item.kind === "certified_record"
    ? verifyCertifiedCatchUpItem({
        page: input.page,
        item,
        membership: input.membership,
      })
    : verifyReanchorCatchUpItem({
        page: input.page,
        item,
        membership: input.membership,
      });
};

export const verifyCatchUpPage = (input: {
  readonly page: CatchUpPage;
  readonly membership: VerifiedMembership;
  readonly responseSenderAgentId: AgentId;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    yield* verifyCatchUpRequest({
      request: input.page.request,
      membership: input.membership,
    });
    const verifiedItem = yield* verifyCatchUpItem(input);
    yield* verifyCatchUpAttestation({
      representation: input.page.attestation,
      membership: input.membership,
      responseSenderAgentId: input.responseSenderAgentId,
      request: input.page.request,
      itemKind: verifiedItem.itemKind,
      itemHash: verifiedItem.itemHash,
      hasMore: input.page.hasMore,
    });
  }).pipe(Effect.withSpan("verifyCatchUpPage"));

export const verifyCatchUpIncomplete = (input: {
  readonly incomplete: CatchUpIncomplete;
  readonly membership: VerifiedMembership;
  readonly responseSenderAgentId: AgentId;
}): Effect.Effect<void, ClientRepresentationError> =>
  Effect.gen(function* () {
    yield* verifyCatchUpRequest({
      request: input.incomplete.request,
      membership: input.membership,
    });
    yield* verifyCatchUpAttestation({
      representation: input.incomplete.attestation,
      membership: input.membership,
      responseSenderAgentId: input.responseSenderAgentId,
      request: input.incomplete.request,
      itemKind: "incomplete",
      itemHash: null,
      hasMore: false,
    });
  }).pipe(Effect.withSpan("verifyCatchUpIncomplete"));

/* eslint-enable jsdoc/require-jsdoc -- Restore package documentation rules. */
