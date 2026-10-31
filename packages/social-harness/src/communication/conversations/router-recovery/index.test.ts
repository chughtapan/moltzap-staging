/** @file Restart catch-up and threshold re-anchor tests for the addressed engine. */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { SqlClient } from "@effect/sql";
import { scopedLive as it, scoped as itOnTestClock } from "@effect/vitest";
import {
  Chunk,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Option,
  Queue,
  Ref,
  Schedule,
  Schema,
  TestClock,
  TestContext,
  TestServices,
} from "effect";
import { generateKeyPairSync } from "node:crypto";
import { describe, expect, vi } from "vitest";
import type { RegistryLookupResult } from "../../../identity/registry/contract.js";
import type {
  EndpointRecovery,
  StoredOutboundMessage,
} from "../../store/index.js";
import type { EngineRuntime } from "../engine-context.js";
import {
  digest,
  identifier,
  issueTestCard,
  makeTestAuthority,
  type RegistryKeyPair,
} from "../../../__tests__/agent-card-fixtures.js";
import {
  buildCertifiedFirstMessage,
  signEvidence,
  type SigningIdentity,
} from "../../../__tests__/certified-history-fixtures.js";
import {
  openTestDatabase,
  type TestDatabase,
} from "../../../__tests__/communication-test-database.js";
import { authoredMessage } from "../../../__tests__/conversation-fixtures.js";
import { forwardStoredOutbound } from "../../../__tests__/forward-stored-outbound.js";
import { openOuterBody } from "../../../__tests__/outer-body-fixtures.js";
import {
  corruptSignature,
  pollCursor as fixturePollCursor,
} from "../../../__tests__/router-worker-fixtures.js";
import { testStore } from "../../../__tests__/store-schema-fixtures.js";
import {
  decodeCanonical,
  encodeCanonical,
} from "../../../encoding/canonical-json.js";
import {
  AgentCard,
  agentViewOf,
  type VerifiedAgentCard,
} from "../../../identity/cards.js";
import { SignedMessage } from "../../../identity/envelope/signed-message.js";
import { Ed25519PublicKey } from "../../../identity/keys.js";
import { saveLocalIdentity } from "../../../identity/local-store.js";
import { WIRE_VERSION } from "../../../identity/wire-version.js";
import { Message, messageFromIntent } from "../../message.js";
import {
  type ActionCertifiedRecord as ActionCertifiedRecordValue,
  type ActionProposal,
  type CatchUpIncomplete,
  type CatchUpPage,
  type CatchUpRequest,
  type CertifiedRecord,
  type CompletedReanchor,
  type DecodedOuterBody,
  decodeOuterBody,
  deriveConversationId,
  type DirectPacket,
  EvidenceStatement,
  type EvidenceStatement as EvidenceStatementValue,
  type FirstMessageActionCore,
  hashAction,
  hashAnchor,
  hashMembershipDescriptor,
  hashMessageIntent,
  hashRecord,
  InitialAnchorBody,
  MembershipDescriptor,
  MembershipHash,
  type MessageActionCore,
  MessageIntent,
  mintMsgId,
  quorumThreshold,
  type ReanchorBody,
  type RecordCore,
  RecordHash,
  signEvidenceMessage,
  signOuterEvidence,
  signOuterPacket,
  type VerifiedMembership,
  verifyCatchUpPage,
  verifyCompletedReanchor,
  verifyMembershipDescriptor,
} from "../../records/index.js";
import { compareAgentIds } from "../../records/values.js";
import { RouterInstanceId } from "../../router/api/contract.js";
import {
  type RouterDiscontinuityReason,
  type RouterIngressDisposition,
  RouterWorkerDiscontinuityError,
  type RouterWorkerIngress,
} from "../../router/index.js";
import { acceptEngineRecoveryIngress } from "../certification/index.js";
import {
  type EndpointEngine,
  type EndpointEngineInput,
  EngineOutboundError,
  makeEndpointEngine,
} from "../index.js";
import { makeOutbox } from "../outbox.js";
import { recoverEngineState } from "../restore.js";
import {
  acceptCatchUpPage,
  type CatchUpRun,
  makeCatchUpState,
  requestCertifiedHistory,
  resendCertifiedHistoryRequest,
  waitBehindEarlierReanchor,
} from "./catch-up.js";
import { catchUpRetryAttempts } from "./index.js";

/* eslint-disable max-lines, max-lines-per-function -- One exact cryptographic trace keeps protocol order and assertions together. */

/** The Router-worker operations an engine consumes. */
type EngineRouterPort = EndpointEngineInput["routerWorker"];

/**
 * Bounds a hang while recovery signs, seals, and verifies traffic for every
 * fixed member. Cryptographic work slows with machine load; no assertion
 * depends on this timeout.
 */
const TRACE_TIMEOUT_MS = 60_000;

vi.setConfig({ testTimeout: TRACE_TIMEOUT_MS });

interface RecoveryFixture {
  readonly engine: EndpointEngine;
  readonly input: EndpointEngineInput;
  readonly store: TestDatabase;
  readonly local: SigningIdentity;
  readonly remote: SigningIdentity;
  readonly registryKeys: RegistryKeyPair;
  readonly registrySignerPublicKey: typeof Ed25519PublicKey.Type;
  readonly membership: VerifiedMembership;
  readonly certifiedRecord: CertifiedRecord;
  readonly normalOutbound: Queue.Queue<SignedMessage>;
  readonly recoveryOutbound: Queue.Queue<SignedMessage>;
}

interface N4Foundation {
  readonly engine: EndpointEngine;
  readonly membership: VerifiedMembership;
  readonly third: SigningIdentity;
  readonly fourth: SigningIdentity;
}

/** A group conversation the fixture endpoint holds with fixed members. */
interface GroupFoundation {
  readonly engine: EndpointEngine;
  readonly membership: VerifiedMembership;
  /** Every member but the local endpoint, the remote member first. */
  readonly others: readonly SigningIdentity[];
}

/**
 * A group conversation's certified FIRST_MESSAGE head, and a MESSAGE at that head the
 * remote member authors, action-certified by every member, whose durability
 * no member has certified.
 */
interface GroupHistory {
  readonly certifiedHead: CertifiedRecord;
  readonly successor: ActionCertifiedRecordValue;
}

interface N4PartialHistory {
  readonly certifiedHead: CertifiedRecord;
  readonly stagedSuccessor: ActionCertifiedRecordValue;
  /**
   * `stagedSuccessor` with the remote, third and fourth members' durability
   * votes. It carries none of the local endpoint's, so it also fits a test
   * whose endpoint staged a re-anchor candidate at the head, after which the
   * endpoint holds no vote for any successor of that head.
   */
  readonly certifiedSuccessor: CertifiedRecord;
}

/**
 * Where a fixture Router worker delivers the envelopes the endpoint sends:
 * catch-up and re-anchor traffic to `recoveryOutbound`, everything else to
 * `normalOutbound`, so a test reads each stream on its own.
 */
interface FixtureRouterContext {
  readonly store: TestDatabase;
  readonly local: SigningIdentity;
  readonly normalOutbound: Queue.Queue<SignedMessage>;
  readonly recoveryOutbound: Queue.Queue<SignedMessage>;
}

interface QueuedActionProposal {
  readonly message: SignedMessage;
  readonly proposal: ActionProposal;
}

interface HeldRouterInput {
  readonly holdOutbound: Ref.Ref<boolean>;
  readonly pendingOutboundIds: Queue.Queue<string>;
}

type ReanchorVote = Extract<
  EvidenceStatementValue,
  { readonly kind: "reanchor_vote" }
>;

const actionSignatureKind: EvidenceStatementValue["kind"] = "action_signature";
const acceptedDisposition: RouterIngressDisposition = "accepted";
const ignoredDisposition: RouterIngressDisposition = "ignored";
const durabilityVoteKind: EvidenceStatementValue["kind"] = "durability_vote";

const oldRouterInstanceId = Schema.decodeUnknownSync(RouterInstanceId)(
  identifier("rti_", 8),
);
const newRouterInstanceId = Schema.decodeUnknownSync(RouterInstanceId)(
  identifier("rti_", 9),
);
const laterRouterInstanceId = Schema.decodeUnknownSync(RouterInstanceId)(
  identifier("rti_", 10),
);
const pollCursor = fixturePollCursor(1);

/**
 * The endpoint that sent each envelope a fixture Router forwarded. Every outer
 * body is sealed to its own sender too, so a test opens what an endpoint sent
 * as that endpoint.
 */
const forwardedBy = new WeakMap<SignedMessage, SigningIdentity>();

/**
 * Sends one outbox row the way the fixture Router worker delivers it: to the
 * context's recovery queue when it carries catch-up or re-anchor traffic,
 * otherwise to its normal queue.
 * @param context Store and queues the fixture worker delivers through.
 * @param envelopeId Outbox identity the endpoint sends.
 * @returns Completion once the envelope is delivered or found inactive.
 */
function forwardSorted(
  context: FixtureRouterContext,
  envelopeId: string,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    const staging = yield* Queue.unbounded<SignedMessage>();
    yield* forwardStoredOutbound(context.store, staging, envelopeId);
    const sent = yield* Queue.takeAll(staging);
    for (const message of sent) {
      forwardedBy.set(message, context.local);
    }
    yield* Effect.forEach(
      sent,
      (message) =>
        isRecoveryTraffic(message).pipe(
          Effect.flatMap((recovery) =>
            Queue.offer(
              recovery ? context.recoveryOutbound : context.normalOutbound,
              message,
            ),
          ),
        ),
      { concurrency: 1, discard: true },
    );
  });
}

/**
 * Whether an envelope carries catch-up or re-anchor traffic.
 * @param message Envelope the endpoint sent.
 * @returns True for catch-up packets, completed re-anchors and re-anchor votes.
 */
function isRecoveryTraffic(message: SignedMessage): Effect.Effect<boolean> {
  return openForwarded(message).pipe(
    Effect.flatMap((body) =>
      body.kind === "evidence"
        ? decodeCanonical(EvidenceStatement, body.message.body).pipe(
            Effect.map((statement) => statement.kind === "reanchor_vote"),
          )
        : Effect.succeed(
            body.packet.kind === "catch_up_request" ||
              body.packet.kind === "catch_up_page" ||
              body.packet.kind === "catch_up_incomplete" ||
              body.packet.kind === "completed_reanchor",
          ),
    ),
    Effect.orDie,
  );
}

/**
 * The Client value inside an envelope a fixture Router forwarded, opened as
 * the endpoint that sent it.
 * @param message Envelope taken from a fixture Router queue.
 * @returns The decoded body; an envelope no fixture Router forwarded is a
 *     defect.
 */
function openForwarded(message: SignedMessage) {
  const sender = forwardedBy.get(message);
  return sender === undefined
    ? Effect.dieMessage("no fixture Router forwarded this envelope")
    : openOuterBody(message, sender);
}

/**
 * The direct packet or evidence statement inside an envelope a fixture Router
 * forwarded. Packet and statement kinds are disjoint, so a kind names one.
 * @param message Envelope taken from a fixture Router queue.
 * @returns The packet, or the decoded statement.
 */
function carriedBy(
  message: SignedMessage,
): Effect.Effect<DirectPacket | EvidenceStatementValue> {
  return Effect.gen(function* () {
    const body = yield* openForwarded(message);
    return body.kind === "evidence"
      ? yield* decodeCanonical(EvidenceStatement, body.message.body)
      : body.packet;
  }).pipe(Effect.orDie);
}

/**
 * Takes envelopes from `outbound` until one carries `kind`, dropping those
 * before it.
 * @param outbound Queue the endpoint sends to.
 * @param kind Direct packet or evidence statement kind to wait for.
 * @returns The first envelope that carries `kind`.
 */
function takeNextCarrying(
  outbound: Queue.Queue<SignedMessage>,
  kind: (DirectPacket | EvidenceStatementValue)["kind"],
): Effect.Effect<SignedMessage> {
  return Queue.take(outbound).pipe(
    Effect.flatMap((message) =>
      carriedBy(message).pipe(
        Effect.flatMap((carried) =>
          carried.kind === kind
            ? Effect.succeed(message)
            : takeNextCarrying(outbound, kind),
        ),
      ),
    ),
  );
}

function makeHeldRouter(input: HeldRouterInput) {
  return (context: FixtureRouterContext): EngineRouterPort => ({
    currentAnchor: Effect.succeed({
      routerInstanceId: oldRouterInstanceId,
      pollCursor,
    }),
    awaitAnchor: Effect.succeed({
      routerInstanceId: oldRouterInstanceId,
      pollCursor,
    }),
    send: (envelopeId) =>
      Ref.get(input.holdOutbound).pipe(
        Effect.flatMap((hold) =>
          hold
            ? Queue.offer(input.pendingOutboundIds, envelopeId).pipe(
                Effect.asVoid,
              )
            : forwardSorted(context, envelopeId),
        ),
      ),
  });
}

/**
 * Opens an endpoint store in a scoped temporary directory holding the
 * conversation foundation of a certified FIRST_MESSAGE record: its membership and
 * its initial Router anchor.
 * @param membership Verified membership of the conversation.
 * @param certifiedRecord Certified FIRST_MESSAGE record whose anchor is stored.
 * @param prefix Temporary directory name prefix.
 * @returns The opened store.
 */
const openFirstMessageStore = (
  membership: VerifiedMembership,
  certifiedRecord: CertifiedRecord,
  prefix: string,
) =>
  Effect.gen(function* () {
    const anchor = certifiedRecord.actionCertifiedRecord.routerAnchor;
    if (anchor.kind !== "initial_anchor_body") {
      return yield* Effect.dieMessage("first message fixture lost its anchor");
    }
    const fileSystem = yield* FileSystem.FileSystem;
    const store = yield* openTestDatabase(
      yield* fileSystem.makeTempDirectoryScoped({ prefix }),
    );
    yield* store.run(
      testStore.putConversationFoundation({
        conversationId: membership.descriptor.conversationId,
        membershipHash: membership.hash,
        canonicalMembership: yield* encodeCanonical(
          MembershipDescriptor,
          membership.descriptor,
        ),
        anchorHash: yield* hashAnchor(anchor),
        canonicalAnchor: yield* encodeCanonical(InitialAnchorBody, anchor),
      }),
    );
    return store;
  });

const makeFixtureWithRouter = (
  makeRouter: (context: FixtureRouterContext) => EngineRouterPort,
  identityBytes: { readonly local: number; readonly remote: number } = {
    local: 1,
    remote: 2,
  },
) =>
  Effect.gen(function* () {
    const registryKeys = generateKeyPairSync("ed25519");
    const registrySignerPublicKey = yield* Schema.decodeUnknown(
      Ed25519PublicKey,
    )(registryKeys.publicKey.export({ format: "jwk" }));
    const localAuthority = yield* makeTestAuthority();
    const remoteAuthority = yield* makeTestAuthority();
    const local: SigningIdentity = {
      card: yield* issueTestCard({
        byte: identityBytes.local,
        name: `recovery-${identityBytes.local}`,
        authority: localAuthority,
        registryKeys,
      }),
      authority: localAuthority,
    };
    const remote: SigningIdentity = {
      card: yield* issueTestCard({
        byte: identityBytes.remote,
        name: `recovery-${identityBytes.remote}`,
        authority: remoteAuthority,
        registryKeys,
      }),
      authority: remoteAuthority,
    };
    const localCard = yield* Schema.encode(AgentCard)(local.card);
    const remoteCard = yield* Schema.encode(AgentCard)(remote.card);
    const conversationId = yield* deriveConversationId([
      local.card.agentId,
      remote.card.agentId,
    ]);
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptor)({
      sharnessVersion: WIRE_VERSION,
      kind: "membership_descriptor",
      conversationId,
      members: [localCard, remoteCard],
    });
    const membership = yield* verifyMembershipDescriptor(
      descriptor,
      registrySignerPublicKey,
    );
    const certifiedRecord = yield* buildCertifiedFirstMessage(
      local,
      remote,
      membership,
      oldRouterInstanceId,
    );
    const store = yield* openFirstMessageStore(
      membership,
      certifiedRecord,
      "sharness-recovery-",
    );
    const cards: readonly [VerifiedAgentCard, VerifiedAgentCard] = [
      local.card,
      remote.card,
    ];
    const registry: EndpointEngineInput["registry"] = {
      lookup: (request) => {
        const card = cards.find((candidate) =>
          "agentId" in request
            ? candidate.agentId === request.agentId
            : candidate.agentName === request.agentName,
        );
        const result: RegistryLookupResult =
          card === undefined
            ? { kind: "not_found" }
            : { kind: "found", agentCard: card };
        return Effect.succeed(result);
      },
    };
    const normalOutbound = yield* Queue.unbounded<SignedMessage>();
    const recoveryOutbound = yield* Queue.unbounded<SignedMessage>();
    const input = {
      localAgentCard: local.card,
      signingAuthority: local.authority,
      registrySignerPublicKey,
      registry,
      reportStorageFault: () => Effect.void,
      routerWorker: makeRouter({
        store,
        local,
        normalOutbound,
        recoveryOutbound,
      }),
    } satisfies EndpointEngineInput;
    yield* store.run(saveLocalIdentity(local.card));
    const engine = yield* makeEndpointEngine(input)
      .pipe(Effect.provideService(SqlClient.SqlClient, store.sql))
      .pipe(Effect.orDie);
    return {
      engine,
      input,
      store,
      local,
      remote,
      registryKeys,
      registrySignerPublicKey,
      membership,
      certifiedRecord,
      normalOutbound,
      recoveryOutbound,
    } satisfies RecoveryFixture;
  }).pipe(Effect.provide(NodeFileSystem.layer));

/**
 * A Router worker anchored at `routerInstanceId` that forwards every sent
 * outbox row to the fixture's normal outbound queue.
 * @param routerInstanceId Router instance the worker is anchored at.
 * @returns The worker, built over the fixture's store and queue.
 */
const fixtureRouterAt =
  (routerInstanceId: typeof RouterInstanceId.Type) =>
  (context: FixtureRouterContext): EngineRouterPort => ({
    currentAnchor: Effect.succeed({ routerInstanceId, pollCursor }),
    awaitAnchor: Effect.succeed({ routerInstanceId, pollCursor }),
    send: (envelopeId) => forwardSorted(context, envelopeId),
  });

const makeFixtureRouter = fixtureRouterAt(newRouterInstanceId);

const makeFixture = makeFixtureWithRouter(makeFixtureRouter);
const makeNonLexicalAgentOrderFixture = makeFixtureWithRouter(
  makeFixtureRouter,
  { local: 0, remote: 208 },
);

/** An endpoint under recovery and the queue its recovery traffic lands in. */
interface RecoveringEndpoint {
  readonly engine: EndpointEngine;
  readonly recoveryOutbound: Queue.Queue<SignedMessage>;
}

/**
 * Starts a recovery of the endpoint at `routerInstanceId` and keeps its
 * outbox sending, as the Router worker does once the recovery run starts.
 * The recovery runs under its own test clock, which nothing advances, so its
 * catch-up retries never resend a request in the middle of a trace; a trace
 * about retries passes `clock: "caller"` and drives the clock it runs under.
 * @param endpoint Endpoint under recovery and its recovery traffic queue.
 * @param reason Discontinuity that started the recovery.
 * @param routerInstanceId Router instance the recovery anchors to.
 * @param options How the recovery's clock runs.
 * @param options.clock `held` for a test clock nothing advances, `caller`
 *     for the clock the caller runs under.
 * @returns The running recovery and the queue its recovery traffic lands in.
 */
const forkRecovery = (
  endpoint: RecoveringEndpoint,
  reason: RouterDiscontinuityReason,
  routerInstanceId: typeof RouterInstanceId.Type,
  options: { readonly clock: "held" | "caller" } = { clock: "held" },
) =>
  Effect.gen(function* () {
    yield* endpoint.engine.abandonVolatileFolds(reason);
    yield* endpoint.engine.runOutbound.pipe(Effect.orDie, Effect.forkScoped);
    const recovering = endpoint.engine.recoverCertifiedHistory({
      reason,
      anchor: { routerInstanceId, pollCursor },
    });
    const recovery = yield* Effect.fork(
      options.clock === "held"
        ? recovering.pipe(Effect.provide(TestContext.TestContext))
        : recovering,
    );
    return { recovery, outbound: endpoint.recoveryOutbound };
  });

/**
 * Accepts one ingress through the engine's recovery path, as the Router
 * worker does while a recovery runs.
 * @param engine Endpoint receiving the ingress.
 * @param ingress The ingress to deliver.
 * @returns How the engine disposed of the ingress.
 */
const deliverRecovery = (
  engine: EndpointEngine,
  ingress: Effect.Effect<RouterWorkerIngress<DecodedOuterBody>>,
) => ingress.pipe(Effect.flatMap((value) => engine.acceptRouterIngress(value)));

/**
 * Opens a group conversation of `size` fixed members on the fixture's store:
 * the local endpoint, the remote member, and further test members. The
 * engine it returns sends through the fixture's Router queues.
 * @param fixture Endpoint and store the conversation is added to.
 * @param size Number of members, at least three.
 * @param initialRouterInstanceId Router instance the initial anchor binds.
 * @returns The engine, the verified membership, and every member but the
 *     local endpoint, the remote member first.
 */
const addGroupFoundation = (
  fixture: RecoveryFixture,
  size: number,
  initialRouterInstanceId: typeof RouterInstanceId.Type = oldRouterInstanceId,
) =>
  Effect.gen(function* () {
    const further = yield* Effect.forEach(
      [...Array.from({ length: size - 2 }).keys()].map((index) => index + 3),
      (byte) =>
        Effect.gen(function* () {
          const authority = yield* makeTestAuthority();
          const identity: SigningIdentity = {
            card: yield* issueTestCard({
              byte,
              name: `recovery-${String(byte)}`,
              authority,
              registryKeys: fixture.registryKeys,
            }),
            authority,
          };
          return identity;
        }),
      { concurrency: 1 },
    );
    const members = [fixture.local, fixture.remote, ...further];
    const conversationId = yield* deriveConversationId([
      fixture.local.card.agentId,
      fixture.remote.card.agentId,
      ...further.map((member) => member.card.agentId),
    ]);
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptor)({
      sharnessVersion: WIRE_VERSION,
      kind: "membership_descriptor",
      conversationId,
      members: yield* Effect.forEach(
        members,
        (identity) => Schema.encode(AgentCard)(identity.card),
        { concurrency: 1 },
      ),
    });
    const membership = yield* verifyMembershipDescriptor(
      descriptor,
      fixture.registrySignerPublicKey,
    );
    const anchor: InitialAnchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "initial_anchor_body",
      conversationId,
      membershipHash: membership.hash,
      routerInstanceId: initialRouterInstanceId,
    };
    yield* fixture.store.run(
      testStore.putConversationFoundation({
        conversationId,
        membershipHash: membership.hash,
        canonicalMembership: yield* encodeCanonical(
          MembershipDescriptor,
          membership.descriptor,
        ),
        anchorHash: yield* hashAnchor(anchor),
        canonicalAnchor: yield* encodeCanonical(InitialAnchorBody, anchor),
      }),
    );
    const cards = members.map((member) => member.card);
    const registry: EndpointEngineInput["registry"] = {
      lookup: (request) => {
        const card = cards.find((candidate) =>
          "agentId" in request
            ? candidate.agentId === request.agentId
            : candidate.agentName === request.agentName,
        );
        const result: RegistryLookupResult =
          card === undefined
            ? { kind: "not_found" }
            : { kind: "found", agentCard: card };
        return Effect.succeed(result);
      },
    };
    const engine = yield* makeEndpointEngine({
      localAgentCard: fixture.local.card,
      signingAuthority: fixture.local.authority,
      registrySignerPublicKey: fixture.registrySignerPublicKey,
      registry,
      reportStorageFault: () => Effect.void,
      routerWorker: {
        currentAnchor: Effect.succeed({
          routerInstanceId: newRouterInstanceId,
          pollCursor,
        }),
        awaitAnchor: Effect.succeed({
          routerInstanceId: newRouterInstanceId,
          pollCursor,
        }),
        send: (envelopeId) => forwardSorted(fixture, envelopeId),
      },
    })
      .pipe(Effect.provideService(SqlClient.SqlClient, fixture.store.sql))
      .pipe(Effect.orDie);
    return {
      engine,
      membership,
      others: [fixture.remote, ...further],
    } satisfies GroupFoundation;
  }).pipe(Effect.orDie);

const addN4Foundation = (
  fixture: RecoveryFixture,
  initialRouterInstanceId: typeof RouterInstanceId.Type = oldRouterInstanceId,
) =>
  addGroupFoundation(fixture, 4, initialRouterInstanceId).pipe(
    Effect.flatMap(({ engine, membership, others }) => {
      const [, third, fourth] = others;
      return third === undefined || fourth === undefined
        ? Effect.dieMessage("the N4 conversation lacks a member")
        : Effect.succeed({
            engine,
            membership,
            third,
            fourth,
          } satisfies N4Foundation);
    }),
  );

/**
 * The items of a list a certificate needs at least one of.
 * @param items The certificate's signatures or votes.
 * @returns The same items as a non-empty tuple.
 */
const nonEmpty = <A>(items: readonly A[]) => {
  const [first, ...rest] = items;
  return first === undefined
    ? Effect.dieMessage("a certificate needs at least one entry")
    : Effect.succeed([first, ...rest] as const);
};

/**
 * Builds a group conversation's certified FIRST_MESSAGE head and an uncertified
 * MESSAGE the remote member authors at it, action-signed by every member in
 * decoded AgentId order.
 * @param fixture Endpoint whose local and remote members sign.
 * @param group The group conversation and its other members.
 * @returns The certified head and the action-certified successor.
 */
const buildGroupHistory = (
  fixture: RecoveryFixture,
  group: GroupFoundation,
): Effect.Effect<GroupHistory> =>
  Effect.gen(function* () {
    const conversationId = group.membership.descriptor.conversationId;
    const members = [fixture.local, ...group.others].sort((left, right) =>
      compareAgentIds(left.card.agentId, right.card.agentId),
    );
    const anchor: InitialAnchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "initial_anchor_body",
      conversationId,
      membershipHash: group.membership.hash,
      routerInstanceId: oldRouterInstanceId,
    };
    const anchorHash = yield* hashAnchor(anchor);
    const signedByAll = (
      actionHash: Effect.Effect.Success<ReturnType<typeof hashAction>>,
    ) =>
      Effect.forEach(
        members,
        (member) =>
          signEvidence(member, {
            sharnessVersion: WIRE_VERSION,
            kind: "action_signature",
            signerAgentId: member.card.agentId,
            actionHash,
          }),
        { concurrency: 1 },
      ).pipe(Effect.flatMap(nonEmpty));
    const intent = (text: string) =>
      Effect.map(
        mintMsgId(),
        (msgId): MessageIntent => ({
          sharnessVersion: WIRE_VERSION,
          kind: "message_intent",
          conversationId,
          membershipHash: group.membership.hash,
          authorAgentId: fixture.remote.card.agentId,
          msgId,
          content: [{ type: "text", text }],
        }),
      );
    const headIntent = yield* intent("certified group head");
    const headAction: FirstMessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "FIRST_MESSAGE",
      conversationId,
      membership: group.membership.descriptor,
      anchor,
      previousRecordHash: null,
      messageIntent: headIntent,
      messageIntentHash: yield* hashMessageIntent(headIntent),
    };
    const headActionHash = yield* hashAction(headAction);
    const headCore: RecordCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "record_core",
      membership: group.membership.descriptor,
      anchorHash,
      action: headAction,
      actionHash: headActionHash,
    };
    const headRecordHash = yield* hashRecord(headCore);
    const certifiedHead: CertifiedRecord = {
      sharnessVersion: WIRE_VERSION,
      kind: "certified_record",
      actionCertifiedRecord: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_certified_record",
        recordHash: headRecordHash,
        recordCore: headCore,
        routerAnchor: anchor,
        actionCertificate: {
          sharnessVersion: WIRE_VERSION,
          kind: "action_certificate",
          actionHash: headActionHash,
          signatures: yield* signedByAll(headActionHash),
        },
      },
      durabilityCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_certificate",
        recordHash: headRecordHash,
        votes: yield* Effect.forEach(
          members.slice(0, quorumThreshold(members.length)),
          (member) =>
            signEvidence(member, {
              sharnessVersion: WIRE_VERSION,
              kind: "durability_vote",
              signerAgentId: member.card.agentId,
              conversationId,
              membershipHash: group.membership.hash,
              recordHash: headRecordHash,
            }),
          { concurrency: 1 },
        ).pipe(Effect.flatMap(nonEmpty)),
      },
    };
    const successorIntent = yield* intent("staged group successor");
    const successorAction: MessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "MESSAGE",
      conversationId,
      membershipHash: group.membership.hash,
      anchorHash,
      previousRecordHash: headRecordHash,
      messageIntent: successorIntent,
      messageIntentHash: yield* hashMessageIntent(successorIntent),
    };
    const successorActionHash = yield* hashAction(successorAction);
    const successorCore: RecordCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "record_core",
      membershipHash: group.membership.hash,
      anchorHash,
      action: successorAction,
      actionHash: successorActionHash,
    };
    const successor: ActionCertifiedRecordValue = {
      sharnessVersion: WIRE_VERSION,
      kind: "action_certified_record",
      recordHash: yield* hashRecord(successorCore),
      recordCore: successorCore,
      routerAnchor: anchor,
      actionCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_certificate",
        actionHash: successorActionHash,
        signatures: yield* signedByAll(successorActionHash),
      },
    };
    return { certifiedHead, successor };
  }).pipe(Effect.orDie);

const buildN4PartialHistory = (
  fixture: RecoveryFixture,
  n4: N4Foundation,
): Effect.Effect<N4PartialHistory> =>
  Effect.gen(function* () {
    const headIntent: MessageIntent = {
      sharnessVersion: WIRE_VERSION,
      kind: "message_intent",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      authorAgentId: fixture.remote.card.agentId,
      msgId: yield* mintMsgId(),
      content: [{ type: "text", text: "certified N4 head" }],
    };
    const headAnchor: InitialAnchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "initial_anchor_body",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      routerInstanceId: oldRouterInstanceId,
    };
    const headAction: FirstMessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "FIRST_MESSAGE",
      conversationId: n4.membership.descriptor.conversationId,
      membership: n4.membership.descriptor,
      anchor: headAnchor,
      previousRecordHash: null,
      messageIntent: headIntent,
      messageIntentHash: yield* hashMessageIntent(headIntent),
    };
    const headActionHash = yield* hashAction(headAction);
    const headLocalAction = yield* signEvidence(fixture.local, {
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: fixture.local.card.agentId,
      actionHash: headActionHash,
    });
    const headRemoteAction = yield* signEvidence(fixture.remote, {
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: fixture.remote.card.agentId,
      actionHash: headActionHash,
    });
    const headThirdAction = yield* signEvidence(n4.third, {
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: n4.third.card.agentId,
      actionHash: headActionHash,
    });
    const headFourthAction = yield* signEvidence(n4.fourth, {
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: n4.fourth.card.agentId,
      actionHash: headActionHash,
    });
    const headAnchorHash = yield* hashAnchor(headAnchor);
    const headCore: RecordCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "record_core",
      membership: n4.membership.descriptor,
      anchorHash: headAnchorHash,
      action: headAction,
      actionHash: headActionHash,
    };
    const headRecordHash = yield* hashRecord(headCore);
    const headLocalDurability = yield* signEvidence(fixture.local, {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_vote",
      signerAgentId: fixture.local.card.agentId,
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      recordHash: headRecordHash,
    });
    const headRemoteDurability = yield* signEvidence(fixture.remote, {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_vote",
      signerAgentId: fixture.remote.card.agentId,
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      recordHash: headRecordHash,
    });
    const headFourthDurability = yield* signEvidence(n4.fourth, {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_vote",
      signerAgentId: n4.fourth.card.agentId,
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      recordHash: headRecordHash,
    });
    const certifiedHead: CertifiedRecord = {
      sharnessVersion: WIRE_VERSION,
      kind: "certified_record",
      actionCertifiedRecord: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_certified_record",
        recordHash: headRecordHash,
        recordCore: headCore,
        routerAnchor: headAnchor,
        actionCertificate: {
          sharnessVersion: WIRE_VERSION,
          kind: "action_certificate",
          actionHash: headActionHash,
          signatures: [
            headLocalAction,
            headRemoteAction,
            headThirdAction,
            headFourthAction,
          ],
        },
      },
      durabilityCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_certificate",
        recordHash: headRecordHash,
        votes: [
          headLocalDurability,
          headRemoteDurability,
          headFourthDurability,
        ],
      },
    };

    const successorIntent: MessageIntent = {
      sharnessVersion: WIRE_VERSION,
      kind: "message_intent",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      authorAgentId: n4.fourth.card.agentId,
      msgId: yield* mintMsgId(),
      content: [{ type: "text", text: "partially disseminated successor" }],
    };
    const successorAction: MessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "MESSAGE",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      anchorHash: headAnchorHash,
      previousRecordHash: headRecordHash,
      messageIntent: successorIntent,
      messageIntentHash: yield* hashMessageIntent(successorIntent),
    };
    const successorActionHash = yield* hashAction(successorAction);
    const successorRemoteAction = yield* signEvidence(fixture.remote, {
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: fixture.remote.card.agentId,
      actionHash: successorActionHash,
    });
    const successorThirdAction = yield* signEvidence(n4.third, {
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: n4.third.card.agentId,
      actionHash: successorActionHash,
    });
    const successorFourthAction = yield* signEvidence(n4.fourth, {
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: n4.fourth.card.agentId,
      actionHash: successorActionHash,
    });
    const successorCore: RecordCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "record_core",
      membershipHash: n4.membership.hash,
      anchorHash: headAnchorHash,
      action: successorAction,
      actionHash: successorActionHash,
    };
    const successorRecordHash = yield* hashRecord(successorCore);
    const stagedSuccessor: ActionCertifiedRecordValue = {
      sharnessVersion: WIRE_VERSION,
      kind: "action_certified_record",
      recordHash: successorRecordHash,
      recordCore: successorCore,
      routerAnchor: headAnchor,
      actionCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_certificate",
        actionHash: successorActionHash,
        signatures: [
          successorRemoteAction,
          successorThirdAction,
          successorFourthAction,
        ],
      },
    };
    const successorThirdDurability = yield* signEvidence(n4.third, {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_vote",
      signerAgentId: n4.third.card.agentId,
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      recordHash: successorRecordHash,
    });
    const successorRemoteDurability = yield* signEvidence(fixture.remote, {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_vote",
      signerAgentId: fixture.remote.card.agentId,
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      recordHash: successorRecordHash,
    });
    const successorFourthDurability = yield* signEvidence(n4.fourth, {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_vote",
      signerAgentId: n4.fourth.card.agentId,
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      recordHash: successorRecordHash,
    });
    const certifiedSuccessor: CertifiedRecord = {
      sharnessVersion: WIRE_VERSION,
      kind: "certified_record",
      actionCertifiedRecord: stagedSuccessor,
      durabilityCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_certificate",
        recordHash: successorRecordHash,
        votes: [
          successorRemoteDurability,
          successorThirdDurability,
          successorFourthDurability,
        ],
      },
    };
    return { certifiedHead, stagedSuccessor, certifiedSuccessor };
  }).pipe(Effect.orDie);

/**
 * A certified MESSAGE in the N4 conversation that the fourth member authors at
 * `previousRecordHash` under a completed re-anchor, certified by the remote,
 * third and fourth members' action signatures and durability votes.
 * @param fixture Endpoint whose remote member signs.
 * @param n4 N4 conversation and its third and fourth members.
 * @param position Where the message sits.
 * @param position.routerAnchor Completed re-anchor the message binds.
 * @param position.previousRecordHash Record the message extends.
 * @returns The certified record.
 */
const certifiedN4MessageAt = (
  fixture: RecoveryFixture,
  n4: N4Foundation,
  position: {
    readonly routerAnchor: CompletedReanchor;
    readonly previousRecordHash: typeof RecordHash.Type;
  },
): Effect.Effect<CertifiedRecord> =>
  Effect.gen(function* () {
    const signers = [fixture.remote, n4.third, n4.fourth].sort((left, right) =>
      compareAgentIds(left.card.agentId, right.card.agentId),
    );
    const intent: MessageIntent = {
      sharnessVersion: WIRE_VERSION,
      kind: "message_intent",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      authorAgentId: n4.fourth.card.agentId,
      msgId: yield* mintMsgId(),
      content: [{ type: "text", text: "message under the new anchor" }],
    };
    const action: MessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "MESSAGE",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      anchorHash: position.routerAnchor.anchorHash,
      previousRecordHash: position.previousRecordHash,
      messageIntent: intent,
      messageIntentHash: yield* hashMessageIntent(intent),
    };
    const actionHash = yield* hashAction(action);
    const recordCore: RecordCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "record_core",
      membershipHash: n4.membership.hash,
      anchorHash: position.routerAnchor.anchorHash,
      action,
      actionHash,
    };
    const recordHash = yield* hashRecord(recordCore);
    const [firstSignature, ...signatures] = yield* Effect.forEach(
      signers,
      (signer) =>
        signEvidence(signer, {
          sharnessVersion: WIRE_VERSION,
          kind: "action_signature",
          signerAgentId: signer.card.agentId,
          actionHash,
        }),
      { concurrency: 1 },
    );
    const [firstVote, ...votes] = yield* Effect.forEach(
      signers,
      (signer) =>
        signEvidence(signer, {
          sharnessVersion: WIRE_VERSION,
          kind: "durability_vote",
          signerAgentId: signer.card.agentId,
          conversationId: n4.membership.descriptor.conversationId,
          membershipHash: n4.membership.hash,
          recordHash,
        }),
      { concurrency: 1 },
    );
    if (firstSignature === undefined || firstVote === undefined) {
      return yield* Effect.dieMessage("a certificate needs signers");
    }
    const record: CertifiedRecord = {
      sharnessVersion: WIRE_VERSION,
      kind: "certified_record",
      actionCertifiedRecord: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_certified_record",
        recordHash,
        recordCore,
        routerAnchor: position.routerAnchor,
        actionCertificate: {
          sharnessVersion: WIRE_VERSION,
          kind: "action_certificate",
          actionHash,
          signatures: [firstSignature, ...signatures],
        },
      },
      durabilityCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_certificate",
        recordHash,
        votes: [firstVote, ...votes],
      },
    };
    return record;
  }).pipe(Effect.orDie);

const decodeCatchUpRequest = (
  message: SignedMessage,
): Effect.Effect<CatchUpRequest> =>
  carriedBy(message).pipe(
    Effect.flatMap((carried) =>
      carried.kind === "catch_up_request"
        ? Effect.succeed(carried)
        : Effect.dieMessage(
            `expected catch-up request, received ${carried.kind}`,
          ),
    ),
  );

/** The action proposal `body` carries; any other body is a defect naming `expected`. */
function actionProposalIn(body: DecodedOuterBody, expected: string) {
  if (body.kind === "direct" && body.packet.kind === "action_proposal") {
    return Effect.succeed(body.packet);
  }
  const received = body.kind === "evidence" ? body.kind : body.packet.kind;
  return Effect.dieMessage(`expected ${expected}, received ${received}`);
}

/**
 * Take the bodies `outbound` carries up to and including the next action
 * proposal. A proposal follows its head's certified record, so it can reach
 * the queue after a drain returns.
 * @param outbound Router queue the engine forwards to.
 * @param taken Bodies already taken, in order.
 * @returns Every body through the proposal.
 */
function takeBodiesThroughActionProposal(
  outbound: Queue.Queue<SignedMessage>,
  taken: readonly DecodedOuterBody[] = [],
): Effect.Effect<readonly DecodedOuterBody[]> {
  return Queue.take(outbound).pipe(
    Effect.flatMap((message) => openForwarded(message)),
    Effect.flatMap((body) =>
      body.kind === "direct" && body.packet.kind === "action_proposal"
        ? Effect.succeed([...taken, body])
        : takeBodiesThroughActionProposal(outbound, [...taken, body]),
    ),
    Effect.orDie,
  );
}

/**
 * The next action proposal a fixture Router forwarded for one conversation.
 * Proposals of other conversations, such as an author's resends of a proposal
 * whose action certificate is incomplete, are skipped.
 * @param outbound Fixture Router queue.
 * @param conversationId Conversation whose proposal is awaited.
 * @returns The proposal and the envelope that carried it.
 */
function takeActionProposalIn(
  outbound: Queue.Queue<SignedMessage>,
  conversationId: string,
): Effect.Effect<QueuedActionProposal> {
  return takeActionProposalAfterEvidence(outbound).pipe(
    Effect.flatMap((taken) =>
      taken.proposal.action.conversationId === conversationId
        ? Effect.succeed(taken)
        : takeActionProposalIn(outbound, conversationId),
    ),
  );
}

/**
 * Drain a fixture Router queue and count the action proposals it held for one
 * conversation.
 * @param outbound Fixture Router queue.
 * @param conversationId Conversation whose proposals are counted.
 * @returns How many of the drained envelopes propose in that conversation.
 */
function directProposalsIn(
  outbound: Queue.Queue<SignedMessage>,
  conversationId: string,
): Effect.Effect<number> {
  return Queue.takeAll(outbound).pipe(
    Effect.flatMap((messages) =>
      Effect.forEach(messages, (message) => openForwarded(message), {
        concurrency: 1,
      }),
    ),
    Effect.map(
      (bodies) =>
        bodies.filter(
          (body) =>
            body.kind !== "evidence" &&
            body.packet.kind === "action_proposal" &&
            body.packet.action.conversationId === conversationId,
        ).length,
    ),
    Effect.orDie,
  );
}

/**
 * Take the next action proposal from `outbound`, skipping the evidence,
 * action-certified-record and certified-head envelopes queued ahead of it.
 * @param outbound Router queue the engine forwards to.
 * @returns The proposal envelope and its decoded packet; any other direct
 *     packet is a defect.
 */
function takeActionProposalAfterEvidence(
  outbound: Queue.Queue<SignedMessage>,
): Effect.Effect<QueuedActionProposal> {
  return Queue.take(outbound).pipe(
    Effect.flatMap((message) =>
      openForwarded(message).pipe(
        Effect.flatMap((body) => {
          if (
            body.kind === "evidence" ||
            body.packet.kind === "action_certified_record" ||
            body.packet.kind === "certified_record"
          ) {
            return takeActionProposalAfterEvidence(outbound);
          }
          return body.packet.kind === "action_proposal"
            ? Effect.succeed({ message, proposal: body.packet })
            : Effect.dieMessage(
                `expected re-anchored action proposal, received ${body.packet.kind}`,
              );
        }),
      ),
    ),
    Effect.orDie,
  );
}

const stageCatchUpOutbound = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    const request: CatchUpRequest = {
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_request",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      requesterAgentId: fixture.local.card.agentId,
      knownRecordHash: null,
      knownAnchorHash: null,
    };
    const message = yield* signOuterPacket({
      packet: request,
      membership: fixture.membership,
      agentCard: fixture.local.card,
      signingAuthority: fixture.local.authority,
    });
    const canonicalSignedMessage = yield* encodeCanonical(
      SignedMessage,
      message,
    );
    return yield* fixture.store.run(
      testStore.enqueueOutbound({
        conversationId: request.conversationId,
        envelopeId: message.envelopeId,
        canonicalSignedMessage,
        wireVersion: SignedMessage.wireVersion(message),
      }),
    );
  }).pipe(Effect.orDie);

const directPacketIngressFrom = (input: {
  readonly membership: VerifiedMembership;
  readonly sender: SigningIdentity;
  readonly packet: DirectPacket;
  readonly routerInstanceId: typeof RouterInstanceId.Type;
}): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> =>
  signOuterPacket({
    packet: input.packet,
    membership: input.membership,
    agentCard: input.sender.card,
    signingAuthority: input.sender.authority,
  }).pipe(
    Effect.map(
      (message): RouterWorkerIngress<DecodedOuterBody> => ({
        routerInstanceId: input.routerInstanceId,
        message,
        senderCard: input.sender.card,
        payload: { kind: "direct", packet: input.packet },
      }),
    ),
    Effect.orDie,
  );

const certifiedRecordIngress = (
  fixture: RecoveryFixture,
): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> =>
  signOuterPacket({
    packet: fixture.certifiedRecord,
    membership: fixture.membership,
    agentCard: fixture.remote.card,
    signingAuthority: fixture.remote.authority,
  }).pipe(
    Effect.map(
      (message): RouterWorkerIngress<DecodedOuterBody> => ({
        routerInstanceId: oldRouterInstanceId,
        message,
        senderCard: fixture.remote.card,
        payload: { kind: "direct", packet: fixture.certifiedRecord },
      }),
    ),
    Effect.orDie,
  );

const retainCertifiedRecord = (fixture: RecoveryFixture) =>
  certifiedRecordIngress(fixture).pipe(
    Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    Effect.asVoid,
  );

const restartWithNonLexicalAgentOrder = () =>
  Effect.gen(function* () {
    const fixture = yield* makeNonLexicalAgentOrderFixture;
    expect(
      compareAgentIds(fixture.local.card.agentId, fixture.remote.card.agentId),
    ).toBeLessThan(0);
    expect(fixture.remote.card.agentId < fixture.local.card.agentId).toBe(true);

    yield* retainCertifiedRecord(fixture);
    const restarted = yield* makeEndpointEngine(fixture.input).pipe(
      Effect.provideService(SqlClient.SqlClient, fixture.store.sql),
    );
    expect((yield* restarted.readPendingMessages())[0]?.message.author).toEqual(
      agentViewOf(fixture.remote.card),
    );

    expect(
      (yield* restarted.readPendingMessages()).map(
        (message) => message.recordHash,
      ),
    ).toEqual([fixture.certifiedRecord.actionCertifiedRecord.recordHash]);
  });

const stageAttachedDissemination = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    yield* retainCertifiedRecord(fixture);
    const [record] = (yield* fixture.store.run(testStore.recover()))
      .stagedRecords;
    if (record === undefined) {
      return yield* Effect.dieMessage("The fixture has no staged record");
    }
    const message = yield* signOuterPacket({
      packet: fixture.certifiedRecord.actionCertifiedRecord,
      membership: fixture.membership,
      agentCard: fixture.local.card,
      signingAuthority: fixture.local.authority,
    });
    return yield* fixture.store.run(
      testStore.stageRecordAndOutbound(record, {
        conversationId: record.conversationId,
        envelopeId: message.envelopeId,
        wireVersion: WIRE_VERSION,
        canonicalSignedMessage: yield* encodeCanonical(SignedMessage, message),
      }),
    );
  }).pipe(Effect.orDie);

const catchUpPageIngress = (
  fixture: RecoveryFixture,
  request: CatchUpRequest,
): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> =>
  Effect.gen(function* () {
    const recordHash = fixture.certifiedRecord.actionCertifiedRecord.recordHash;
    const attestation = yield* signEvidenceMessage({
      statement: {
        sharnessVersion: WIRE_VERSION,
        kind: "catch_up_attestation",
        signerAgentId: fixture.remote.card.agentId,
        request,
        itemKind: "certified_record",
        itemHash: recordHash,
        hasMore: false,
      },
      agentCard: fixture.remote.card,
      signingAuthority: fixture.remote.authority,
    });
    const packet: CatchUpPage = {
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_page",
      request,
      item: fixture.certifiedRecord,
      hasMore: false,
      attestation: yield* Schema.encode(SignedMessage)(attestation),
    };
    const message = yield* signOuterPacket({
      packet,
      membership: fixture.membership,
      agentCard: fixture.remote.card,
      signingAuthority: fixture.remote.authority,
    });
    const ingress: RouterWorkerIngress<DecodedOuterBody> = {
      routerInstanceId: newRouterInstanceId,
      message,
      senderCard: fixture.remote.card,
      payload: { kind: "direct", packet },
    };
    return ingress;
  }).pipe(Effect.orDie);

const catchUpPageIngressFrom = (input: {
  readonly membership: VerifiedMembership;
  readonly responder: SigningIdentity;
  readonly request: CatchUpRequest;
  readonly item: CatchUpPage["item"];
  readonly routerInstanceId: typeof RouterInstanceId.Type;
}): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> =>
  Effect.gen(function* () {
    const attestation = yield* signEvidenceMessage({
      statement: {
        sharnessVersion: WIRE_VERSION,
        kind: "catch_up_attestation",
        signerAgentId: input.responder.card.agentId,
        request: input.request,
        itemKind: input.item.kind,
        itemHash:
          input.item.kind === "certified_record"
            ? input.item.actionCertifiedRecord.recordHash
            : input.item.anchorHash,
        hasMore: false,
      },
      agentCard: input.responder.card,
      signingAuthority: input.responder.authority,
    });
    const packet: CatchUpPage = {
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_page",
      request: input.request,
      item: input.item,
      hasMore: false,
      attestation: yield* Schema.encode(SignedMessage)(attestation),
    };
    return yield* directPacketIngressFrom({
      membership: input.membership,
      sender: input.responder,
      packet,
      routerInstanceId: input.routerInstanceId,
    });
  }).pipe(Effect.orDie);

const catchUpIncompleteIngressFrom = (input: {
  readonly membership: VerifiedMembership;
  readonly responder: SigningIdentity;
  readonly request: CatchUpRequest;
  readonly routerInstanceId: typeof RouterInstanceId.Type;
}): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> =>
  Effect.gen(function* () {
    const attestation = yield* signEvidenceMessage({
      statement: {
        sharnessVersion: WIRE_VERSION,
        kind: "catch_up_attestation",
        signerAgentId: input.responder.card.agentId,
        request: input.request,
        itemKind: "incomplete",
        itemHash: null,
        hasMore: false,
      },
      agentCard: input.responder.card,
      signingAuthority: input.responder.authority,
    });
    const packet: CatchUpIncomplete = {
      sharnessVersion: WIRE_VERSION,
      kind: "catch_up_incomplete",
      request: input.request,
      attestation: yield* Schema.encode(SignedMessage)(attestation),
    };
    const message = yield* signOuterPacket({
      packet,
      membership: input.membership,
      agentCard: input.responder.card,
      signingAuthority: input.responder.authority,
    });
    const ingress: RouterWorkerIngress<DecodedOuterBody> = {
      routerInstanceId: input.routerInstanceId,
      message,
      senderCard: input.responder.card,
      payload: { kind: "direct", packet },
    };
    return ingress;
  }).pipe(Effect.orDie);

const catchUpIncompleteIngress = (
  fixture: RecoveryFixture,
  request: CatchUpRequest,
): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> =>
  catchUpIncompleteIngressFrom({
    membership: fixture.membership,
    responder: fixture.remote,
    request,
    routerInstanceId: newRouterInstanceId,
  });

const decodeReanchorVote = (
  message: SignedMessage,
): Effect.Effect<ReanchorVote> =>
  carriedBy(message).pipe(
    Effect.flatMap((carried) =>
      carried.kind === "reanchor_vote"
        ? Effect.succeed(carried)
        : Effect.dieMessage(
            `expected re-anchor vote, received ${carried.kind}`,
          ),
    ),
  );

const decodeEvidenceKind = (
  message: SignedMessage,
): Effect.Effect<EvidenceStatementValue["kind"]> =>
  openForwarded(message).pipe(
    Effect.flatMap((body) =>
      body.kind === "evidence"
        ? decodeCanonical(EvidenceStatement, body.message.body)
        : Effect.dieMessage("expected resumed evidence"),
    ),
    Effect.map((statement) => statement.kind),
    Effect.orDie,
  );

/**
 * Router ingress at the new Router instance carrying `statement`, signed by
 * the fixture's remote member and sent in that member's outer envelope.
 * @param fixture Endpoint the evidence is addressed to.
 * @param statement Evidence the remote member signs.
 * @returns The Router-ordered evidence.
 */
const peerEvidenceIngress = (
  fixture: RecoveryFixture,
  statement: EvidenceStatementValue,
): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> =>
  peerEvidenceIngressFrom({
    membership: fixture.membership,
    responder: fixture.remote,
    statement,
    routerInstanceId: newRouterInstanceId,
  });

const peerReanchorVoteIngress = (
  fixture: RecoveryFixture,
  proposal: ReanchorVote,
): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> =>
  peerEvidenceIngress(fixture, proposal);

/**
 * Router ingress carrying a re-anchor vote whose outer envelope `responder`
 * signs and addresses to `membership`. The vote's evidence is signed by
 * `evidenceSigner` when given, otherwise by `responder`.
 */
function peerReanchorVoteIngressFrom(input: {
  readonly membership: VerifiedMembership;
  readonly responder: SigningIdentity;
  readonly proposal: ReanchorVote;
  readonly routerInstanceId: typeof RouterInstanceId.Type;
  readonly evidenceSigner?: SigningIdentity;
}): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> {
  return peerEvidenceIngressFrom({ ...input, statement: input.proposal });
}

/**
 * Router ingress carrying `statement` in an outer envelope `responder` signs
 * and addresses to `membership`. The evidence is signed by `evidenceSigner`
 * when given, otherwise by `responder`.
 */
function peerEvidenceIngressFrom(input: {
  readonly membership: VerifiedMembership;
  readonly responder: SigningIdentity;
  readonly statement: EvidenceStatementValue;
  readonly routerInstanceId: typeof RouterInstanceId.Type;
  readonly evidenceSigner?: SigningIdentity;
}): Effect.Effect<RouterWorkerIngress<DecodedOuterBody>> {
  const signer = input.evidenceSigner ?? input.responder;
  return Effect.gen(function* () {
    const evidence = yield* signEvidenceMessage({
      statement: {
        ...input.statement,
        signerAgentId: signer.card.agentId,
      },
      agentCard: signer.card,
      signingAuthority: signer.authority,
    });
    const message = yield* signOuterEvidence({
      evidence,
      membership: input.membership,
      agentCard: input.responder.card,
      signingAuthority: input.responder.authority,
    });
    const ingress: RouterWorkerIngress<DecodedOuterBody> = {
      routerInstanceId: input.routerInstanceId,
      message,
      senderCard: input.responder.card,
      payload: { kind: "evidence", message: evidence },
    };
    return ingress;
  }).pipe(Effect.orDie);
}

/**
 * The peer's correctly signed vote for `reanchor`, naming the anchor hash its
 * body hashes to.
 * @param fixture Endpoint the vote is addressed to.
 * @param reanchor Re-anchor body the peer votes for.
 * @returns The vote as recovery ingress.
 */
const peerVoteFor = (fixture: RecoveryFixture, reanchor: ReanchorBody) =>
  hashAnchor(reanchor).pipe(
    Effect.orDie,
    Effect.flatMap((anchorHash) =>
      peerReanchorVoteIngress(fixture, {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_vote",
        signerAgentId: fixture.remote.card.agentId,
        anchorHash,
        reanchor,
      }),
    ),
  );

/**
 * A completed re-anchor of `reanchor` certified by `voters`, whose votes the
 * certificate lists in decoded AgentId order.
 * @param reanchor Re-anchor body every voter signs.
 * @param voters Members whose votes form the certificate, at least one.
 * @returns The completed re-anchor.
 */
const completedReanchorBy = (
  reanchor: ReanchorBody,
  voters: readonly SigningIdentity[],
) =>
  Effect.gen(function* () {
    const anchorHash = yield* hashAnchor(reanchor);
    const ordered = [...voters].sort((left, right) =>
      compareAgentIds(left.card.agentId, right.card.agentId),
    );
    const [first, ...rest] = yield* Effect.forEach(
      ordered,
      (voter) =>
        signEvidence(voter, {
          sharnessVersion: WIRE_VERSION,
          kind: "reanchor_vote",
          signerAgentId: voter.card.agentId,
          anchorHash,
          reanchor,
        }),
      { concurrency: 1 },
    );
    if (first === undefined) {
      return yield* Effect.dieMessage("a re-anchor certificate needs a vote");
    }
    const completed: CompletedReanchor = {
      sharnessVersion: WIRE_VERSION,
      kind: "completed_reanchor",
      anchorHash,
      reanchor,
      certificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_certificate",
        anchorHash,
        votes: [first, ...rest],
      },
    };
    return completed;
  }).pipe(Effect.orDie);

/**
 * A peer's correctly signed vote to re-anchor the fixture conversation at the
 * new Router instance. Only a recovery run started by a Router restart takes
 * such a vote.
 */
const peerReanchorVote = (fixture: RecoveryFixture) => {
  const { recordCore, recordHash } =
    fixture.certifiedRecord.actionCertifiedRecord;
  return peerVoteFor(fixture, {
    sharnessVersion: WIRE_VERSION,
    kind: "reanchor_body",
    conversationId: recordCore.action.conversationId,
    membershipHash: fixture.membership.hash,
    previousAnchorHash: recordCore.anchorHash,
    selectedRecordHash: recordHash,
    routerInstanceId: newRouterInstanceId,
  });
};

/**
 * Runs one recovery of the fixture at its unchanged Router instance to
 * completion, handing `during` the moment between the catch-up request and
 * its incomplete answer.
 * @param fixture Endpoint under recovery.
 * @param reason Discontinuity that started the recovery.
 * @param during Work run while the recovery is still active.
 * @returns What `during` returned, once recovery has completed.
 */
const runSameInstanceRecovery = <A, E>(
  fixture: RecoveryFixture,
  reason: RouterDiscontinuityReason,
  during: Effect.Effect<A, E>,
) =>
  Effect.gen(function* () {
    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(fixture, reason, oldRouterInstanceId);
    const request = yield* takeCatchUpRequest(recoveryOutbound);
    const result = yield* during;
    yield* catchUpIncompleteIngress(fixture, request).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Fiber.join(recovering);
    return result;
  });

const ignoresReanchorVoteDuringFeedGapRecovery = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const disposition = yield* runSameInstanceRecovery(
      fixture,
      "feed_gap",
      peerReanchorVote(fixture).pipe(
        Effect.flatMap((ingress) =>
          fixture.engine.acceptRouterIngress(ingress),
        ),
      ),
    );
    expect(disposition).toBe(ignoredDisposition);
  });

const ignoresReanchorVoteAfterRecovery = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* runSameInstanceRecovery(fixture, "feed_gap", Effect.void);
    const disposition = yield* peerReanchorVote(fixture).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    expect(disposition).toBe(ignoredDisposition);
  });

const completeRestartRecovery = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const staleOutbound = yield* stageCatchUpOutbound(fixture);
    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(fixture, "router_restarted", newRouterInstanceId);
    const firstOutbound = yield* Queue.take(recoveryOutbound);
    expect(firstOutbound.envelopeId).not.toBe(staleOutbound.envelopeId);
    const firstRequest = yield* decodeCatchUpRequest(firstOutbound);
    yield* catchUpPageIngress(fixture, firstRequest).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const terminalRequest = yield* takeCatchUpRequest(recoveryOutbound);
    yield* catchUpIncompleteIngress(fixture, terminalRequest).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const proposal = yield* takeReanchorVote(recoveryOutbound);
    expect(proposal.reanchor).toMatchObject({
      previousAnchorHash:
        fixture.certifiedRecord.actionCertifiedRecord.recordCore.anchorHash,
      selectedRecordHash:
        fixture.certifiedRecord.actionCertifiedRecord.recordHash,
      routerInstanceId: newRouterInstanceId,
    });
    yield* peerReanchorVoteIngress(fixture, proposal).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const completed = yield* Queue.take(recoveryOutbound).pipe(
      Effect.flatMap((message) => openForwarded(message)),
    );
    expect(completed).toMatchObject({
      kind: "direct",
      packet: {
        kind: "completed_reanchor",
        anchorHash: proposal.anchorHash,
      },
    });
    yield* Fiber.join(recovering);
    yield* fixture.engine.drainOutbound;
    const recovered = yield* fixture.store.run(testStore.recover());
    expect(recovered.certifiedRecords).toHaveLength(1);
    expect(recovered.outboundMessages).toHaveLength(0);
    expect(recovered.positions[0]?.currentAnchorHash).toBe(proposal.anchorHash);
    const resumedActionEvidence = yield* Queue.take(
      fixture.normalOutbound,
    ).pipe(Effect.flatMap(decodeEvidenceKind));
    const resumedDurabilityEvidence = yield* Queue.take(
      fixture.normalOutbound,
    ).pipe(Effect.flatMap(decodeEvidenceKind));
    expect(resumedActionEvidence).toBe(actionSignatureKind);
    expect(resumedDurabilityEvidence).toBe(durabilityVoteKind);

    const sending = yield* Effect.fork(
      fixture.engine.send(
        yield* authoredMessage(fixture, [
          { type: "text", text: "normal traffic resumes" },
        ]),
      ),
    );
    const { proposal: resumedProposal } =
      yield* takeActionProposalAfterEvidence(fixture.normalOutbound);
    expect(resumedProposal.action).toMatchObject({
      kind: "MESSAGE",
      anchorHash: proposal.anchorHash,
    });
    yield* Fiber.interrupt(sending);
  });

const reproposesPendingMessageAfterRestart = () =>
  Effect.gen(function* () {
    const pendingOutboundIds = yield* Queue.unbounded<string>();
    const holdOutbound = yield* Ref.make(false);
    const fixture = yield* makeFixtureWithRouter(
      makeHeldRouter({ holdOutbound, pendingOutboundIds }),
    );
    yield* retainCertifiedRecord(fixture);
    yield* fixture.engine.drainOutbound.pipe(Effect.orDie);
    yield* Ref.set(holdOutbound, true);
    const sending = yield* Effect.fork(
      fixture.engine.send(
        yield* authoredMessage(fixture, [
          { type: "text", text: "survive Router restart" },
        ]),
      ),
    );
    const headRecordOutboundId = yield* Queue.take(pendingOutboundIds);
    const staleOutboundId = yield* Queue.take(pendingOutboundIds);
    const before = yield* fixture.store.run(testStore.recover());
    expect(before.outboundMessages.map(({ envelopeId }) => envelopeId)).toEqual(
      [headRecordOutboundId, staleOutboundId],
    );
    const staleOutbound = before.outboundMessages[1];
    if (staleOutbound === undefined) {
      return yield* Effect.dieMessage("pending MESSAGE was not retained");
    }
    expect(staleOutbound.envelopeId).toBe(staleOutboundId);
    const staleProposal = yield* decodeCanonical(
      SignedMessage,
      staleOutbound.canonicalSignedMessage,
    ).pipe(
      Effect.flatMap((message) => openOuterBody(message, fixture.local)),
      Effect.flatMap((body) =>
        actionProposalIn(body, "old-instance action proposal"),
      ),
    );
    if (staleProposal.action.kind !== "MESSAGE") {
      return yield* Effect.dieMessage("expected an old-instance MESSAGE");
    }
    const msgId = staleProposal.action.messageIntent.msgId;
    expect(staleProposal.action.anchorHash).toBe(
      fixture.certifiedRecord.actionCertifiedRecord.recordCore.anchorHash,
    );
    const staleActionHash = yield* hashAction(staleProposal.action);

    yield* Queue.takeAll(fixture.normalOutbound);
    yield* Ref.set(holdOutbound, false);
    const resumedOutbound = fixture.normalOutbound;
    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(fixture, "router_restarted", newRouterInstanceId);
    const request = yield* takeCatchUpRequest(recoveryOutbound);
    yield* catchUpIncompleteIngress(fixture, request).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const reanchor = yield* takeReanchorVote(recoveryOutbound);
    yield* peerReanchorVoteIngress(fixture, reanchor).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Queue.take(recoveryOutbound);
    yield* Fiber.join(recovering);
    yield* Ref.set(holdOutbound, false);
    yield* fixture.engine.drainOutbound;

    const { message: reproposedMessage, proposal: reproposed } =
      yield* takeActionProposalAfterEvidence(resumedOutbound);
    if (reproposed.action.kind !== "MESSAGE") {
      return yield* Effect.dieMessage("expected a re-anchored MESSAGE");
    }
    expect(reproposedMessage.envelopeId).not.toBe(staleOutbound.envelopeId);
    expect(reproposed.action.messageIntent.msgId).toBe(msgId);
    expect(reproposed.action.messageIntentHash).toBe(
      staleProposal.action.messageIntentHash,
    );
    expect(reproposed.action.previousRecordHash).toBe(
      staleProposal.action.previousRecordHash,
    );
    expect(reproposed.action.anchorHash).toBe(reanchor.anchorHash);
    expect(yield* hashAction(reproposed.action)).not.toBe(staleActionHash);
    expect(
      "outbound" in
        (yield* fixture.store.run(testStore.beginOutbound(staleOutboundId))),
    ).toBe(false);
    expect(
      (yield* fixture.store.run(testStore.recover())).outboundMessages,
    ).toHaveLength(0);
    expect(yield* Queue.size(pendingOutboundIds)).toBe(0);
    yield* Fiber.interrupt(sending);
  });

const recoverSameRouterInstance = (reason: RouterDiscontinuityReason) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const sending = yield* Effect.fork(
      fixture.engine.send(
        yield* authoredMessage(fixture, [
          { type: "text", text: "retain this proposal" },
        ]),
      ),
    );
    yield* Queue.take(fixture.normalOutbound);
    const retained = yield* stageCatchUpOutbound(fixture);
    yield* runSameInstanceRecovery(fixture, reason, Effect.void);
    const resumed = yield* Queue.take(fixture.recoveryOutbound);
    expect(yield* encodeCanonical(SignedMessage, resumed)).toEqual(
      retained.canonicalSignedMessage,
    );
    yield* Fiber.interrupt(sending);
  });

const recoverColdStartAtUnchangedInstance = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const retained = yield* stageCatchUpOutbound(fixture);
    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(fixture, "router_restarted", oldRouterInstanceId);
    const request = yield* takeCatchUpRequest(recoveryOutbound);
    yield* catchUpIncompleteIngressFrom({
      membership: fixture.membership,
      responder: fixture.remote,
      request,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Fiber.join(recovering);
    const resumed = yield* Queue.take(recoveryOutbound);
    expect(yield* Queue.size(recoveryOutbound)).toBe(0);
    const recovered = yield* fixture.store.run(testStore.recover());
    expect(recovered.stagedReanchors).toHaveLength(0);
    expect(recovered.anchors).toHaveLength(1);
    expect(recovered.positions[0]?.currentAnchorHash).toBe(
      fixture.certifiedRecord.actionCertifiedRecord.recordCore.anchorHash,
    );
    expect(resumed.envelopeId).toBe(retained.envelopeId);
  });

const takeAllEnvelopeIds = (queue: Queue.Queue<SignedMessage>) =>
  Queue.takeAll(queue).pipe(
    Effect.map((messages) =>
      Chunk.toReadonlyArray(
        Chunk.map(messages, (message) => message.envelopeId),
      ),
    ),
  );

const recoverMixedRouterInstances = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const n4 = yield* addN4Foundation(fixture, newRouterInstanceId);
    const unchanged = fixture.membership.descriptor.conversationId;
    const changed = n4.membership.descriptor.conversationId;
    const retainedUnchanged = yield* stageCatchUpOutbound(fixture);
    const staleChanged = yield* stageCatchUpOutbound({
      ...fixture,
      membership: n4.membership,
    });
    const resumedOutbound = fixture.normalOutbound;
    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(
        { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
        "router_restarted",
        oldRouterInstanceId,
      );
    const requests = yield* Effect.forEach(
      [1, 2],
      () => takeCatchUpRequest(recoveryOutbound),
      { concurrency: 1 },
    );
    const unchangedRequest = requests.find(
      (request) => request.conversationId === unchanged,
    );
    const changedRequest = requests.find(
      (request) => request.conversationId === changed,
    );
    if (unchangedRequest === undefined || changedRequest === undefined) {
      return yield* Effect.dieMessage("expected one request per conversation");
    }
    yield* catchUpIncompleteIngressFrom({
      membership: fixture.membership,
      responder: fixture.remote,
      request: unchangedRequest,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );

    const certified = fixture.certifiedRecord.actionCertifiedRecord;
    const sameInstanceBody: ReanchorVote["reanchor"] = {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_body",
      conversationId: unchanged,
      membershipHash: fixture.membership.hash,
      previousAnchorHash: certified.recordCore.anchorHash,
      selectedRecordHash: certified.recordHash,
      routerInstanceId: oldRouterInstanceId,
    };
    yield* peerReanchorVoteIngressFrom({
      membership: fixture.membership,
      responder: fixture.remote,
      proposal: {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_vote",
        signerAgentId: fixture.remote.card.agentId,
        anchorHash: yield* hashAnchor(sameInstanceBody),
        reanchor: sameInstanceBody,
      },
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const resumedUnchanged = yield* Queue.take(recoveryOutbound);
    expect(resumedUnchanged.envelopeId).toBe(retainedUnchanged.envelopeId);
    expect(yield* Queue.size(recoveryOutbound)).toBe(0);

    yield* Effect.forEach(
      [fixture.remote, n4.third, n4.fourth],
      (responder) =>
        catchUpIncompleteIngressFrom({
          membership: n4.membership,
          responder,
          request: changedRequest,
          routerInstanceId: oldRouterInstanceId,
        }).pipe(
          Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
        ),
      { concurrency: 1, discard: true },
    );
    yield* Fiber.join(recovering);
    yield* n4.engine.drainOutbound;
    const after = yield* fixture.store.run(testStore.recover());
    expect(after.stagedReanchors).toEqual([]);
    expect(
      after.evidence.filter((evidence) => evidence.kind === "reanchor"),
    ).toEqual([]);
    expect(
      after.positions.find((position) => position.conversationId === unchanged)
        ?.currentAnchorHash,
    ).toBe(certified.recordCore.anchorHash);
    const changedAnchor = after.anchors.find(
      (stored) => stored.conversationId === changed,
    );
    if (changedAnchor === undefined) {
      return yield* Effect.dieMessage("changed conversation lost its anchor");
    }
    expect(
      (yield* decodeCanonical(InitialAnchorBody, changedAnchor.canonicalAnchor))
        .routerInstanceId,
    ).toBe(oldRouterInstanceId);
    const sentIds = [
      ...(yield* takeAllEnvelopeIds(resumedOutbound)),
      ...(yield* takeAllEnvelopeIds(recoveryOutbound)),
    ];
    expect(sentIds).not.toContain(staleChanged.envelopeId);
    expect(after.outboundMessages).toEqual([]);
  });

/** One Router send held until the test releases it. */
interface HeldSend {
  readonly envelopeId: string;
  readonly release: Deferred.Deferred<undefined>;
}

/**
 * Hold every Router send until the test releases it, then forward it once.
 * @param sends Receives each attempted send with its release latch.
 * @returns A fixture Router whose sends complete in test-chosen order.
 */
function makeGatedRouter(sends: Queue.Queue<HeldSend>) {
  return (context: FixtureRouterContext): EngineRouterPort => ({
    ...makeFixtureRouter(context),
    send: (envelopeId) =>
      Effect.gen(function* () {
        const release = yield* Deferred.make<undefined>();
        yield* Queue.offer(sends, { envelopeId, release });
        yield* Deferred.await(release);
        yield* forwardSorted(context, envelopeId);
      }),
  });
}

const releaseHeldSend = (held: HeldSend) =>
  Deferred.succeed(held.release, undefined);

/**
 * The catch-up request the fixture's remote member sends for its position.
 * @param fixture Endpoint the request is addressed to.
 * @param known The requester's position; FIRST_MESSAGE when omitted.
 * @param membership The conversation asked about, by default the fixture's
 *     direct conversation.
 * @returns The remote member's catch-up request.
 */
const peerCatchUpRequest = (
  fixture: RecoveryFixture,
  known: Pick<CatchUpRequest, "knownRecordHash" | "knownAnchorHash"> = {
    knownRecordHash: null,
    knownAnchorHash: null,
  },
  membership?: VerifiedMembership,
): CatchUpRequest => {
  const asked = membership ?? fixture.membership;
  return {
    sharnessVersion: WIRE_VERSION,
    kind: "catch_up_request",
    conversationId: asked.descriptor.conversationId,
    membershipHash: asked.hash,
    requesterAgentId: fixture.remote.card.agentId,
    ...known,
  };
};

/**
 * Queue one normal outbound envelope by answering a peer catch-up request.
 * @param fixture Engine that answers outside recovery.
 * @param known Position the peer's request names; by default the peer holds
 *     no history.
 * @returns The queued outbox row.
 */
const queuePeerCatchUpResponse = (
  fixture: RecoveryFixture,
  known: Pick<CatchUpRequest, "knownRecordHash" | "knownAnchorHash"> = {
    knownRecordHash: null,
    knownAnchorHash: null,
  },
) =>
  Effect.gen(function* () {
    const before = yield* fixture.store.run(testStore.recover());
    const request = peerCatchUpRequest(fixture, known);
    const ingress = yield* directPacketIngressFrom({
      membership: fixture.membership,
      sender: fixture.remote,
      packet: request,
      routerInstanceId: oldRouterInstanceId,
    });
    yield* fixture.engine.acceptRouterIngress(ingress);
    const after = yield* fixture.store.run(testStore.recover());
    const queued = after.outboundMessages.filter(
      (row) =>
        !before.outboundMessages.some(
          (prior) => prior.envelopeId === row.envelopeId,
        ),
    );
    const row = queued[0];
    if (queued.length !== 1 || row === undefined) {
      return yield* Effect.dieMessage("expected one queued catch-up response");
    }
    return row;
  }).pipe(Effect.orDie);

/**
 * A peer that asks from first message is answered with a page holding the
 * certified record the endpoint retains, read back from its stored rows.
 */
const answersFirstMessageCatchUpWithRetainedRecord = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);

    const answer = yield* queuePeerCatchUpResponse(fixture).pipe(
      Effect.flatMap(decodeQueuedCatchUpPage(fixture.local)),
      Effect.exit,
    );

    expect(answer, "answer to a catch-up request from first message").toEqual(
      Exit.succeed(
        expect.objectContaining({
          hasMore: false,
          item: fixture.certifiedRecord,
        }),
      ),
    );
  });

const recoverWhileDrainAwaitsWorker = () =>
  Effect.gen(function* () {
    const sends = yield* Queue.unbounded<HeldSend>();
    const fixture = yield* makeFixtureWithRouter(makeGatedRouter(sends));
    const queued = yield* queuePeerCatchUpResponse(fixture);
    const draining = yield* Effect.fork(fixture.engine.drainOutbound);
    const held = yield* Queue.take(sends);
    expect(held.envelopeId).toBe(queued.envelopeId);

    yield* fixture.engine.abandonVolatileFolds("feed_gap");
    yield* fixture.engine.recoverCertifiedHistory({
      reason: "feed_gap",
      anchor: { routerInstanceId: oldRouterInstanceId, pollCursor },
    });
    yield* releaseHeldSend(held);
    const requestSend = yield* Queue.take(sends);
    yield* releaseHeldSend(requestSend);
    yield* Fiber.join(draining);
    const sent = yield* Effect.forEach(
      yield* Queue.takeAll(fixture.recoveryOutbound),
      (message) => openForwarded(message),
      { concurrency: 1 },
    );

    expect(sent).toMatchObject([
      { kind: "direct", packet: { kind: "catch_up_incomplete" } },
      { kind: "direct", packet: { kind: "catch_up_request" } },
    ]);
    expect(
      (yield* fixture.store.run(testStore.recover())).outboundMessages,
    ).toEqual([]);
  });

const concurrentDrainsSendOnceInOrder = () =>
  Effect.gen(function* () {
    const sends = yield* Queue.unbounded<HeldSend>();
    const fixture = yield* makeFixtureWithRouter(makeGatedRouter(sends));
    const first = yield* queuePeerCatchUpResponse(fixture);
    const second = yield* queuePeerCatchUpResponse(fixture);

    const leading = yield* Effect.fork(fixture.engine.drainOutbound);
    const leadingFirst = yield* Queue.take(sends);
    const trailing = yield* Effect.fork(fixture.engine.drainOutbound);
    const trailingFirst = yield* Queue.take(sends);
    expect([leadingFirst.envelopeId, trailingFirst.envelopeId]).toEqual([
      first.envelopeId,
      first.envelopeId,
    ]);

    yield* releaseHeldSend(trailingFirst);
    const trailingSecond = yield* Queue.take(sends);
    expect(trailingSecond.envelopeId).toBe(second.envelopeId);
    yield* releaseHeldSend(leadingFirst);
    const leadingSecond = yield* Queue.take(sends);
    expect(leadingSecond.envelopeId).toBe(second.envelopeId);
    yield* releaseHeldSend(trailingSecond);
    yield* releaseHeldSend(leadingSecond);
    yield* Fiber.join(leading);
    yield* Fiber.join(trailing);

    expect(yield* takeAllEnvelopeIds(fixture.recoveryOutbound)).toEqual([
      first.envelopeId,
      second.envelopeId,
    ]);
    expect(
      (yield* fixture.store.run(testStore.recover())).outboundMessages,
    ).toEqual([]);
    yield* fixture.engine.drainOutbound;
    expect(yield* Queue.size(sends)).toBe(0);
  });

const staleDrainKeepsLaterHead = () =>
  Effect.gen(function* () {
    const sends = yield* Queue.unbounded<HeldSend>();
    const fixture = yield* makeFixtureWithRouter(makeGatedRouter(sends));
    const first = yield* queuePeerCatchUpResponse(fixture);

    const stale = yield* Effect.fork(fixture.engine.drainOutbound);
    const staleFirst = yield* Queue.take(sends);
    const current = yield* Effect.fork(fixture.engine.drainOutbound);
    const currentFirst = yield* Queue.take(sends);
    yield* releaseHeldSend(currentFirst);
    yield* Fiber.join(current);

    const second = yield* queuePeerCatchUpResponse(fixture);
    yield* releaseHeldSend(staleFirst);
    const staleSecond = yield* Queue.take(sends);
    expect(staleSecond.envelopeId).toBe(second.envelopeId);
    yield* releaseHeldSend(staleSecond);
    yield* Fiber.join(stale);

    expect(yield* takeAllEnvelopeIds(fixture.recoveryOutbound)).toEqual([
      first.envelopeId,
      second.envelopeId,
    ]);
    expect(
      (yield* fixture.store.run(testStore.recover())).outboundMessages,
    ).toEqual([]);
  });

interface RestartingRouterInput {
  readonly engineReady: Deferred.Deferred<EndpointEngine>;
  readonly restartPending: Ref.Ref<boolean>;
  readonly recovered: Deferred.Deferred<undefined>;
}

/**
 * Recover a Router restart on the sending fiber, as the worker does when a
 * send observes `router_restarted`, then fail that send as a discontinuity.
 * @param input Engine latch, one-shot restart flag, and recovery observers.
 * @returns A fixture Router whose first send runs recovery in place.
 */
function makeRestartingRouter(input: RestartingRouterInput) {
  return (context: FixtureRouterContext): EngineRouterPort => {
    const recover = (engine: EndpointEngine) =>
      engine.abandonVolatileFolds("router_restarted").pipe(
        Effect.zipRight(
          engine.recoverCertifiedHistory({
            reason: "router_restarted",
            anchor: { routerInstanceId: newRouterInstanceId, pollCursor },
          }),
        ),
        Effect.orDie,
        Effect.zipRight(Deferred.succeed(input.recovered, undefined)),
        Effect.zipRight(Effect.fail(new RouterWorkerDiscontinuityError())),
      );
    return {
      ...makeFixtureRouter(context),
      send: (envelopeId) =>
        Ref.getAndSet(input.restartPending, false).pipe(
          Effect.flatMap((restarted) =>
            restarted
              ? Deferred.await(input.engineReady).pipe(Effect.flatMap(recover))
              : forwardSorted(context, envelopeId),
          ),
        ),
    };
  };
}

const drainRecoversRouterRestartOnItsOwnFiber = () =>
  Effect.gen(function* () {
    const engineReady = yield* Deferred.make<EndpointEngine>();
    const restartPending = yield* Ref.make(true);
    const recovered = yield* Deferred.make<undefined>();
    const fixture = yield* makeFixtureWithRouter(
      makeRestartingRouter({ engineReady, restartPending, recovered }),
    );
    yield* Deferred.succeed(engineReady, fixture.engine);
    yield* queuePeerCatchUpResponse(fixture);

    const drained = yield* Effect.exit(fixture.engine.drainOutbound);
    yield* fixture.engine.drainOutbound;
    const request = yield* takeCatchUpRequest(fixture.recoveryOutbound);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, request),
    );

    expect(drained).toStrictEqual(
      Exit.fail(new EngineOutboundError({ reason: "network" })),
    );
    expect(yield* Deferred.isDone(recovered)).toBe(true);
    const after = yield* fixture.store.run(testStore.recover());
    expect(after.outboundMessages).toEqual([]);
    const storedAnchor = after.anchors[0];
    if (storedAnchor === undefined) {
      return yield* Effect.dieMessage("restarted anchor was not stored");
    }
    const restartedAnchor = yield* decodeCanonical(
      InitialAnchorBody,
      storedAnchor.canonicalAnchor,
    );
    expect(restartedAnchor.routerInstanceId).toBe(newRouterInstanceId);
  });

const recoverDisseminationObligations = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const stale = yield* stageAttachedDissemination(fixture);
    const before = yield* fixture.store.run(testStore.recover());
    expect(before.outboundMessages).toHaveLength(1);
    const resumedOutbound = fixture.normalOutbound;
    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(fixture, "router_restarted", newRouterInstanceId);
    const request = yield* takeCatchUpRequest(recoveryOutbound);
    yield* catchUpIncompleteIngress(fixture, request).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const proposal = yield* takeReanchorVote(recoveryOutbound);
    yield* peerReanchorVoteIngress(fixture, proposal).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Queue.take(recoveryOutbound).pipe(
      Effect.flatMap((message) => openForwarded(message)),
    );
    yield* Fiber.join(recovering);
    yield* fixture.engine.drainOutbound;
    const rebuilt = yield* Queue.take(resumedOutbound);
    expect(rebuilt.envelopeId).not.toBe(stale.envelopeId);
    expect(yield* openForwarded(rebuilt)).toMatchObject({
      kind: "direct",
      packet: { kind: "action_certified_record" },
    });
    expect(
      yield* Queue.take(resumedOutbound).pipe(
        Effect.flatMap(decodeEvidenceKind),
      ),
    ).toBe(actionSignatureKind);
    expect(
      yield* Queue.take(resumedOutbound).pipe(
        Effect.flatMap(decodeEvidenceKind),
      ),
    ).toBe(durabilityVoteKind);
    expect(yield* Queue.size(resumedOutbound)).toBe(0);
    const after = yield* fixture.store.run(testStore.recover());
    expect(after.outboundMessages).toHaveLength(0);
  });

const recoverWhileNormalSendIsHeld = () =>
  Effect.gen(function* () {
    const sendEntered = yield* Deferred.make<undefined>();
    const releaseSend = yield* Deferred.make<undefined>();
    const fixture = yield* makeFixtureWithRouter((context) => {
      const router = makeFixtureRouter(context);
      return {
        ...router,
        send: (envelopeId) =>
          Deferred.succeed(sendEntered, undefined).pipe(
            Effect.zipRight(Deferred.await(releaseSend)),
            Effect.zipRight(router.send(envelopeId)),
          ),
      };
    });
    const sending = yield* Effect.forkScoped(
      fixture.engine.send(
        yield* authoredMessage(fixture, [
          { type: "text", text: "retained during recovery" },
        ]),
      ),
    );
    yield* Deferred.await(sendEntered);
    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(fixture, "router_restarted", oldRouterInstanceId);
    yield* Fiber.join(recovering);
    expect(
      (yield* fixture.store.run(testStore.recover())).messageIntents,
    ).toHaveLength(1);
    yield* Deferred.succeed(releaseSend, undefined);
    const request = yield* takeCatchUpRequest(recoveryOutbound);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, request),
    );
    yield* fixture.engine.drainOutbound;
    expect(
      (yield* fixture.store.run(testStore.recover())).outboundMessages,
    ).toHaveLength(0);
    yield* Fiber.interrupt(sending);
  });

const restartEmptyConversation = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(fixture, "router_restarted", newRouterInstanceId);
    const request = yield* takeCatchUpRequest(recoveryOutbound);
    yield* catchUpIncompleteIngress(fixture, request).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Fiber.join(recovering);
    const recovered = yield* fixture.store.run(testStore.recover());
    expect(recovered.stagedReanchors).toHaveLength(0);
    expect(recovered.positions[0]?.headRecordHash).toBeUndefined();
    expect(recovered.anchors).toHaveLength(1);
    const storedAnchor = recovered.anchors[0];
    if (storedAnchor === undefined) {
      return yield* Effect.dieMessage("replacement anchor was not stored");
    }
    const restartedAnchor = yield* decodeCanonical(
      InitialAnchorBody,
      storedAnchor.canonicalAnchor,
    );
    expect(restartedAnchor.routerInstanceId).toBe(newRouterInstanceId);
  });

/**
 * Takes the next envelope a recovery sent, which must be a catch-up request.
 * @param outbound Queue the recovery sends to.
 * @returns The decoded catch-up request.
 */
function takeCatchUpRequest(
  outbound: Queue.Queue<SignedMessage>,
): Effect.Effect<CatchUpRequest> {
  return Queue.take(outbound).pipe(Effect.flatMap(decodeCatchUpRequest));
}

/**
 * Takes the next envelope a recovery sent, which must be a re-anchor vote.
 * @param outbound Queue the recovery sends to.
 * @returns The decoded vote statement.
 */
function takeReanchorVote(
  outbound: Queue.Queue<SignedMessage>,
): Effect.Effect<ReanchorVote> {
  return Queue.take(outbound).pipe(Effect.flatMap(decodeReanchorVote));
}

/**
 * Starts a restart recovery at the new Router instance and waits until it has
 * sent its first catch-up request, so the run is active.
 * @param fixture Endpoint under recovery.
 * @returns The running recovery, its queue, and its first catch-up request.
 */
const startRestartRecovery = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    const { recovery, outbound } = yield* forkRecovery(
      fixture,
      "router_restarted",
      newRouterInstanceId,
    );
    const request = yield* takeCatchUpRequest(outbound);
    return { recovery, outbound, request };
  });

/**
 * Runs a restart recovery that re-anchors the fixture conversation at the new
 * Router instance with the peer's vote, and takes the completed re-anchor the
 * endpoint relays. The store has committed the new anchor by then, so a later
 * restart recovery finds the conversation anchored.
 * @param fixture Endpoint under recovery, holding a retained certified record.
 * @returns The endpoint's re-anchor vote and the completed re-anchor it relayed.
 */
const reanchorUntilCompletionSend = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    const { outbound, request } = yield* startRestartRecovery(fixture);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, request),
    );
    const proposal = yield* takeReanchorVote(outbound);
    yield* deliverRecovery(
      fixture.engine,
      peerReanchorVoteIngress(fixture, proposal),
    );
    const completed = yield* takeNextCarrying(
      outbound,
      "completed_reanchor",
    ).pipe(
      Effect.flatMap(carriedBy),
      Effect.flatMap((carried) =>
        carried.kind === "completed_reanchor"
          ? Effect.succeed(carried)
          : Effect.dieMessage(
              `expected a completed re-anchor, received ${carried.kind}`,
            ),
      ),
    );
    return { proposal, completed };
  });

/**
 * Complete a re-anchor between identities whose encoded and decoded AgentId
 * orders differ, which the other re-anchor traces do not. The store returns
 * re-anchor votes in encoded order, and a certificate requires decoded-byte
 * order: a certificate carrying the store's order fails its own verification,
 * so the endpoint never sends it.
 */
const reanchorsWithNonLexicalAgentOrder = () =>
  Effect.gen(function* () {
    const fixture = yield* makeNonLexicalAgentOrderFixture;
    yield* retainCertifiedRecord(fixture);

    const { proposal, completed } = yield* reanchorUntilCompletionSend(fixture);

    expect(
      yield* verifyCompletedReanchor({
        completed,
        membership: fixture.membership,
      }),
    ).toBe(proposal.anchorHash);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(proposal.anchorHash);
  });

const ignoresRelayedCompletionForAnchoredConversation = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const { completed } = yield* reanchorUntilCompletionSend(fixture);
    const { recovery } = yield* startRestartRecovery(fixture);
    const disposition = yield* directPacketIngressFrom({
      membership: fixture.membership,
      sender: fixture.remote,
      packet: completed,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    expect(disposition).toBe(ignoredDisposition);
    yield* Fiber.interrupt(recovery);
  });

/**
 * The stable evidence message an outer evidence envelope carries, encoded as
 * a certificate holds it.
 * @param message Outer envelope whose body is evidence.
 * @returns The encoded inner evidence message.
 */
const carriedEvidence = (message: SignedMessage) =>
  openForwarded(message).pipe(
    Effect.flatMap((body) =>
      body.kind === "evidence"
        ? Schema.encode(SignedMessage)(body.message)
        : Effect.dieMessage("expected evidence"),
    ),
    Effect.orDie,
  );

/**
 * Starts a restart recovery of the fixture's retained record and answers its
 * catch-up request as incomplete, so the endpoint sends its own re-anchor vote
 * and waits for the peer's.
 * @param fixture Endpoint under recovery.
 * @returns The running recovery, its queue, the endpoint's vote, and that
 *     vote encoded.
 */
const proposeAtRestart = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    yield* retainCertifiedRecord(fixture);
    const { recovery, outbound, request } =
      yield* startRestartRecovery(fixture);
    yield* catchUpIncompleteIngress(fixture, request).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const voteMessage = yield* Queue.take(outbound);
    return {
      recovery,
      outbound,
      proposal: yield* decodeReanchorVote(voteMessage),
      localVote: yield* carriedEvidence(voteMessage),
    };
  });

/**
 * A peer that reached the threshold first relays the completed re-anchor; the
 * endpoint still re-anchoring reports it accepted, adopts its anchor, and
 * then catches up from that anchor before it recovers, since members may
 * already have certified messages under it. Protects the receiving side of a
 * relayed completion; fails when a completion the run adopts is reported
 * ignored, or the run recovers on the relay alone and never fetches what
 * follows the new anchor.
 */
const adoptsRelayedCompletionForReanchoringConversation = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery, outbound, proposal, localVote } =
      yield* proposeAtRestart(fixture);
    const peerVote = yield* signEvidence(fixture.remote, {
      ...proposal,
      signerAgentId: fixture.remote.card.agentId,
    });
    const completed: DirectPacket = {
      sharnessVersion: WIRE_VERSION,
      kind: "completed_reanchor",
      anchorHash: proposal.anchorHash,
      reanchor: proposal.reanchor,
      certificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_certificate",
        anchorHash: proposal.anchorHash,
        votes: [localVote, peerVote],
      },
    };

    const disposition = yield* directPacketIngressFrom({
      membership: fixture.membership,
      sender: fixture.remote,
      packet: completed,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const followUp = yield* takeCatchUpRequest(outbound);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, followUp),
    );
    yield* Fiber.join(recovery);

    expect(disposition).toBe(acceptedDisposition);
    expect(followUp.knownAnchorHash).toBe(proposal.anchorHash);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(proposal.anchorHash);
  });

/**
 * A peer vote naming an anchor hash its re-anchor body does not hash to is
 * reported ignored, and the peer's genuine vote still completes the
 * re-anchor. Protects a vote's binding to the anchor it names; fails when
 * that check is dropped, or a vote the run declines is reported accepted.
 */
const ignoresReanchorVoteWithMismatchedAnchorHash = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery, proposal } = yield* proposeAtRestart(fixture);
    const otherAnchorHash = yield* hashAnchor({
      ...proposal.reanchor,
      routerInstanceId: oldRouterInstanceId,
    });

    const mismatched = yield* peerReanchorVoteIngress(fixture, {
      ...proposal,
      anchorHash: otherAnchorHash,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const genuine = yield* peerReanchorVoteIngress(fixture, proposal).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Fiber.join(recovery);

    expect(mismatched).toBe(ignoredDisposition);
    expect(genuine).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(proposal.anchorHash);
  });

/**
 * A peer's vote that arrives before the endpoint's own catch-up completes is
 * held and replayed once the position is ready, and the replay completes the
 * re-anchor without the peer voting again. Fails when a held vote is not
 * replayed, or when the endpoint still proposes its own re-anchor after the
 * replay completed it: that candidate no longer matches the durable position,
 * so the run reports a persistence failure.
 */
const replaysPeerVoteHeldBeforeCatchUpCompletes = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const { recovery, outbound, request } =
      yield* startRestartRecovery(fixture);

    const early = yield* peerReanchorVote(fixture).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* catchUpIncompleteIngress(fixture, request).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Fiber.join(recovery);
    const proposal = yield* takeReanchorVote(outbound);

    expect(early).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(proposal.anchorHash);
  });

/**
 * A peer's re-anchor vote whose outer envelope is addressed beyond the
 * conversation's members fails the envelope's membership check, so it is
 * reported ignored and the run still takes the peer's genuine vote. Fails
 * when that check surfaces as a persistence failure, which ends the Router
 * worker's poll loop.
 */
const ignoresReanchorVoteAddressedBeyondItsMembers = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery, proposal } = yield* proposeAtRestart(fixture);
    const outsiderAuthority = yield* makeTestAuthority();
    const outsider = yield* issueTestCard({
      byte: 3,
      name: "recovery-outsider",
      authority: outsiderAuthority,
      registryKeys: fixture.registryKeys,
    });

    const misaddressed = yield* peerReanchorVoteIngressFrom({
      membership: {
        ...fixture.membership,
        members: [...fixture.membership.members, outsider],
      },
      responder: fixture.remote,
      proposal,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const genuine = yield* peerReanchorVoteIngress(fixture, proposal).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Fiber.join(recovery);

    expect(misaddressed).toBe(ignoredDisposition);
    expect(genuine).toBe(acceptedDisposition);
  });

/**
 * A member's envelope carrying re-anchor vote evidence signed by an agent
 * outside the conversation passes the envelope check and fails the evidence
 * membership check, so it is reported ignored and the run still takes the
 * peer's genuine vote. Fails when the evidence check surfaces as a
 * persistence failure, which ends the Router worker's poll loop.
 */
const ignoresReanchorVoteWhoseEvidenceSignerIsNotAMember = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery, proposal } = yield* proposeAtRestart(fixture);
    const outsiderAuthority = yield* makeTestAuthority();
    const outsider = yield* issueTestCard({
      byte: 3,
      name: "recovery-outsider",
      authority: outsiderAuthority,
      registryKeys: fixture.registryKeys,
    });

    const forged = yield* peerReanchorVoteIngressFrom({
      membership: fixture.membership,
      responder: fixture.remote,
      proposal,
      routerInstanceId: newRouterInstanceId,
      evidenceSigner: { card: outsider, authority: outsiderAuthority },
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const genuine = yield* peerReanchorVoteIngress(fixture, proposal).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* Fiber.join(recovery);

    expect(forged).toBe(ignoredDisposition);
    expect(genuine).toBe(acceptedDisposition);
  });

const rebroadcastsPersistedLocalVote = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    yield* fixture.engine.abandonVolatileFolds("router_restarted");
    yield* fixture.engine.recoverCertifiedHistory({
      reason: "router_restarted",
      anchor: { routerInstanceId: newRouterInstanceId, pollCursor },
    });
    yield* fixture.engine.drainOutbound;
    const firstRequest = yield* takeCatchUpRequest(fixture.recoveryOutbound);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, firstRequest),
    );
    expect(
      (yield* fixture.store.run(testStore.recover())).evidence.filter(
        (evidence) => evidence.kind === "reanchor",
      ),
    ).toHaveLength(1);
    expect(yield* Queue.size(fixture.recoveryOutbound)).toBe(0);

    const {
      recovery: secondRecovery,
      outbound: secondOutbound,
      request: secondRequest,
    } = yield* startRestartRecovery(fixture);
    yield* catchUpIncompleteIngress(fixture, secondRequest).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const replayedVote = yield* takeReanchorVote(secondOutbound);
    expect(replayedVote.signerAgentId).toBe(fixture.local.card.agentId);
    yield* Fiber.interrupt(secondRecovery);
  });

const rebroadcastsPersistedCompletedReanchor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const { proposal } = yield* reanchorUntilCompletionSend(fixture);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(proposal.anchorHash);

    const {
      recovery: secondRecovery,
      outbound: secondOutbound,
      request: secondRequest,
    } = yield* startRestartRecovery(fixture);
    yield* catchUpIncompleteIngress(fixture, secondRequest).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const replayed = yield* Queue.take(secondOutbound).pipe(
      Effect.flatMap((message) => openForwarded(message)),
    );
    expect(replayed).toMatchObject({
      kind: "direct",
      packet: {
        kind: "completed_reanchor",
        anchorHash: proposal.anchorHash,
      },
    });
    yield* Fiber.join(secondRecovery);
  });

const recoversN4PartiallyDisseminatedSuccessor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: fixture.remote,
      packet: history.certifiedHead,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: n4.fourth,
      packet: history.stagedSuccessor,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const before = yield* fixture.store.run(testStore.recover());
    expect(
      before.positions.find(
        ({ conversationId }) =>
          conversationId === n4.membership.descriptor.conversationId,
      )?.headRecordHash,
    ).toBe(history.certifiedHead.actionCertifiedRecord.recordHash);
    expect(
      before.stagedRecords.some(
        ({ recordHash }) => recordHash === history.stagedSuccessor.recordHash,
      ),
    ).toBe(true);
    expect(
      before.certifiedRecords.some(
        ({ recordHash }) => recordHash === history.stagedSuccessor.recordHash,
      ),
    ).toBe(false);
    expect(
      before.evidence.filter(
        ({ kind, subjectId }) =>
          kind === "durability" &&
          subjectId === history.stagedSuccessor.recordHash,
      ),
    ).toHaveLength(1);

    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(
        { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
        "router_restarted",
        newRouterInstanceId,
      );
    const firstRequest = yield* takeCatchUpRequest(recoveryOutbound);
    const secondRequest = yield* takeCatchUpRequest(recoveryOutbound);
    const directRequest = [firstRequest, secondRequest].find(
      (request) =>
        request.conversationId === fixture.membership.descriptor.conversationId,
    );
    const n4Request = [firstRequest, secondRequest].find(
      (request) =>
        request.conversationId === n4.membership.descriptor.conversationId,
    );
    if (directRequest === undefined || n4Request === undefined) {
      return yield* Effect.dieMessage(
        "recovery did not request both conversation positions",
      );
    }
    yield* catchUpIncompleteIngressFrom({
      membership: fixture.membership,
      responder: fixture.remote,
      request: directRequest,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* catchUpIncompleteIngressFrom({
      membership: n4.membership,
      responder: fixture.remote,
      request: n4Request,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* catchUpIncompleteIngressFrom({
      membership: n4.membership,
      responder: n4.third,
      request: n4Request,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const beforeCompleteSource = yield* fixture.store.run(testStore.recover());
    expect(
      beforeCompleteSource.stagedReanchors.filter(
        ({ conversationId }) =>
          conversationId === n4.membership.descriptor.conversationId,
      ),
    ).toHaveLength(0);
    expect(
      beforeCompleteSource.evidence.filter(
        ({ conversationId, kind }) =>
          conversationId === n4.membership.descriptor.conversationId &&
          kind === "reanchor",
      ),
    ).toHaveLength(0);
    expect(Option.isNone(yield* Queue.poll(recoveryOutbound))).toBe(true);

    yield* catchUpPageIngressFrom({
      membership: n4.membership,
      responder: n4.fourth,
      request: n4Request,
      item: history.certifiedSuccessor,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const advancedRequest = yield* takeCatchUpRequest(recoveryOutbound);
    expect(advancedRequest).toMatchObject({
      conversationId: n4.membership.descriptor.conversationId,
      knownRecordHash: history.stagedSuccessor.recordHash,
      knownAnchorHash: history.stagedSuccessor.recordCore.anchorHash,
    });
    const afterCompleteSource = yield* fixture.store.run(testStore.recover());
    expect(
      afterCompleteSource.positions.find(
        ({ conversationId }) =>
          conversationId === n4.membership.descriptor.conversationId,
      )?.headRecordHash,
    ).toBe(history.stagedSuccessor.recordHash);
    expect(
      afterCompleteSource.certifiedRecords.some(
        ({ recordHash }) => recordHash === history.stagedSuccessor.recordHash,
      ),
    ).toBe(true);

    yield* catchUpIncompleteIngressFrom({
      membership: n4.membership,
      responder: fixture.remote,
      request: advancedRequest,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* catchUpIncompleteIngressFrom({
      membership: n4.membership,
      responder: n4.third,
      request: advancedRequest,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* catchUpIncompleteIngressFrom({
      membership: n4.membership,
      responder: n4.fourth,
      request: advancedRequest,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const proposal = yield* takeReanchorVote(recoveryOutbound);
    expect(proposal.reanchor.selectedRecordHash).toBe(
      history.stagedSuccessor.recordHash,
    );
    yield* peerReanchorVoteIngressFrom({
      membership: n4.membership,
      responder: fixture.remote,
      proposal,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* peerReanchorVoteIngressFrom({
      membership: n4.membership,
      responder: n4.third,
      proposal,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const completed = yield* Queue.take(recoveryOutbound).pipe(
      Effect.flatMap((message) => openForwarded(message)),
    );
    expect(completed).toMatchObject({
      kind: "direct",
      packet: {
        kind: "completed_reanchor",
        anchorHash: proposal.anchorHash,
      },
    });

    yield* Fiber.join(recovering);
    const recovered = yield* fixture.store.run(testStore.recover());
    expect(
      recovered.positions.find(
        ({ conversationId }) =>
          conversationId === n4.membership.descriptor.conversationId,
      ),
    ).toMatchObject({
      headRecordHash: history.stagedSuccessor.recordHash,
      currentAnchorHash: proposal.anchorHash,
    });
  });

const blocksN4ReanchorBehindStagedSuccessor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: fixture.remote,
      packet: history.certifiedHead,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: n4.fourth,
      packet: history.stagedSuccessor,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );

    const { recovery: recovering, outbound: recoveryOutbound } =
      yield* forkRecovery(
        { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
        "router_restarted",
        newRouterInstanceId,
      );
    const firstRequest = yield* takeCatchUpRequest(recoveryOutbound);
    const secondRequest = yield* takeCatchUpRequest(recoveryOutbound);
    const directRequest = [firstRequest, secondRequest].find(
      (request) =>
        request.conversationId === fixture.membership.descriptor.conversationId,
    );
    const n4Request = [firstRequest, secondRequest].find(
      (request) =>
        request.conversationId === n4.membership.descriptor.conversationId,
    );
    if (directRequest === undefined || n4Request === undefined) {
      return yield* Effect.dieMessage(
        "recovery did not request both conversation positions",
      );
    }
    yield* catchUpIncompleteIngressFrom({
      membership: fixture.membership,
      responder: fixture.remote,
      request: directRequest,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* catchUpIncompleteIngressFrom({
      membership: n4.membership,
      responder: fixture.remote,
      request: n4Request,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* catchUpIncompleteIngressFrom({
      membership: n4.membership,
      responder: n4.third,
      request: n4Request,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* catchUpIncompleteIngressFrom({
      membership: n4.membership,
      responder: n4.fourth,
      request: n4Request,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );

    const blocked = yield* fixture.store.run(testStore.recover());
    expect(
      blocked.stagedReanchors.filter(
        ({ conversationId }) =>
          conversationId === n4.membership.descriptor.conversationId,
      ),
    ).toHaveLength(0);
    expect(
      blocked.evidence.filter(
        ({ conversationId, kind }) =>
          conversationId === n4.membership.descriptor.conversationId &&
          kind === "reanchor",
      ),
    ).toHaveLength(0);
    expect(Option.isNone(yield* Queue.poll(recoveryOutbound))).toBe(true);
    expect(
      blocked.positions.find(
        ({ conversationId }) =>
          conversationId === n4.membership.descriptor.conversationId,
      )?.currentAnchorHash,
    ).toBe(history.certifiedHead.actionCertifiedRecord.recordCore.anchorHash);
    yield* Fiber.interrupt(recovering);
  });

/**
 * How a restart recovery's conversation re-anchors at the endpoint's head,
 * given the run's queue and its first catch-up request.
 */
type ReanchorRoute = (
  fixture: RecoveryFixture,
  run: {
    readonly outbound: Queue.Queue<SignedMessage>;
    readonly request: CatchUpRequest;
  },
) => Effect.Effect<CompletedReanchor["anchorHash"]>;

/**
 * The member reports nothing past the endpoint's head, the endpoint votes to
 * re-anchor there, and the member's vote completes the re-anchor.
 * @param fixture Endpoint under restart recovery.
 * @param run The run's queue and its first catch-up request.
 * @returns The hash of the anchor the conversation re-anchored at.
 */
const reanchorByVote: ReanchorRoute = (fixture, run) =>
  Effect.gen(function* () {
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, run.request),
    );
    const vote = yield* takeReanchorVote(run.outbound);
    yield* deliverRecovery(
      fixture.engine,
      peerReanchorVoteIngress(fixture, vote),
    );
    return vote.anchorHash;
  }).pipe(Effect.orDie);

/**
 * Both members completed the re-anchor at the endpoint's head while it was
 * down, and the member's catch-up page carries the completion.
 * Catch-up adoption must retire the superseded proposal's fold just as vote
 * completion does, so no signature survives for an action without a lock.
 */
const reanchorByCatchUp: ReanchorRoute = (fixture, run) =>
  Effect.gen(function* () {
    const { recordCore, recordHash } =
      fixture.certifiedRecord.actionCertifiedRecord;
    const completed = yield* completedReanchorBy(
      {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_body",
        conversationId: recordCore.action.conversationId,
        membershipHash: fixture.membership.hash,
        previousAnchorHash: recordCore.anchorHash,
        selectedRecordHash: recordHash,
        routerInstanceId: newRouterInstanceId,
      },
      [fixture.local, fixture.remote],
    );
    yield* deliverRecovery(
      fixture.engine,
      catchUpPageIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: run.request,
        item: completed,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const next = yield* takeCatchUpRequest(run.outbound);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, next),
    );
    return completed.anchorHash;
  }).pipe(Effect.orDie);

/**
 * The Router orders a message's proposal at head R, so the endpoint locks R for
 * that proposal, and the Router restarts before it certifies. The
 * conversation re-anchors at R, and the message's proposal at the new anchor,
 * once the new Router orders it, certifies with the peer's signature and
 * vote, and the endpoint starts again over the same store. Fails when the
 * lock taken under the old anchor still holds R, so every proposal at R
 * after the re-anchor conflicts with it and the conversation never
 * certifies another record, when the old proposal's signatures outlive its
 * lock and startup refuses the store, or when the old proposal's fold
 * resends its signature after the re-anchor. With `stale` set to `staged`,
 * the peer's signature arrives before the restart, so the endpoint assembles
 * the old proposal's action certificate and stages its record with a
 * dissemination obligation, which the re-anchor retires as well. A direct
 * conversation's completion always carries this endpoint's vote, which it
 * never casts behind a staged successor, so the staged trace stands in for an
 * n ≥ 4 conversation completed without the record's author: it checks the
 * retirement, not how the completion was reached.
 * @param reanchorAt How the conversation re-anchors at R.
 * @param stale Whether the old proposal is only locked or also staged.
 * @returns The trace, run to completion.
 */
const certifiesAtALockedHeadAfterReanchoring = (
  reanchorAt: ReanchorRoute,
  stale: "locked" | "staged" = "locked",
) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixtureWithRouter(
      fixtureRouterAt(oldRouterInstanceId),
    );
    yield* retainCertifiedRecord(fixture);
    yield* fixture.engine.drainOutbound.pipe(Effect.orDie);
    yield* Queue.takeAll(fixture.normalOutbound);
    const head = fixture.certifiedRecord.actionCertifiedRecord.recordHash;
    const sending = yield* Effect.fork(
      fixture.engine.send(
        yield* authoredMessage(fixture, [
          { type: "text", text: "certify after re-anchoring" },
        ]),
      ),
    );
    const staleProposal = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );
    const staleOrdered = yield* fixture.engine.acceptRouterIngress(
      yield* directPacketIngressFrom({
        membership: fixture.membership,
        sender: fixture.local,
        packet: staleProposal.proposal,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const staleActionHash = yield* hashAction(staleProposal.proposal.action);
    if (stale === "staged") {
      yield* fixture.engine.acceptRouterIngress(
        yield* peerEvidenceIngressFrom({
          membership: fixture.membership,
          responder: fixture.remote,
          statement: {
            sharnessVersion: WIRE_VERSION,
            kind: "action_signature",
            signerAgentId: fixture.remote.card.agentId,
            actionHash: staleActionHash,
          },
          routerInstanceId: oldRouterInstanceId,
        }),
      );
    }
    const beforeRestart = yield* fixture.store.run(testStore.recover());
    const locksBeforeRestart = beforeRestart.proposalLocks;

    const { recovery, outbound, request } =
      yield* startRestartRecovery(fixture);
    const anchorHash = yield* reanchorAt(fixture, { outbound, request });
    yield* Fiber.join(recovery);
    yield* fixture.engine.drainOutbound;
    const resumed = yield* takeBodiesThroughActionProposal(
      fixture.normalOutbound,
    );
    const resumedEvidence = yield* Effect.forEach(
      resumed.flatMap((body) =>
        body.kind === "evidence" ? [body.message] : [],
      ),
      (message) => decodeCanonical(EvidenceStatement, message.body),
      { concurrency: 1 },
    );
    const [reproposal] = resumed.flatMap((body) =>
      body.kind === "direct" && body.packet.kind === "action_proposal"
        ? [body.packet]
        : [],
    );
    if (reproposal === undefined) {
      return yield* Effect.dieMessage("recovery reproposed nothing");
    }
    const reproposedOrdered = yield* fixture.engine.acceptRouterIngress(
      yield* directPacketIngressFrom({
        membership: fixture.membership,
        sender: fixture.local,
        packet: reproposal,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const actionHash = yield* hashAction(reproposal.action);
    yield* fixture.engine.acceptRouterIngress(
      yield* peerEvidenceIngress(fixture, {
        sharnessVersion: WIRE_VERSION,
        kind: "action_signature",
        signerAgentId: fixture.remote.card.agentId,
        actionHash,
      }),
    );
    const staged = (yield* fixture.store.run(
      testStore.recover(),
    )).stagedRecords.find((record) => record.actionHash === actionHash);
    if (staged === undefined) {
      return yield* Effect.dieMessage(
        `the proposal at the new anchor staged no record; the Router-ordered proposal was ${reproposedOrdered}`,
      );
    }
    const recordHash = yield* Schema.decodeUnknown(RecordHash)(
      staged.recordHash,
    );
    yield* fixture.engine.acceptRouterIngress(
      yield* peerEvidenceIngress(fixture, {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_vote",
        signerAgentId: fixture.remote.card.agentId,
        conversationId: fixture.membership.descriptor.conversationId,
        membershipHash: fixture.membership.hash,
        recordHash,
      }),
    );
    const sent = yield* Fiber.join(sending);
    const restarted = yield* Effect.exit(
      makeEndpointEngine(fixture.input).pipe(
        Effect.provideService(SqlClient.SqlClient, fixture.store.sql),
      ),
    );

    const afterRestart = yield* fixture.store.run(testStore.recover());

    expect(staleOrdered).toBe(acceptedDisposition);
    expect(resumedEvidence).not.toContainEqual(
      expect.objectContaining({ actionHash: staleActionHash }),
    );
    expect(locksBeforeRestart).toMatchObject([
      {},
      { previousRecordHash: head },
    ]);
    expect(
      beforeRestart.stagedRecords.some(
        (record) => record.actionHash === staleActionHash,
      ),
    ).toBe(stale === "staged");
    expect(afterRestart.stagedRecords).not.toContainEqual(
      expect.objectContaining({ actionHash: staleActionHash }),
    );
    expect(reproposal.action).toMatchObject({
      previousRecordHash: head,
      anchorHash,
    });
    expect(reproposedOrdered).toBe(acceptedDisposition);
    expect(sent.recordHash).toBe(recordHash);
    expect(restarted).toMatchObject({ _tag: "Success" });
  });

/**
 * How a quorum's completed re-anchor reaches this endpoint: in a member's
 * catch-up page answering its pending request, or relayed on its own after
 * every member has answered its request as incomplete.
 */
type CompletionArrival = "catch-up" | "relay";

/**
 * Members re-anchored the N4 conversation at its certified head while this
 * endpoint held a staged successor of that head under the old anchor, and
 * the completion reaches it by `arrival`. The endpoint adopts it, retires the
 * staged successor that can no longer certify, takes the members' next
 * record at that head under the new anchor, recovers, ignores a late
 * durability vote for the retired successor, and starts again over the same
 * store. Fails when the staged successor keeps its lock, so the record under
 * the new anchor conflicts with it and the conversation never recovers; when
 * a relayed completion is held behind the staged successor; when the retired
 * successor's fold still takes evidence; or when retired rows leave startup a
 * record no lock selects.
 */
const retiresAStagedSuccessorWhenAReanchorSelectsItsHead = (
  arrival: CompletionArrival,
) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    yield* Effect.forEach(
      [
        { sender: fixture.remote, packet: history.certifiedHead },
        { sender: n4.fourth, packet: history.stagedSuccessor },
      ],
      ({ sender, packet }) =>
        directPacketIngressFrom({
          membership: n4.membership,
          sender,
          packet,
          routerInstanceId: oldRouterInstanceId,
        }).pipe(
          Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
        ),
      { concurrency: 1, discard: true },
    );
    const { recordCore, recordHash } =
      history.certifiedHead.actionCertifiedRecord;
    const completed = yield* completedReanchorBy(
      {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_body",
        conversationId: n4.membership.descriptor.conversationId,
        membershipHash: n4.membership.hash,
        previousAnchorHash: recordCore.anchorHash,
        selectedRecordHash: recordHash,
        routerInstanceId: newRouterInstanceId,
      },
      [fixture.remote, n4.third, n4.fourth],
    );
    const extension = yield* certifiedN4MessageAt(fixture, n4, {
      routerAnchor: completed,
      previousRecordHash: recordHash,
    });
    const { recovery, outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      newRouterInstanceId,
    );
    const requests = [
      yield* takeCatchUpRequest(outbound),
      yield* takeCatchUpRequest(outbound),
    ];
    const directRequest = requests.find(
      (request) =>
        request.conversationId === fixture.membership.descriptor.conversationId,
    );
    const n4Request = requests.find(
      (request) =>
        request.conversationId === n4.membership.descriptor.conversationId,
    );
    if (directRequest === undefined || n4Request === undefined) {
      return yield* Effect.dieMessage(
        "recovery did not request both conversation positions",
      );
    }
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngress(fixture, directRequest),
    );
    const incompleteFromEveryMember = (request: CatchUpRequest) =>
      Effect.forEach(
        [fixture.remote, n4.third, n4.fourth],
        (responder) =>
          deliverRecovery(
            n4.engine,
            catchUpIncompleteIngressFrom({
              membership: n4.membership,
              responder,
              request,
              routerInstanceId: newRouterInstanceId,
            }),
          ),
        { concurrency: 1, discard: true },
      );
    const adopted =
      arrival === "catch-up"
        ? yield* deliverRecovery(
            n4.engine,
            catchUpPageIngressFrom({
              membership: n4.membership,
              responder: n4.fourth,
              request: n4Request,
              item: completed,
              routerInstanceId: newRouterInstanceId,
            }),
          )
        : yield* incompleteFromEveryMember(n4Request).pipe(
            Effect.zipRight(
              deliverRecovery(
                n4.engine,
                directPacketIngressFrom({
                  membership: n4.membership,
                  sender: n4.third,
                  packet: completed,
                  routerInstanceId: newRouterInstanceId,
                }),
              ),
            ),
          );
    yield* incompleteFromEveryMember(yield* takeCatchUpRequest(outbound));
    yield* Fiber.join(recovery);
    const extended = yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: n4.third,
      packet: extension,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const lateVote = yield* peerEvidenceIngressFrom({
      membership: n4.membership,
      responder: n4.third,
      statement: {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_vote",
        signerAgentId: n4.third.card.agentId,
        conversationId: n4.membership.descriptor.conversationId,
        membershipHash: n4.membership.hash,
        recordHash: history.stagedSuccessor.recordHash,
      },
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) =>
        Effect.exit(n4.engine.acceptRouterIngress(ingress)),
      ),
    );
    const recovered = yield* fixture.store.run(testStore.recover());
    const restarted = yield* Effect.exit(
      makeEndpointEngine(fixture.input).pipe(
        Effect.provideService(SqlClient.SqlClient, fixture.store.sql),
      ),
    );

    expect(adopted).toBe(acceptedDisposition);
    expect(extended).toBe(acceptedDisposition);
    expect(lateVote).toStrictEqual(Exit.succeed(ignoredDisposition));
    expect(
      recovered.positions.find(
        ({ conversationId }) =>
          conversationId === n4.membership.descriptor.conversationId,
      ),
    ).toMatchObject({
      headRecordHash: extension.actionCertifiedRecord.recordHash,
      currentAnchorHash: completed.anchorHash,
    });
    expect(recovered.stagedRecords).not.toContainEqual(
      expect.objectContaining({
        recordHash: history.stagedSuccessor.recordHash,
      }),
    );
    expect(recovered.proposalLocks).not.toContainEqual(
      expect.objectContaining({
        actionHash: history.stagedSuccessor.recordCore.actionHash,
      }),
    );
    expect(restarted).toMatchObject({ _tag: "Success" });
  });

// @agent-code-guard/regression-only: these traces pin restart liveness and fail-closed ancestry handling.
describe("endpoint restart recovery", () => {
  it(
    "starts its recovery run while a normal send is held",
    recoverWhileNormalSendIsHeld,
  );
  it(
    "recovers certificates when encoded and canonical AgentId orders differ",
    restartWithNonLexicalAgentOrder,
  );
  it(
    "answers a peer's catch-up request from first message with its retained record",
    answersFirstMessageCatchUpWithRetainedRecord,
  );
  it(
    "re-anchors with a certificate in canonical order when encoded and canonical AgentId orders differ",
    reanchorsWithNonLexicalAgentOrder,
  );
  itOnTestClock(
    "waits for the complete N4 successor before re-anchoring its latest head",
    recoversN4PartiallyDisseminatedSuccessor,
  );
  it(
    "does not re-anchor behind a staged N4 successor when every peer reports incomplete",
    blocksN4ReanchorBehindStagedSuccessor,
  );
  it("retires a staged N4 successor when a caught-up re-anchor selects its head", () =>
    retiresAStagedSuccessorWhenAReanchorSelectsItsHead("catch-up"));
  it("retires a staged N4 successor when a relayed re-anchor selects its head", () =>
    retiresAStagedSuccessorWhenAReanchorSelectsItsHead("relay"));
  it(
    "rebroadcasts a persisted local vote after an interrupted send",
    rebroadcastsPersistedLocalVote,
  );
  it(
    "rebroadcasts a persisted completed re-anchor after an interrupted send",
    rebroadcastsPersistedCompletedReanchor,
  );
  it(
    "re-anchors, requeues retained evidence, and resumes normal sends",
    completeRestartRecovery,
  );
  it(
    "discards an old-instance MESSAGE and reproposes the same MsgId at the new anchor",
    reproposesPendingMessageAfterRestart,
  );
  it("certifies a message at a head a proposal locked before the conversation re-anchored there", () =>
    certifiesAtALockedHeadAfterReanchoring(reanchorByVote));
  it("certifies a message at a head a proposal locked before a caught-up re-anchor there", () =>
    certifiesAtALockedHeadAfterReanchoring(reanchorByCatchUp));
  it("certifies a message at a head where it staged a record before a caught-up re-anchor there", () =>
    certifiesAtALockedHeadAfterReanchoring(reanchorByCatchUp, "staged"));
  it(
    "restarts an empty foundation at the new Router instance",
    restartEmptyConversation,
  );
  it("resumes a same-instance persisted intent without reproposing", () =>
    recoverSameRouterInstance("feed_gap"));
  it("preserves retained envelope bytes when startup finds the same Router", () =>
    recoverSameRouterInstance("router_restarted"));
  it(
    "catches up without re-anchoring when a cold start finds the anchored Router instance",
    recoverColdStartAtUnchangedInstance,
  );
  it(
    "re-anchors only the conversation anchored to another Router instance",
    recoverMixedRouterInstances,
  );
  it(
    "starts its recovery run while an outbound drain waits on the worker",
    recoverWhileDrainAwaitsWorker,
  );
  it(
    "rebuilds one discarded record dissemination without duplication",
    recoverDisseminationObligations,
  );
  it(
    "ignores a re-anchor vote during a feed_gap recovery",
    ignoresReanchorVoteDuringFeedGapRecovery,
  );
  it(
    "ignores a re-anchor vote that arrives after recovery completes",
    ignoresReanchorVoteAfterRecovery,
  );
  it(
    "ignores a relayed completed re-anchor for a conversation already anchored at the new Router",
    ignoresRelayedCompletionForAnchoredConversation,
  );
  it(
    "adopts a relayed completed re-anchor for a conversation it is re-anchoring",
    adoptsRelayedCompletionForReanchoringConversation,
  );
  it(
    "ignores a re-anchor vote whose anchor hash its body does not hash to",
    ignoresReanchorVoteWithMismatchedAnchorHash,
  );
  it(
    "ignores a re-anchor vote whose envelope is addressed beyond its members",
    ignoresReanchorVoteAddressedBeyondItsMembers,
  );
  it(
    "ignores a re-anchor vote whose evidence an agent outside the conversation signed",
    ignoresReanchorVoteWhoseEvidenceSignerIsNotAMember,
  );
  it(
    "replays a peer's re-anchor vote held from before its catch-up completed",
    replaysPeerVoteHeldBeforeCatchUpCompletes,
  );
});

// @agent-code-guard/regression-only: these traces pin outbound drain order and gate release.
describe("endpoint outbound drain", () => {
  it(
    "sends each queued envelope once, in order, across concurrent drains",
    concurrentDrainsSendOnceInOrder,
  );
  it(
    "keeps a later head when another drain removed the one it sent",
    staleDrainKeepsLaterHead,
  );
  it(
    "completes Router recovery that runs on the draining fiber",
    drainRecoversRouterRestartOnItsOwnFiber,
  );
});

/**
 * Takes the two catch-up requests a recovery of a group's engine sends first.
 * @param fixture Endpoint whose direct conversation is one of the two.
 * @param n4 The group conversation, the other one.
 * @param outbound Queue the recovery sends to.
 * @returns The request for each conversation.
 */
const takeN4Requests = (
  fixture: RecoveryFixture,
  n4: Pick<N4Foundation, "membership">,
  outbound: Queue.Queue<SignedMessage>,
) =>
  Effect.gen(function* () {
    const first = yield* takeCatchUpRequest(outbound);
    const second = yield* takeCatchUpRequest(outbound);
    const direct = [first, second].find(
      (request) =>
        request.conversationId === fixture.membership.descriptor.conversationId,
    );
    const group = [first, second].find(
      (request) =>
        request.conversationId === n4.membership.descriptor.conversationId,
    );
    if (direct === undefined || group === undefined) {
      return yield* Effect.dieMessage(
        "recovery did not request both conversation positions",
      );
    }
    return { direct, group };
  });

/**
 * N4 members answer `request` that they hold no later history. By default
 * every other member answers at the new Router instance, which makes the N4
 * position ready.
 * @param fixture Endpoint whose remote member is one of the three.
 * @param n4 Engine under recovery and the other two members.
 * @param request The N4 catch-up request being answered.
 * @param answering Which members answer, and through which Router instance.
 * @param answering.responders The members that answer, in order.
 * @param answering.routerInstanceId The Router instance the answers come
 *     through, by default the new one.
 * @returns Each answer's disposition, in the order the members answered.
 */
const answerN4Incomplete = (
  fixture: RecoveryFixture,
  n4: N4Foundation,
  request: CatchUpRequest,
  answering: {
    readonly responders?: readonly SigningIdentity[];
    readonly routerInstanceId?: typeof RouterInstanceId.Type;
  } = {},
) =>
  Effect.forEach(
    answering.responders ?? [fixture.remote, n4.third, n4.fourth],
    (responder) =>
      deliverRecovery(
        n4.engine,
        catchUpIncompleteIngressFrom({
          membership: n4.membership,
          responder,
          request,
          routerInstanceId: answering.routerInstanceId ?? newRouterInstanceId,
        }),
      ),
    { concurrency: 1 },
  );

/**
 * The hash of the initial anchor a restart gives the fixture's direct
 * conversation when it restarts it empty at the new Router instance.
 * @param fixture Endpoint whose direct conversation restarts.
 * @returns The replacement anchor's hash.
 */
const restartedDirectAnchorHash = (fixture: RecoveryFixture) => {
  const anchor: InitialAnchorBody = {
    sharnessVersion: WIRE_VERSION,
    kind: "initial_anchor_body",
    conversationId: fixture.membership.descriptor.conversationId,
    membershipHash: fixture.membership.hash,
    routerInstanceId: newRouterInstanceId,
  };
  return hashAnchor(anchor).pipe(Effect.orDie);
};

/**
 * The durable anchor of the fixture's direct conversation.
 * @param fixture Endpoint whose store is read.
 * @returns The direct conversation's current anchor hash.
 */
const directAnchorHash = (fixture: RecoveryFixture) =>
  fixture.store
    .run(testStore.recover())
    .pipe(
      Effect.map(
        (recovered) =>
          recovered.positions.find(
            ({ conversationId }) =>
              conversationId === fixture.membership.descriptor.conversationId,
          )?.currentAnchorHash,
      ),
    );

/**
 * Re-anchors the N4 conversation at `history`'s certified head after a
 * restart: the endpoint stages its candidate and sends its vote, while the
 * direct conversation restarts empty. `history`'s certified successor then
 * arrives as recovery traffic.
 * @param fixture Endpoint owning the direct conversation.
 * @param n4 Engine holding both conversations.
 * @param history N4 history whose certified head the candidate selects.
 * @returns The running recovery, its queue, the staged vote, and the
 *     successor's disposition.
 */
const deliverN4SuccessorPastStagedCandidate = (
  fixture: RecoveryFixture,
  n4: N4Foundation,
  history: N4PartialHistory,
) =>
  Effect.gen(function* () {
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: fixture.remote,
      packet: history.certifiedHead,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const { recovery, outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      newRouterInstanceId,
    );
    const requests = yield* takeN4Requests(fixture, n4, outbound);
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: requests.direct,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    yield* answerN4Incomplete(fixture, n4, requests.group);
    const staged = yield* takeReanchorVote(outbound);
    const successor = yield* deliverRecovery(
      n4.engine,
      directPacketIngressFrom({
        membership: n4.membership,
        sender: n4.fourth,
        packet: history.certifiedSuccessor,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    return { recovery, outbound, staged, successor };
  });

/**
 * A three-of-four durability certificate advances the head after this
 * endpoint stages a departure. A certificate may carry an earlier local
 * signature; adopting that evidence must not cast a new local vote.
 * @param priorLocalVote Whether the certificate includes an earlier local vote.
 * @returns Completion after the adopted head and outbound evidence are checked.
 */
const adoptsCertifiedSuccessorWithoutVotingAfterDeparture = (
  priorLocalVote: boolean,
) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordHash } = history.stagedSuccessor;
    const earlierVote = yield* signEvidence(fixture.local, {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_vote",
      signerAgentId: fixture.local.card.agentId,
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      recordHash,
    });
    const certificate = history.certifiedSuccessor.durabilityCertificate;
    const completedHistory: N4PartialHistory = {
      ...history,
      certifiedSuccessor: {
        ...history.certifiedSuccessor,
        durabilityCertificate: {
          ...certificate,
          votes: priorLocalVote
            ? [earlierVote, ...certificate.votes.slice(0, 2)]
            : certificate.votes,
        },
      },
    };
    expect(n4.membership.descriptor.members).toHaveLength(4);
    expect(
      completedHistory.certifiedSuccessor.durabilityCertificate.votes,
    ).toHaveLength(3);

    const delivered = yield* deliverN4SuccessorPastStagedCandidate(
      fixture,
      n4,
      completedHistory,
    );
    yield* n4.engine.drainOutbound;
    const sent = yield* Effect.forEach(
      yield* Queue.takeAll(fixture.normalOutbound),
      (message) => carriedBy(message),
      { concurrency: 1 },
    );
    const recovered = yield* fixture.store.run(testStore.recover());
    const localVotes = recovered.evidence.filter(
      (evidence) =>
        evidence.kind === "durability" &&
        evidence.subjectId === recordHash &&
        evidence.evidenceKey === fixture.local.card.agentId,
    );

    expect(delivered.successor).toBe(acceptedDisposition);
    expect(
      recovered.positions.find(
        (position) =>
          position.conversationId === n4.membership.descriptor.conversationId,
      )?.headRecordHash,
    ).toBe(recordHash);
    expect(localVotes).toHaveLength(priorLocalVote ? 1 : 0);
    expect(
      sent.filter(
        (packet) =>
          packet.kind === "durability_vote" && packet.recordHash === recordHash,
      ),
    ).toEqual([]);
    yield* Fiber.interrupt(delivered.recovery);
  });

describe("certificate adoption after departure", () => {
  it("adopts a three-of-four external certificate without casting a local vote", () =>
    adoptsCertifiedSuccessorWithoutVotingAfterDeparture(false));
  it("adopts a three-of-four certificate carrying an earlier local vote without signing again", () =>
    adoptsCertifiedSuccessorWithoutVotingAfterDeparture(true));
});

/**
 * Delivers a certified successor past the endpoint's candidate at the N4
 * head, as `deliverN4SuccessorPastStagedCandidate` does. The successor moves
 * the head past the staged candidate, and the run asks for the history after
 * the new head.
 * @param fixture Endpoint owning the direct conversation.
 * @param n4 Engine holding both conversations.
 * @returns The running recovery, its queue, the history, the staged vote,
 *     the successor's disposition, and the request at the new head.
 */
const advanceN4HeadPastStagedCandidate = (
  fixture: RecoveryFixture,
  n4: N4Foundation,
) =>
  Effect.gen(function* () {
    const history = yield* buildN4PartialHistory(fixture, n4);
    const delivered = yield* deliverN4SuccessorPastStagedCandidate(
      fixture,
      n4,
      history,
    );
    const advancedRequest = yield* takeCatchUpRequest(delivered.outbound);
    return { ...delivered, history, advancedRequest };
  });

/**
 * A peer's vote whose previous anchor is neither this endpoint's anchor nor
 * one of its ancestors is reported ignored, and the peer's genuine vote still
 * completes the re-anchor. Fails when that vote ends recovery with a
 * persistence failure, which ends the Router worker's poll loop.
 */
const ignoresReanchorVoteFromUnknownPreviousAnchor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery, proposal } = yield* proposeAtRestart(fixture);
    const unknownAnchorHash = yield* hashAnchor({
      ...proposal.reanchor,
      routerInstanceId: oldRouterInstanceId,
    });

    const unresolvable = yield* deliverRecovery(
      fixture.engine,
      peerVoteFor(fixture, {
        ...proposal.reanchor,
        previousAnchorHash: unknownAnchorHash,
      }),
    );
    const genuine = yield* deliverRecovery(
      fixture.engine,
      peerReanchorVoteIngress(fixture, proposal),
    );
    yield* Fiber.join(recovery);

    expect(unresolvable).toBe(ignoredDisposition);
    expect(genuine).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(proposal.anchorHash);
  });

/**
 * Once the endpoint has staged its candidate, a peer's vote selecting a
 * record this endpoint does not hold is reported ignored, and the peer's
 * genuine vote still completes the re-anchor. Fails when that vote ends
 * recovery with a persistence failure.
 */
const ignoresReanchorVoteForUnknownHeadOnceStaged = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery, proposal } = yield* proposeAtRestart(fixture);
    const unknown = yield* buildCertifiedFirstMessage(
      fixture.local,
      fixture.remote,
      fixture.membership,
      oldRouterInstanceId,
    );

    const unresolvable = yield* deliverRecovery(
      fixture.engine,
      peerVoteFor(fixture, {
        ...proposal.reanchor,
        selectedRecordHash: unknown.actionCertifiedRecord.recordHash,
      }),
    );
    const genuine = yield* deliverRecovery(
      fixture.engine,
      peerReanchorVoteIngress(fixture, proposal),
    );
    yield* Fiber.join(recovery);

    expect(unresolvable).toBe(ignoredDisposition);
    expect(genuine).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(proposal.anchorHash);
  });

/**
 * A peer's vote held before catch-up completes selects a record that catch-up
 * never supplies. When the position is ready that vote does not count: the
 * endpoint proposes at its own head and the peer's genuine vote completes the
 * re-anchor. Fails when the unresolved held vote ends recovery with a
 * persistence failure at position-ready.
 */
const reanchorsPastHeldVoteForUnsuppliedHead = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const { recordCore, recordHash } =
      fixture.certifiedRecord.actionCertifiedRecord;
    const unknown = yield* buildCertifiedFirstMessage(
      fixture.local,
      fixture.remote,
      fixture.membership,
      oldRouterInstanceId,
    );
    const { recovery, outbound, request } =
      yield* startRestartRecovery(fixture);

    const held = yield* deliverRecovery(
      fixture.engine,
      peerVoteFor(fixture, {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_body",
        conversationId: recordCore.action.conversationId,
        membershipHash: fixture.membership.hash,
        previousAnchorHash: recordCore.anchorHash,
        selectedRecordHash: unknown.actionCertifiedRecord.recordHash,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const ready = yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, request),
    );
    const proposal = yield* takeReanchorVote(outbound);
    const genuine = yield* deliverRecovery(
      fixture.engine,
      peerReanchorVoteIngress(fixture, proposal),
    );
    yield* Fiber.join(recovery);

    expect(held).toBe(acceptedDisposition);
    expect(ready).toBe(acceptedDisposition);
    expect(proposal.reanchor.selectedRecordHash).toBe(recordHash);
    expect(genuine).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(proposal.anchorHash);
  });

/**
 * After the run restarts the direct conversation's empty foundation, that
 * conversation is anchored to the run's Router instance, so a peer's vote for
 * it is reported ignored and the run still recovers the N4 conversation.
 * Fails when the vote ends recovery with a persistence failure.
 */
const ignoresReanchorVoteForRestartedEmptyConversation = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const { recovery, outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      newRouterInstanceId,
    );
    const requests = yield* takeN4Requests(fixture, n4, outbound);
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: requests.direct,
        routerInstanceId: newRouterInstanceId,
      }),
    );

    const vote = yield* deliverRecovery(n4.engine, peerReanchorVote(fixture));
    yield* answerN4Incomplete(fixture, n4, requests.group);
    yield* Fiber.join(recovery);

    expect(vote).toBe(ignoredDisposition);
    expect(yield* directAnchorHash(fixture)).toBe(
      yield* restartedDirectAnchorHash(fixture),
    );
  });

/**
 * After the run restarts the direct conversation's empty foundation, that
 * conversation is anchored to the run's Router instance, so a relayed
 * completed re-anchor for it is reported ignored and the run still recovers
 * the N4 conversation. Fails when the completion ends recovery with a
 * persistence failure, or is reported accepted although the run took none of
 * its votes.
 */
const ignoresRelayedCompletionForRestartedEmptyConversation = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const { recordCore, recordHash } =
      fixture.certifiedRecord.actionCertifiedRecord;
    const reanchor: ReanchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_body",
      conversationId: recordCore.action.conversationId,
      membershipHash: fixture.membership.hash,
      previousAnchorHash: recordCore.anchorHash,
      selectedRecordHash: recordHash,
      routerInstanceId: newRouterInstanceId,
    };
    const completed = yield* completedReanchorBy(reanchor, [
      fixture.local,
      fixture.remote,
    ]);
    const { recovery, outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      newRouterInstanceId,
    );
    const requests = yield* takeN4Requests(fixture, n4, outbound);
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: requests.direct,
        routerInstanceId: newRouterInstanceId,
      }),
    );

    const relayed = yield* deliverRecovery(
      n4.engine,
      directPacketIngressFrom({
        membership: fixture.membership,
        sender: fixture.remote,
        packet: completed,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    yield* answerN4Incomplete(fixture, n4, requests.group);
    yield* Fiber.join(recovery);

    expect(relayed).toBe(ignoredDisposition);
    expect(yield* directAnchorHash(fixture)).toBe(
      yield* restartedDirectAnchorHash(fixture),
    );
  });

/**
 * A second catch-up page for an already answered request, carrying a
 * different certified successor, is reported ignored; the first successor
 * stays the head and catch-up completes from it. Fails when the conflicting
 * page ends recovery with a persistence failure.
 */
const ignoresCatchUpPageWithConflictingSuccessor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const conflicting = yield* buildCertifiedFirstMessage(
      fixture.local,
      fixture.remote,
      fixture.membership,
      oldRouterInstanceId,
    );
    const { recovery, outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
    );
    const first = yield* takeCatchUpRequest(outbound);
    const applied = yield* deliverRecovery(
      fixture.engine,
      catchUpPageIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: first,
        item: fixture.certifiedRecord,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const next = yield* takeCatchUpRequest(outbound);

    const conflict = yield* deliverRecovery(
      fixture.engine,
      catchUpPageIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: first,
        item: conflicting,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const complete = yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: next,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    yield* Fiber.join(recovery);

    expect(applied).toBe(acceptedDisposition);
    expect(conflict).toBe(ignoredDisposition);
    expect(complete).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.headRecordHash,
    ).toBe(fixture.certifiedRecord.actionCertifiedRecord.recordHash);
  });

/**
 * A catch-up page whose verified FIRST_MESSAGE record binds another Router
 * instance's anchor does not extend the conversation, so it is reported
 * ignored and catch-up completes at the empty position. Fails when the page
 * ends recovery with a persistence failure.
 */
const ignoresCatchUpPageWhoseRecordDoesNotExtendTheConversation = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const elsewhere = yield* buildCertifiedFirstMessage(
      fixture.local,
      fixture.remote,
      fixture.membership,
      newRouterInstanceId,
    );
    const { recovery, outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
    );
    const request = yield* takeCatchUpRequest(outbound);

    const page = yield* deliverRecovery(
      fixture.engine,
      catchUpPageIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request,
        item: elsewhere,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const complete = yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    yield* Fiber.join(recovery);

    expect(page).toBe(ignoredDisposition);
    expect(complete).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.headRecordHash,
    ).toBeUndefined();
  });

/**
 * The Router worker polls recovery traffic while the recovery run is still
 * starting. A certified record delivered before the run exists is applied
 * and reported accepted, and the run then catches up from it. Fails when the
 * missing run ends recovery with a persistence failure.
 */
const acceptsCertifiedRecordBeforeTheRunStarts = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* fixture.engine.abandonVolatileFolds("feed_gap");

    const early = yield* deliverRecovery(
      fixture.engine,
      certifiedRecordIngress(fixture),
    );
    yield* runSameInstanceRecovery(fixture, "feed_gap", Effect.void);

    expect(early).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.headRecordHash,
    ).toBe(fixture.certifiedRecord.actionCertifiedRecord.recordHash);
  });

/**
 * The endpoint staged its candidate at the N4 head, and a certified
 * successor then moved the head. A peer's vote at the new head would need a
 * second candidate for the same anchor and Router instance, which the
 * endpoint never stages, so the vote is reported ignored; the direct
 * conversation is still recovered and the run keeps running. Fails when the
 * vote ends recovery with a persistence failure.
 */
const ignoresReanchorVotePastTheStagedCandidate = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const { recovery, history, staged, successor } =
      yield* advanceN4HeadPastStagedCandidate(fixture, n4);
    const atNewHead: ReanchorBody = {
      ...staged.reanchor,
      selectedRecordHash:
        history.certifiedSuccessor.actionCertifiedRecord.recordHash,
    };

    const vote = yield* deliverRecovery(
      n4.engine,
      peerReanchorVoteIngressFrom({
        membership: n4.membership,
        responder: fixture.remote,
        proposal: {
          ...staged,
          anchorHash: yield* hashAnchor(atNewHead),
          reanchor: atNewHead,
        },
        routerInstanceId: newRouterInstanceId,
      }),
    );

    expect(successor).toBe(acceptedDisposition);
    expect(vote).toBe(ignoredDisposition);
    expect(yield* directAnchorHash(fixture)).toBe(
      yield* restartedDirectAnchorHash(fixture),
    );
    expect(yield* Fiber.await(recovery)).toStrictEqual(Exit.void);
    yield* Fiber.interrupt(recovery);
  });

/**
 * The endpoint staged its candidate at the N4 head, and a certified
 * successor then moved the head. When every peer reports nothing after the
 * new head, the endpoint does not stage or sign a second candidate for the
 * same anchor and Router instance: that conversation waits, the direct
 * conversation is recovered, and the run keeps running. Fails when the
 * position-ready step ends recovery with a persistence failure.
 */
const keepsOneCandidateAfterTheHeadMovesPastIt = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const { recovery, outbound, staged, successor, advancedRequest } =
      yield* advanceN4HeadPastStagedCandidate(fixture, n4);

    const [, , ready] = yield* answerN4Incomplete(fixture, n4, advancedRequest);
    const conversationId = n4.membership.descriptor.conversationId;
    const durable = yield* fixture.store.run(testStore.recover());

    expect(successor).toBe(acceptedDisposition);
    expect(ready).toBe(acceptedDisposition);
    expect(
      durable.stagedReanchors
        .filter((candidate) => candidate.conversationId === conversationId)
        .map(({ anchorHash }) => anchorHash),
    ).toEqual([staged.anchorHash]);
    expect(
      durable.evidence
        .filter(
          (row) =>
            row.kind === "reanchor" &&
            row.conversationId === conversationId &&
            row.evidenceKey === fixture.local.card.agentId,
        )
        .map(({ subjectId }) => subjectId),
    ).toEqual([staged.anchorHash]);
    expect(Option.isNone(yield* Queue.poll(outbound))).toBe(true);
    expect(yield* directAnchorHash(fixture)).toBe(
      yield* restartedDirectAnchorHash(fixture),
    );
    expect(yield* Fiber.await(recovery)).toStrictEqual(Exit.void);
    yield* Fiber.interrupt(recovery);
  });

/**
 * The canonical bytes of `message` with one bit of its signature flipped:
 * the same statement under a different signature.
 * @param message Signed evidence message to alter.
 * @returns Canonical bytes a store row holds for the altered message.
 */
const differentlySignedEvidence = (message: SignedMessage) =>
  corruptSignature(message).pipe(
    Effect.flatMap((altered) => encodeCanonical(SignedMessage, altered)),
    Effect.orDie,
  );

/**
 * A member's re-anchor vote whose evidence key already holds a different copy
 * of the same vote does not count: it is reported ignored and the run keeps
 * running. The row seeded through the store's public `mergeEvidence` stands
 * in for an earlier, differently signed copy of that vote, which a member can
 * produce because one statement has more than one valid signature. Fails
 * when the store's conflict on that key ends recovery with a persistence
 * failure.
 */
const ignoresReanchorVoteConflictingWithAStoredCopy = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery, proposal } = yield* proposeAtRestart(fixture);
    const peerVote = yield* signEvidenceMessage({
      statement: {
        ...proposal,
        signerAgentId: fixture.remote.card.agentId,
      },
      agentCard: fixture.remote.card,
      signingAuthority: fixture.remote.authority,
    });
    yield* fixture.store.run(
      testStore.mergeEvidence({
        conversationId: proposal.reanchor.conversationId,
        kind: "reanchor",
        subjectId: proposal.anchorHash,
        evidenceKey: fixture.remote.card.agentId,
        canonicalEvidence: yield* differentlySignedEvidence(peerVote),
      }),
    );

    const vote = yield* deliverRecovery(
      fixture.engine,
      peerReanchorVoteIngress(fixture, proposal),
    );

    expect(vote).toBe(ignoredDisposition);
    expect(yield* Fiber.await(recovery)).toStrictEqual(Exit.void);
    yield* Fiber.interrupt(recovery);
  });

/**
 * A two-member conversation between the fixture's endpoint and a new member,
 * which the endpoint's store does not hold yet, and its certified FIRST_MESSAGE
 * record.
 * @param fixture Endpoint the conversation includes.
 * @returns The new member, the conversation's membership, and its record.
 */
const unstoredConversationWith = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    const authority = yield* makeTestAuthority();
    const member: SigningIdentity = {
      card: yield* issueTestCard({
        byte: 5,
        name: "recovery-5",
        authority,
        registryKeys: fixture.registryKeys,
      }),
      authority,
    };
    const conversationId = yield* deriveConversationId([
      fixture.local.card.agentId,
      member.card.agentId,
    ]);
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptor)({
      sharnessVersion: WIRE_VERSION,
      kind: "membership_descriptor",
      conversationId,
      members: yield* Effect.forEach(
        [fixture.local, member],
        (identity) => Schema.encode(AgentCard)(identity.card),
        { concurrency: 1 },
      ),
    });
    const membership = yield* verifyMembershipDescriptor(
      descriptor,
      fixture.registrySignerPublicKey,
    );
    const firstMessage = yield* buildCertifiedFirstMessage(
      fixture.local,
      member,
      membership,
      oldRouterInstanceId,
    );
    return { member, membership, firstMessage };
  }).pipe(Effect.orDie);

/**
 * The endpoint staged its candidate at the N4 head, and a certified
 * successor then moved the head, so it can never stage another candidate for
 * that anchor and Router instance. The other three members certify a
 * re-anchor at the new head, and it reaches the endpoint by `arrival`. The
 * quorum certificate supersedes the endpoint's own staged candidate: the
 * completion becomes the conversation's anchor and replaces that candidate,
 * and a vote for the superseded candidate no longer counts. Fails when the
 * endpoint keeps refusing every completion in a scope it already claimed, so
 * the conversation never recovers.
 * @param arrival How the completion reaches the endpoint.
 * @returns The trace, run to completion.
 */
const adoptsCompletionSupersedingTheStagedCandidate = (
  arrival: CompletionArrival,
) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const { recovery, history, staged, advancedRequest } =
      yield* advanceN4HeadPastStagedCandidate(fixture, n4);
    const completed = yield* completedReanchorBy(
      {
        ...staged.reanchor,
        selectedRecordHash:
          history.certifiedSuccessor.actionCertifiedRecord.recordHash,
      },
      [fixture.remote, n4.third, n4.fourth],
    );

    const adopted = yield* deliverRecovery(
      n4.engine,
      arrival === "catch-up"
        ? catchUpPageIngressFrom({
            membership: n4.membership,
            responder: fixture.remote,
            request: advancedRequest,
            item: completed,
            routerInstanceId: newRouterInstanceId,
          })
        : directPacketIngressFrom({
            membership: n4.membership,
            sender: fixture.remote,
            packet: completed,
            routerInstanceId: newRouterInstanceId,
          }),
    );
    const staleVote = yield* deliverRecovery(
      n4.engine,
      peerReanchorVoteIngressFrom({
        membership: n4.membership,
        responder: n4.third,
        proposal: staged,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const recovered = yield* fixture.store.run(testStore.recover());
    const conversationId = n4.membership.descriptor.conversationId;

    expect(adopted).toBe(acceptedDisposition);
    expect(staleVote).toBe(ignoredDisposition);
    expect(
      recovered.positions.find(
        (position) => position.conversationId === conversationId,
      )?.currentAnchorHash,
    ).toBe(completed.anchorHash);
    expect(
      recovered.stagedReanchors
        .filter((candidate) => candidate.conversationId === conversationId)
        .map(({ anchorHash }) => anchorHash),
    ).toEqual([completed.anchorHash]);
    expect(
      recovered.evidence.filter(
        (evidence) =>
          evidence.kind === "reanchor" &&
          evidence.subjectId === staged.anchorHash,
      ),
    ).toEqual([]);
    yield* Fiber.interrupt(recovery);
  });

/**
 * A two-member conversation between the fixture's endpoint and a member whose
 * card a Registry other than the endpoint's issued, which the endpoint's
 * store does not hold, and its certified FIRST_MESSAGE record. Its membership is
 * assembled from the cards without checking them against any Registry.
 * @param fixture Endpoint the conversation includes.
 * @returns The foreign member, the conversation's membership, and its record.
 */
const conversationWithForeignCard = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    const authority = yield* makeTestAuthority();
    const member: SigningIdentity = {
      card: yield* issueTestCard({
        byte: 6,
        name: "foreign-6",
        authority,
        registryKeys: generateKeyPairSync("ed25519"),
      }),
      authority,
    };
    const conversationId = yield* deriveConversationId([
      fixture.local.card.agentId,
      member.card.agentId,
    ]);
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptor)({
      sharnessVersion: WIRE_VERSION,
      kind: "membership_descriptor",
      conversationId,
      members: yield* Effect.forEach(
        [fixture.local, member],
        (identity) => Schema.encode(AgentCard)(identity.card),
        { concurrency: 1 },
      ),
    });
    const membership: VerifiedMembership = {
      descriptor,
      hash: yield* hashMembershipDescriptor(descriptor),
      members: [fixture.local.card, member.card],
    };
    const firstMessage = yield* buildCertifiedFirstMessage(
      fixture.local,
      member,
      membership,
      oldRouterInstanceId,
    );
    return { member, membership, firstMessage };
  }).pipe(Effect.orDie);

/**
 * A certified MESSAGE the member of a two-member conversation authors directly
 * after its FIRST_MESSAGE, signed and voted by both members.
 * @param conversation The conversation's member, membership and FIRST_MESSAGE.
 * @param conversation.member Author of the message, and one of its two signers.
 * @param conversation.membership Membership the message names by hash.
 * @param conversation.firstMessage Record the message extends, under its anchor.
 * @param local The fixture's endpoint, which also signs and votes.
 * @returns The certified record.
 */
const certifiedMessageAfterFirstMessage = (
  conversation: {
    readonly member: SigningIdentity;
    readonly membership: VerifiedMembership;
    readonly firstMessage: CertifiedRecord;
  },
  local: SigningIdentity,
): Effect.Effect<CertifiedRecord> =>
  Effect.gen(function* () {
    const { member, membership, firstMessage } = conversation;
    const signers = [local, member].sort((left, right) =>
      compareAgentIds(left.card.agentId, right.card.agentId),
    );
    const intent: MessageIntent = {
      sharnessVersion: WIRE_VERSION,
      kind: "message_intent",
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      authorAgentId: member.card.agentId,
      msgId: yield* mintMsgId(),
      content: [{ type: "text", text: "message after first message" }],
    };
    const anchorHash = firstMessage.actionCertifiedRecord.recordCore.anchorHash;
    const action: MessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "MESSAGE",
      conversationId: membership.descriptor.conversationId,
      membershipHash: membership.hash,
      anchorHash,
      previousRecordHash: firstMessage.actionCertifiedRecord.recordHash,
      messageIntent: intent,
      messageIntentHash: yield* hashMessageIntent(intent),
    };
    const actionHash = yield* hashAction(action);
    const recordCore: RecordCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "record_core",
      membershipHash: membership.hash,
      anchorHash,
      action,
      actionHash,
    };
    const recordHash = yield* hashRecord(recordCore);
    const [firstSignature, ...signatures] = yield* Effect.forEach(
      signers,
      (signer) =>
        signEvidence(signer, {
          sharnessVersion: WIRE_VERSION,
          kind: "action_signature",
          signerAgentId: signer.card.agentId,
          actionHash,
        }),
      { concurrency: 1 },
    );
    const [firstVote, ...votes] = yield* Effect.forEach(
      signers,
      (signer) =>
        signEvidence(signer, {
          sharnessVersion: WIRE_VERSION,
          kind: "durability_vote",
          signerAgentId: signer.card.agentId,
          conversationId: membership.descriptor.conversationId,
          membershipHash: membership.hash,
          recordHash,
        }),
      { concurrency: 1 },
    );
    if (firstSignature === undefined || firstVote === undefined) {
      return yield* Effect.dieMessage("a certificate needs signers");
    }
    const record: CertifiedRecord = {
      sharnessVersion: WIRE_VERSION,
      kind: "certified_record",
      actionCertifiedRecord: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_certified_record",
        recordHash,
        recordCore,
        routerAnchor: firstMessage.actionCertifiedRecord.routerAnchor,
        actionCertificate: {
          sharnessVersion: WIRE_VERSION,
          kind: "action_certificate",
          actionHash,
          signatures: [firstSignature, ...signatures],
        },
      },
      durabilityCertificate: {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_certificate",
        recordHash,
        votes: [firstVote, ...votes],
      },
    };
    return record;
  }).pipe(Effect.orDie);

/**
 * A member offers a conversation whose descriptor holds a card a Registry
 * other than this endpoint's issued. Its FIRST_MESSAGE, as an action-certified copy
 * or as a certified record, is ignored and stores no conversation, while a
 * conversation whose cards this endpoint's Registry issued is accepted. Fails
 * when an endpoint takes a conversation's membership from a FIRST_MESSAGE without
 * verifying its cards against the Registry key.
 */
const ignoresFirstMessageWithACardAnotherRegistryIssued = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const foreign = yield* conversationWithForeignCard(fixture);
    const issued = yield* unstoredConversationWith(fixture);
    const deliver = (conversation: typeof foreign, packet: DirectPacket) =>
      directPacketIngressFrom({
        membership: conversation.membership,
        sender: conversation.member,
        packet,
        routerInstanceId: oldRouterInstanceId,
      }).pipe(
        Effect.flatMap((ingress) =>
          fixture.engine.acceptRouterIngress(ingress),
        ),
      );

    const foreignDispositions = [
      yield* deliver(foreign, foreign.firstMessage.actionCertifiedRecord),
      yield* deliver(foreign, foreign.firstMessage),
    ];
    const afterForeign = yield* fixture.store.run(testStore.recover());
    const issuedDisposition = yield* deliver(issued, issued.firstMessage);

    expect(foreignDispositions).toEqual([
      ignoredDisposition,
      ignoredDisposition,
    ]);
    expect(
      afterForeign.positions.some(
        ({ conversationId }) =>
          conversationId === foreign.membership.descriptor.conversationId,
      ),
    ).toBe(false);
    expect(issuedDisposition).toBe(acceptedDisposition);
  });

/**
 * A MESSAGE record names its membership only by hash, so an endpoint that does
 * not hold the conversation ignores it and stores nothing. Once the
 * conversation's FIRST_MESSAGE arrives, the same MESSAGE certifies. Fails when an
 * unheld conversation's MESSAGE is accepted, or is treated as a fault instead of
 * being ignored.
 */
const ignoresAMessageForAConversationItDoesNotHold = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const created = yield* unstoredConversationWith(fixture);
    const message = yield* certifiedMessageAfterFirstMessage(
      created,
      fixture.local,
    );
    const deliver = (packet: DirectPacket) =>
      directPacketIngressFrom({
        membership: created.membership,
        sender: created.member,
        packet,
        routerInstanceId: oldRouterInstanceId,
      }).pipe(
        Effect.flatMap((ingress) =>
          fixture.engine.acceptRouterIngress(ingress),
        ),
      );
    const { conversationId } = created.membership.descriptor;

    const early = yield* deliver(message);
    const beforeFirstMessage = (yield* fixture.store.run(
      testStore.recover(),
    )).positions.find((held) => held.conversationId === conversationId);
    const firstMessage = yield* deliver(created.firstMessage);
    const later = yield* deliver(message);
    const afterMessage = (yield* fixture.store.run(
      testStore.recover(),
    )).positions.find((held) => held.conversationId === conversationId);

    expect([early, firstMessage, later]).toEqual([
      ignoredDisposition,
      acceptedDisposition,
      acceptedDisposition,
    ]);
    expect(beforeFirstMessage).toBeUndefined();
    expect(afterMessage?.headRecordHash).toBe(
      message.actionCertifiedRecord.recordHash,
    );
  });

/**
 * A member creates a new conversation with this endpoint while it recovers,
 * and the conversation's certified FIRST_MESSAGE record arrives as recovery
 * traffic. The run holds no membership for that conversation, so it applies
 * the record without asking for its history and keeps running. Fails when
 * asking for that conversation's history ends recovery with a persistence
 * failure.
 */
const acceptsFirstMessageOfAConversationCreatedDuringRecovery = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const created = yield* unstoredConversationWith(fixture);
    const { recovery, outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
    );
    yield* takeCatchUpRequest(outbound);

    const firstMessage = yield* deliverRecovery(
      fixture.engine,
      directPacketIngressFrom({
        membership: created.membership,
        sender: created.member,
        packet: created.firstMessage,
        routerInstanceId: oldRouterInstanceId,
      }),
    ).pipe(Effect.exit);
    const position = (yield* fixture.store.run(
      testStore.recover(),
    )).positions.find(
      ({ conversationId }) =>
        conversationId === created.membership.descriptor.conversationId,
    );

    expect(firstMessage).toStrictEqual(Exit.succeed(acceptedDisposition));
    expect(position?.headRecordHash).toBe(
      created.firstMessage.actionCertifiedRecord.recordHash,
    );
    expect(yield* Fiber.await(recovery)).toStrictEqual(Exit.void);
    yield* Fiber.interrupt(recovery);
  });

/**
 * The run re-anchors the fixture conversation and keeps running while the N4
 * conversation waits for its catch-up. A member's vote from the new anchor at
 * the same Router instance asks for a second re-anchor nobody needs, so it is
 * reported ignored and the endpoint stages no candidate for it. Fails when
 * the vote stages a candidate from the new anchor and the endpoint signs a
 * vote for it.
 */
const ignoresReanchorVoteAfterTheConversationReanchored = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const n4 = yield* addN4Foundation(fixture);
    const { recovery, outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      newRouterInstanceId,
    );
    const requests = yield* takeN4Requests(fixture, n4, outbound);
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: requests.direct,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const proposal = yield* takeReanchorVote(outbound);
    const completing = yield* deliverRecovery(
      n4.engine,
      peerReanchorVoteIngress(fixture, proposal),
    );

    const fromNewAnchor = yield* deliverRecovery(
      n4.engine,
      peerVoteFor(fixture, {
        ...proposal.reanchor,
        previousAnchorHash: proposal.anchorHash,
      }),
    );
    const staged = (yield* fixture.store.run(
      testStore.recover(),
    )).stagedReanchors.filter(
      ({ conversationId }) =>
        conversationId === fixture.membership.descriptor.conversationId,
    );

    expect(completing).toBe(acceptedDisposition);
    expect(yield* directAnchorHash(fixture)).toBe(proposal.anchorHash);
    expect(fromNewAnchor).toBe(ignoredDisposition);
    expect(staged.map(({ anchorHash }) => anchorHash)).toEqual([
      proposal.anchorHash,
    ]);
    expect(yield* Fiber.await(recovery)).toStrictEqual(Exit.void);
    yield* Fiber.interrupt(recovery);
  });

// @agent-code-guard/regression-only: these traces pin that input from a peer never ends a recovery run.
describe("peer input during recovery", () => {
  it(
    "ignores a FIRST_MESSAGE whose membership holds a card another Registry issued",
    ignoresFirstMessageWithACardAnotherRegistryIssued,
  );
  it(
    "ignores a MESSAGE for a conversation it does not hold, and certifies it after its FIRST_MESSAGE",
    ignoresAMessageForAConversationItDoesNotHold,
  );
  it(
    "ignores a re-anchor vote whose previous anchor this endpoint cannot resolve",
    ignoresReanchorVoteFromUnknownPreviousAnchor,
  );
  it(
    "ignores a re-anchor vote for an unknown head once a candidate is staged",
    ignoresReanchorVoteForUnknownHeadOnceStaged,
  );
  it(
    "re-anchors at its own head past a held vote for a head catch-up never supplied",
    reanchorsPastHeldVoteForUnsuppliedHead,
  );
  it(
    "ignores a re-anchor vote for a conversation restarted empty",
    ignoresReanchorVoteForRestartedEmptyConversation,
  );
  it(
    "ignores a relayed completed re-anchor for a conversation restarted empty",
    ignoresRelayedCompletionForRestartedEmptyConversation,
  );
  it(
    "ignores a catch-up page whose successor conflicts with one already applied",
    ignoresCatchUpPageWithConflictingSuccessor,
  );
  it(
    "ignores a catch-up page whose record does not extend the conversation",
    ignoresCatchUpPageWhoseRecordDoesNotExtendTheConversation,
  );
  it(
    "accepts a certified record delivered before the recovery run starts",
    acceptsCertifiedRecordBeforeTheRunStarts,
  );
  it(
    "ignores a re-anchor vote at a head past the staged candidate",
    ignoresReanchorVotePastTheStagedCandidate,
  );
  it(
    "keeps one candidate after a certified successor moves the head past it",
    keepsOneCandidateAfterTheHeadMovesPastIt,
  );
  it(
    "ignores a re-anchor vote that conflicts with a stored copy of the same vote",
    ignoresReanchorVoteConflictingWithAStoredCopy,
  );
  it("adopts a caught-up re-anchor that supersedes its staged candidate", () =>
    adoptsCompletionSupersedingTheStagedCandidate("catch-up"));
  it("adopts a relayed re-anchor that supersedes its staged candidate", () =>
    adoptsCompletionSupersedingTheStagedCandidate("relay"));
  it(
    "accepts the FIRST_MESSAGE of a conversation created during recovery",
    acceptsFirstMessageOfAConversationCreatedDuringRecovery,
  );
  it(
    "ignores a re-anchor vote once its conversation has re-anchored",
    ignoresReanchorVoteAfterTheConversationReanchored,
  );
});

/**
 * Decodes the catch-up page an outbox row carries.
 * @param local Endpoint whose outbox holds the row.
 * @returns For an outbox row holding one signed outer envelope, the page; any
 *     other body is a defect naming what the row carries.
 */
function decodeQueuedCatchUpPage(local: SigningIdentity) {
  return (row: StoredOutboundMessage) =>
    decodeCanonical(SignedMessage, row.canonicalSignedMessage).pipe(
      Effect.flatMap((message) => openOuterBody(message, local)),
      Effect.flatMap((body) => {
        if (body.kind === "direct" && body.packet.kind === "catch_up_page") {
          return Effect.succeed(body.packet);
        }
        const received =
          body.kind === "evidence" ? body.kind : body.packet.kind;
        return Effect.dieMessage(
          `expected catch-up page, received ${received}`,
        );
      }),
      Effect.orDie,
    );
}

/**
 * A member's catch-up request for a conversation this endpoint holds history
 * in is answered with the first certified record, in a page the requester
 * verifies. Fails when the responder picks the wrong successor, answers
 * incomplete although it holds history, or signs an attestation that does
 * not match the page.
 */
const answersCatchUpRequestWithItsFirstCertifiedRecord = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);

    const page = yield* queuePeerCatchUpResponse(fixture).pipe(
      Effect.flatMap(decodeQueuedCatchUpPage(fixture.local)),
    );
    const verified = yield* Effect.exit(
      verifyCatchUpPage({
        page,
        membership: fixture.membership,
        responseSenderAgentId: fixture.local.card.agentId,
      }),
    );

    expect(page).toMatchObject({
      hasMore: false,
      item: {
        kind: "certified_record",
        actionCertifiedRecord: {
          recordHash: fixture.certifiedRecord.actionCertifiedRecord.recordHash,
        },
      },
    });
    expect(verified).toStrictEqual(Exit.void);
  });

/**
 * A member's catch-up request that arrives while this endpoint is itself
 * recovering is answered at once: the answer goes out through the durable
 * outbox, which the Router worker sends from while the run still recovers,
 * and no row is left once it is sent. Fails when the answer waits for this
 * endpoint's own recovery to finish, so two endpoints recovering together
 * each wait for the other's answer.
 */
const answersCatchUpRequestDuringItsOwnRecovery = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recovery, outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
    );
    const own = yield* takeCatchUpRequest(outbound);
    const peerRequest = peerCatchUpRequest(fixture);

    const answered = yield* deliverRecovery(
      fixture.engine,
      directPacketIngressFrom({
        membership: fixture.membership,
        sender: fixture.remote,
        packet: peerRequest,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const answer = yield* Queue.take(outbound).pipe(
      Effect.flatMap((message) => openForwarded(message)),
    );
    const durable = (yield* fixture.store.run(testStore.recover()))
      .outboundMessages;
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: own,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    yield* Fiber.join(recovery);

    expect(answered).toBe(acceptedDisposition);
    expect(answer).toMatchObject({
      kind: "direct",
      packet: { kind: "catch_up_incomplete", request: peerRequest },
    });
    expect(durable).toEqual([]);
  });

/**
 * The N4 position is ready but waits behind a staged successor. A member's
 * vote selecting that successor, which this endpoint holds only staged, is
 * held, and the run asks the members again for the history after its head.
 * Fails when the vote is held without a new request, so the position waits
 * for the rest of the run.
 */
const requestsCatchUpForAVoteAtAnUnknownHead = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: fixture.remote,
      packet: history.certifiedHead,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: n4.fourth,
      packet: history.stagedSuccessor,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const { recovery, outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      newRouterInstanceId,
    );
    const requests = yield* takeN4Requests(fixture, n4, outbound);
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: requests.direct,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    yield* answerN4Incomplete(fixture, n4, requests.group);
    const waiting = yield* Queue.poll(outbound);
    const atSuccessor: ReanchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_body",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      previousAnchorHash:
        history.certifiedHead.actionCertifiedRecord.recordCore.anchorHash,
      selectedRecordHash: history.stagedSuccessor.recordHash,
      routerInstanceId: newRouterInstanceId,
    };

    const vote = yield* deliverRecovery(
      n4.engine,
      peerReanchorVoteIngressFrom({
        membership: n4.membership,
        responder: n4.fourth,
        proposal: {
          sharnessVersion: WIRE_VERSION,
          kind: "reanchor_vote",
          signerAgentId: n4.fourth.card.agentId,
          anchorHash: yield* hashAnchor(atSuccessor),
          reanchor: atSuccessor,
        },
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const request = yield* takeCatchUpRequest(outbound);

    expect(Option.isNone(waiting)).toBe(true);
    expect(vote).toBe(acceptedDisposition);
    expect(request).toMatchObject({
      conversationId: n4.membership.descriptor.conversationId,
      knownRecordHash: history.certifiedHead.actionCertifiedRecord.recordHash,
    });
    yield* Fiber.interrupt(recovery);
  });

/**
 * The conversation's re-anchor completed after this endpoint voted and before
 * it recorded the completion, and a member's catch-up page carries the
 * completed re-anchor. The run adopts it, asks for the history after the new
 * anchor, relays the completion, and recovers without proposing a re-anchor
 * of its own. Fails when the adopted anchor does not become the
 * conversation's current anchor, so the run proposes a second re-anchor at
 * the same Router instance.
 */
const adoptsReanchorCompletedWhileItWasDown = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const { recordCore, recordHash } =
      fixture.certifiedRecord.actionCertifiedRecord;
    const reanchor: ReanchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_body",
      conversationId: recordCore.action.conversationId,
      membershipHash: fixture.membership.hash,
      previousAnchorHash: recordCore.anchorHash,
      selectedRecordHash: recordHash,
      routerInstanceId: newRouterInstanceId,
    };
    const completed = yield* completedReanchorBy(reanchor, [
      fixture.local,
      fixture.remote,
    ]);
    const { anchorHash } = completed;
    const { recovery, outbound, request } =
      yield* startRestartRecovery(fixture);

    const page = yield* deliverRecovery(
      fixture.engine,
      catchUpPageIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request,
        item: completed,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const next = yield* takeCatchUpRequest(outbound);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, next),
    );
    const relayed = yield* Queue.take(outbound).pipe(
      Effect.flatMap((message) => openForwarded(message)),
    );
    yield* Fiber.join(recovery);

    expect(page).toBe(acceptedDisposition);
    expect(next).toMatchObject({
      knownRecordHash: recordHash,
      knownAnchorHash: anchorHash,
    });
    expect(relayed).toMatchObject({
      kind: "direct",
      packet: { kind: "completed_reanchor", anchorHash },
    });
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.currentAnchorHash,
    ).toBe(anchorHash);
  });

/**
 * An endpoint answers a catch-up request only for the member that sent it,
 * under the conversation's membership. A request naming another member as
 * its requester, one naming a membership hash other than the conversation's,
 * and one this endpoint sent itself are each reported ignored, and nothing is
 * queued in answer. Fails when the endpoint answers a request made on another
 * member's behalf, under another membership, or by itself.
 */
const ignoresCatchUpRequestsItMustNotAnswer = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const request = peerCatchUpRequest(fixture);
    const deliver = (sender: SigningIdentity, packet: CatchUpRequest) =>
      directPacketIngressFrom({
        membership: fixture.membership,
        sender,
        packet,
        routerInstanceId: oldRouterInstanceId,
      }).pipe(
        Effect.flatMap((ingress) =>
          fixture.engine.acceptRouterIngress(ingress),
        ),
      );

    const dispositions = yield* Effect.all({
      forAnotherMember: deliver(fixture.remote, {
        ...request,
        requesterAgentId: fixture.local.card.agentId,
      }),
      underAnotherMembership: deliver(fixture.remote, {
        ...request,
        membershipHash: Schema.decodeUnknownSync(MembershipHash)(
          digest("mbr_", 7),
        ),
      }),
      fromItself: deliver(fixture.local, {
        ...request,
        requesterAgentId: fixture.local.card.agentId,
      }),
    });
    const queued = (yield* fixture.store.run(testStore.recover()))
      .outboundMessages;

    expect(dispositions).toEqual({
      forAnotherMember: ignoredDisposition,
      underAnotherMembership: ignoredDisposition,
      fromItself: ignoredDisposition,
    });
    expect(queued).toEqual([]);
  });

/**
 * A member catching up from no history is answered through the record and
 * the completed re-anchor this endpoint holds after it. The first page
 * carries the record and says more history follows; the page for the
 * record's position carries the completed re-anchor and says none follows.
 * The requester verifies both. Fails when the endpoint reports no further
 * history ahead of a stored re-anchor, or answers a position that a stored
 * completed re-anchor follows as incomplete.
 */
const answersCatchUpThroughAStoredCompletedReanchor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const { proposal } = yield* reanchorUntilCompletionSend(fixture);
    const { recovery, request } = yield* startRestartRecovery(fixture);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, request),
    );
    yield* Fiber.join(recovery);
    const { recordCore, recordHash } =
      fixture.certifiedRecord.actionCertifiedRecord;

    const first = yield* queuePeerCatchUpResponse(fixture).pipe(
      Effect.flatMap(decodeQueuedCatchUpPage(fixture.local)),
    );
    const next = yield* queuePeerCatchUpResponse(fixture, {
      knownRecordHash: recordHash,
      knownAnchorHash: recordCore.anchorHash,
    }).pipe(Effect.flatMap(decodeQueuedCatchUpPage(fixture.local)));
    const verified = yield* Effect.forEach(
      [first, next],
      (page) =>
        Effect.exit(
          verifyCatchUpPage({
            page,
            membership: fixture.membership,
            responseSenderAgentId: fixture.local.card.agentId,
          }),
        ),
      { concurrency: 1 },
    );

    expect(first).toMatchObject({
      hasMore: true,
      item: {
        kind: "certified_record",
        actionCertifiedRecord: { recordHash },
      },
    });
    expect(next).toMatchObject({
      hasMore: false,
      item: { kind: "completed_reanchor", anchorHash: proposal.anchorHash },
    });
    expect(verified).toStrictEqual([Exit.void, Exit.void]);
  });

/**
 * Opens the fixture's remote member as a second endpoint with its own store,
 * holding the conversation foundation and the certified first message record the
 * fixture endpoint holds once it retains that record. The remote member
 * authored that record, so its store also binds the record's message intent,
 * as an author's store does before its message certifies.
 * @param fixture Endpoint whose remote member the peer runs as.
 * @returns The peer engine and its store.
 */
const openPeerEngine = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    const store = yield* openFirstMessageStore(
      fixture.membership,
      fixture.certifiedRecord,
      "sharness-recovery-peer-",
    );
    const recoveryOutbound = yield* Queue.unbounded<SignedMessage>();
    const normalOutbound = yield* Queue.unbounded<SignedMessage>();
    yield* store.run(saveLocalIdentity(fixture.remote.card));
    const engine = yield* makeEndpointEngine({
      ...fixture.input,
      localAgentCard: fixture.remote.card,
      signingAuthority: fixture.remote.authority,
      routerWorker: makeFixtureRouter({
        store,
        local: fixture.remote,
        normalOutbound,
        recoveryOutbound,
      }),
    }).pipe(Effect.provideService(SqlClient.SqlClient, store.sql));
    const { messageIntent } =
      fixture.certifiedRecord.actionCertifiedRecord.recordCore.action;
    const canonicalMessage = yield* encodeCanonical(
      Message,
      yield* messageFromIntent(
        fixture.membership.members.map(agentViewOf),
        messageIntent,
      ),
    );
    yield* store.run(
      testStore.acceptMessage({
        authorAgentId: messageIntent.authorAgentId,
        msgId: messageIntent.msgId,
        conversationId: messageIntent.conversationId,
        canonicalMessage,
      }),
    );
    yield* store.run(
      testStore.bindMessageIntent({
        kind: "existing-conversation",
        intent: {
          conversationId: messageIntent.conversationId,
          membershipHash: messageIntent.membershipHash,
          authorAgentId: messageIntent.authorAgentId,
          msgId: messageIntent.msgId,
          canonicalIntent: yield* encodeCanonical(MessageIntent, messageIntent),
          canonicalMessage,
        },
      }),
    );
    yield* directPacketIngressFrom({
      membership: fixture.membership,
      sender: fixture.local,
      packet: fixture.certifiedRecord,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(Effect.flatMap((ingress) => engine.acceptRouterIngress(ingress)));
    return { engine, store, recoveryOutbound, normalOutbound };
  }).pipe(Effect.provide(NodeFileSystem.layer), Effect.orDie);

/** The members at either end of relayed recovery traffic. */
interface RelayMembers {
  readonly sender: SigningIdentity;
  readonly receiver: SigningIdentity;
}

/**
 * Delivers each envelope one endpoint's recovery sends to `engine` as
 * recovery traffic, in send order, the way the Router worker polls it while
 * a recovery runs.
 * @param sent Queue the sending endpoint's recovery sends to.
 * @param engine Endpoint that receives the traffic.
 * @param members The member whose recovery sends the traffic, and the member
 *     whose endpoint `engine` is, which opens each body.
 * @param members.sender Member whose recovery sends the traffic.
 * @param members.receiver Member whose endpoint `engine` is.
 * @param routerInstanceId The Router instance the traffic comes through.
 * @returns The receiving endpoint's dispositions, in delivery order.
 */
const relayRecoveryTraffic = (
  sent: Queue.Queue<SignedMessage>,
  engine: EndpointEngine,
  { sender, receiver }: RelayMembers,
  routerInstanceId: typeof RouterInstanceId.Type = newRouterInstanceId,
) =>
  Effect.gen(function* () {
    const dispositions = yield* Queue.unbounded<RouterIngressDisposition>();
    yield* Queue.take(sent).pipe(
      Effect.flatMap(Schema.encode(SignedMessage)),
      Effect.flatMap(Schema.decodeUnknown(SignedMessage.live)),
      Effect.flatMap((signedMessage) =>
        SignedMessage.verify({ signedMessage, agentCard: sender.card }),
      ),
      Effect.flatMap((message) =>
        decodeOuterBody({
          message,
          agentCard: receiver.card,
          signingAuthority: receiver.authority,
        }).pipe(
          Effect.flatMap((payload) =>
            engine.acceptRouterIngress({
              routerInstanceId,
              message,
              senderCard: sender.card,
              payload,
            }),
          ),
        ),
      ),
      Effect.flatMap((disposition) => Queue.offer(dispositions, disposition)),
      Effect.forever,
      Effect.orDie,
      Effect.forkScoped,
    );
    return dispositions;
  });

/**
 * Waits until a store's only conversation has left `oldAnchor`, as both
 * members' runs finish the re-anchor after their recoveries have started.
 * @param store Endpoint store to watch.
 * @param oldAnchor Anchor the conversation held before the restart.
 * @returns The store's positions once the conversation re-anchored.
 */
const awaitReanchoredFrom = (store: TestDatabase, oldAnchor: string) =>
  store.run(testStore.recover()).pipe(
    Effect.map(({ positions }) => positions),
    Effect.filterOrFail(
      (positions) =>
        positions[0] !== undefined &&
        positions[0].currentAnchorHash !== oldAnchor,
      () => "the conversation has not re-anchored yet",
    ),
    Effect.retry(Schedule.spaced("10 millis")),
    Effect.orDie,
  );

/**
 * A short live pause while the test clock stands still. A check that nothing
 * was sent reads after it, and a clock driver pauses with it between moves.
 * Nothing waits on it for traffic that must arrive: sealing settles on real
 * promises, so under load a pause ends before the forwarding fibers deliver,
 * which only an absence check survives.
 */
const settle = TestServices.provideLive(Effect.sleep("30 millis"));

/**
 * Runs `wait` while the test clock moves on after each live pause, until
 * `wait` ends. One clock move can finish before a fiber still signing its
 * previous envelope has scheduled its next delay, so no fixed number of moves
 * is sure to release what `wait` waits for.
 * @param wait Effect that ends once what it waits for arrives.
 * @returns What `wait` returns.
 */
const whileTheClockRuns = <A, E>(wait: Effect.Effect<A, E>) =>
  wait.pipe(
    Effect.raceFirst(
      TestClock.adjust("100000 seconds").pipe(
        Effect.zipRight(settle),
        Effect.forever,
      ),
    ),
  );

/**
 * Takes the next catch-up request a recovery under the caller's test clock
 * retries, moving the clock on until it is sent.
 * @param outbound Queue the recovery sends to.
 * @returns The decoded catch-up request.
 */
const takeRetriedCatchUpRequest = (outbound: Queue.Queue<SignedMessage>) =>
  whileTheClockRuns(Queue.take(outbound)).pipe(
    Effect.flatMap(decodeCatchUpRequest),
  );

const forkSend = (engine: EndpointEngine, message: Message) =>
  Effect.fork(engine.send(message));

/**
 * After a Router discontinuity the endpoint holds two conversations. The
 * direct one's only other member is silent, and the N4 conversation's members
 * answer its catch-up. One answer is short of a quorum even when it arrives
 * twice, as an outer message Router appended again after eviction would, so
 * the N4 message still waits; with a second member's answer, the N4 conversation
 * recovers, its message reaches the Router, and its Router-ordered proposal and a
 * member's signature are accepted. Meanwhile the direct conversation stays
 * fenced and the Router receives nothing of its message. Once the direct member
 * answers, that conversation recovers and its held message reaches the Router.
 * Fails when the silent member's conversation holds the N4 conversation's
 * message, as an engine-wide fence does, or when a fenced conversation's message
 * reaches the Router before that conversation recovers.
 */
const recoversOneConversationWhileAnotherWaitsOnASilentMember = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const { outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "feed_gap",
      oldRouterInstanceId,
    );
    const requests = [
      yield* takeCatchUpRequest(outbound),
      yield* takeCatchUpRequest(outbound),
    ];
    const n4Request = requests.find(
      (request) =>
        request.conversationId === n4.membership.descriptor.conversationId,
    );
    const directRequest = requests.find(
      (request) =>
        request.conversationId === fixture.membership.descriptor.conversationId,
    );
    if (n4Request === undefined || directRequest === undefined) {
      return yield* Effect.dieMessage(
        "recovery did not ask both conversations' members",
      );
    }
    const n4Incomplete = (responder: SigningIdentity) =>
      deliverRecovery(
        n4.engine,
        catchUpIncompleteIngressFrom({
          membership: n4.membership,
          responder,
          request: n4Request,
          routerInstanceId: oldRouterInstanceId,
        }),
      );
    yield* n4Incomplete(fixture.remote);
    yield* n4Incomplete(fixture.remote);
    const directSend = yield* forkSend(
      n4.engine,
      yield* authoredMessage(fixture, [
        { type: "text", text: "held behind the silent member" },
      ]),
    );
    yield* forkSend(
      n4.engine,
      yield* authoredMessage(
        fixture,
        [{ type: "text", text: "the recovered conversation sends" }],
        {
          recipients: [fixture.remote, n4.third, n4.fourth].map((member) =>
            agentViewOf(member.card),
          ),
        },
      ),
    );
    const sentBelowQuorum = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    ).pipe(Effect.timeoutOption("300 millis"));
    yield* n4Incomplete(n4.third);
    const proposal = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );
    yield* settle;
    const reachedRouterWhileFenced = yield* directProposalsIn(
      fixture.normalOutbound,
      fixture.membership.descriptor.conversationId,
    );
    const heldSend = yield* Fiber.poll(directSend);
    const ordered = yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: fixture.local,
      packet: proposal.proposal,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const signed = yield* peerEvidenceIngressFrom({
      membership: n4.membership,
      responder: n4.third,
      statement: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_signature",
        signerAgentId: n4.third.card.agentId,
        actionHash: yield* hashAction(proposal.proposal.action),
      },
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngress(fixture, directRequest),
    );
    const released = yield* takeActionProposalIn(
      fixture.normalOutbound,
      fixture.membership.descriptor.conversationId,
    );

    expect(Option.isNone(sentBelowQuorum)).toBe(true);
    expect(proposal.proposal.action.conversationId).toBe(
      n4.membership.descriptor.conversationId,
    );
    expect([ordered, signed]).toEqual([
      acceptedDisposition,
      acceptedDisposition,
    ]);
    expect(reachedRouterWhileFenced).toBe(0);
    expect(Option.isNone(heldSend)).toBe(true);
    expect(released.proposal.action.conversationId).toBe(
      fixture.membership.descriptor.conversationId,
    );
  });

/**
 * A conversation's member stays silent, so its catch-up retries run: each
 * delay doubles within the jitter bounds, and the retries stop after
 * {@link catchUpRetryAttempts}. Fails when the retries do not back off, or
 * keep going without bound.
 */
const backsOffCatchUpRetriesAndStops = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
      { clock: "caller" },
    );
    const first = yield* takeCatchUpRequest(outbound);
    const pendingAt = (elapsed: Duration.DurationInput) =>
      TestClock.setTime(Duration.toMillis(elapsed)).pipe(
        Effect.zipRight(settle),
        Effect.zipRight(Queue.size(outbound)),
      );
    const retries = (count: number) =>
      Effect.replicateEffect(takeCatchUpRequest(outbound), count).pipe(
        Effect.map((taken) => taken.length),
      );

    const beforeFirstRetry = yield* pendingAt("790 millis");
    yield* TestClock.setTime(1200);
    const firstRetry = yield* retries(1);
    const beforeSecondRetry = yield* pendingAt("2390 millis");
    yield* TestClock.setTime(3600);
    const secondRetry = yield* retries(1);
    const beforeThirdRetry = yield* pendingAt("5590 millis");
    yield* TestClock.setTime(8400);
    const thirdRetry = yield* retries(1);
    const laterRetries = yield* Effect.replicateEffect(
      takeRetriedCatchUpRequest(outbound),
      catchUpRetryAttempts - 3,
    ).pipe(Effect.map((taken) => taken.length));
    const afterLastRetry = yield* TestClock.adjust("100000 seconds").pipe(
      Effect.zipRight(settle),
      Effect.zipRight(TestClock.adjust("100000 seconds")),
      Effect.zipRight(settle),
      Effect.zipRight(Queue.size(outbound)),
    );

    expect(first.conversationId).toBe(
      fixture.membership.descriptor.conversationId,
    );
    expect([beforeFirstRetry, beforeSecondRetry, beforeThirdRetry]).toEqual([
      0, 0, 0,
    ]);
    expect(firstRetry + secondRetry + thirdRetry + laterRetries).toBe(
      catchUpRetryAttempts,
    );
    expect(afterLastRetry).toBe(0);
  });

/**
 * A conversation's catch-up retries run out while its member is silent. The
 * member then sends traffic for it, which arms catch-up again with a fresh
 * request; the member's answer recovers the conversation, no further retry
 * follows, and its send goes out. Fails when a conversation whose retries ran
 * out has no way back to recovery.
 */
const rearmsCatchUpAfterRetriesRunOut = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
      { clock: "caller" },
    );
    yield* takeCatchUpRequest(outbound);
    const exhausted = yield* Effect.replicateEffect(
      takeRetriedCatchUpRequest(outbound),
      catchUpRetryAttempts,
    );
    yield* TestClock.adjust("100000 seconds");
    yield* settle;
    const quiet = yield* Queue.size(outbound);

    yield* fixture.engine.acceptRouterIngress(
      yield* directPacketIngressFrom({
        membership: fixture.membership,
        sender: fixture.remote,
        packet: peerCatchUpRequest(fixture),
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const fresh = yield* takeNextCarrying(outbound, "catch_up_request").pipe(
      Effect.flatMap(decodeCatchUpRequest),
    );
    yield* settle;
    const laterRequests = yield* Effect.filter(
      yield* Queue.takeAll(outbound),
      (message) =>
        openForwarded(message).pipe(
          Effect.map(
            (body) =>
              body.kind === "direct" && body.packet.kind === "catch_up_request",
          ),
        ),
    );
    yield* fixture.engine.acceptRouterIngress(
      yield* catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: fresh,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    yield* TestClock.adjust("100000 seconds");
    yield* settle;
    const afterRecovery = yield* Queue.size(outbound);
    yield* forkSend(
      fixture.engine,
      yield* authoredMessage(fixture, [
        { type: "text", text: "recovered after the retries ran out" },
      ]),
    );
    const proposal = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );

    expect(exhausted).toHaveLength(catchUpRetryAttempts);
    expect(quiet).toBe(0);
    expect(laterRequests).toEqual([]);
    expect(afterRecovery).toBe(0);
    expect(proposal.proposal.action.conversationId).toBe(
      fixture.membership.descriptor.conversationId,
    );
  });

/**
 * The endpoint's message in the N4 conversation is proposed at the certified
 * head and sent, but the Router never orders it before a feed gap. Catch-up
 * then brings a member's certified successor of that head. The endpoint
 * proposes nothing while the conversation is still fenced, because it would
 * ignore the Router's echo of its own proposal; once the conversation
 * recovers it proposes the message at the new head. Fails when catching up a
 * record re-proposes a pending message while its conversation is fenced, so
 * members sign a proposal its author never signs.
 */
const proposesPendingMessagesOnlyOnceItsConversationRecovers = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: fixture.remote,
      packet: history.certifiedHead,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    yield* forkSend(
      n4.engine,
      yield* authoredMessage(
        fixture,
        [{ type: "text", text: "proposed before the feed gap" }],
        {
          recipients: [fixture.remote, n4.third, n4.fourth].map((member) =>
            agentViewOf(member.card),
          ),
        },
      ),
    );
    yield* n4.engine.drainOutbound.pipe(Effect.orDie);
    yield* takeActionProposalAfterEvidence(fixture.normalOutbound);
    const { outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "feed_gap",
      oldRouterInstanceId,
    );
    const requests = yield* Effect.replicateEffect(
      takeCatchUpRequest(outbound),
      2,
    );
    const n4Request = requests.find(
      (request) =>
        request.conversationId === n4.membership.descriptor.conversationId,
    );
    if (n4Request === undefined) {
      return yield* Effect.dieMessage("recovery did not ask the N4 members");
    }
    yield* deliverRecovery(
      n4.engine,
      catchUpPageIngressFrom({
        membership: n4.membership,
        responder: n4.fourth,
        request: n4Request,
        item: history.certifiedSuccessor,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const next = yield* takeCatchUpRequest(outbound);
    yield* settle;
    const sentWhileFenced = yield* Queue.size(fixture.normalOutbound);
    yield* Effect.forEach(
      [fixture.remote, n4.third],
      (responder) =>
        deliverRecovery(
          n4.engine,
          catchUpIncompleteIngressFrom({
            membership: n4.membership,
            responder,
            request: next,
            routerInstanceId: oldRouterInstanceId,
          }),
        ),
      { concurrency: 1, discard: true },
    );
    const proposal = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );

    expect(sentWhileFenced).toBe(0);
    expect(proposal.proposal.action.previousRecordHash).toBe(
      history.certifiedSuccessor.actionCertifiedRecord.recordHash,
    );
  });

/**
 * The direct conversation recovers on its member's first answer while the
 * N4 conversation's members stay silent, so the run keeps going. However long
 * the clock runs, every retry asks for the N4 conversation and none for the
 * recovered one. Fails when retries continue for a conversation that has
 * recovered.
 */
const stopsCatchUpRetriesOnceRecovered = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const { outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "feed_gap",
      oldRouterInstanceId,
      { clock: "caller" },
    );
    const requests = [
      yield* takeCatchUpRequest(outbound),
      yield* takeCatchUpRequest(outbound),
    ];
    const directRequest = requests.find(
      (request) =>
        request.conversationId === fixture.membership.descriptor.conversationId,
    );
    if (directRequest === undefined) {
      return yield* Effect.dieMessage("recovery did not ask the peer");
    }
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngress(fixture, directRequest),
    );
    const retried = yield* Effect.replicateEffect(
      takeRetriedCatchUpRequest(outbound),
      catchUpRetryAttempts,
    );
    yield* settle;

    expect(retried.map(({ conversationId }) => conversationId)).toStrictEqual(
      Array.from(
        { length: catchUpRetryAttempts },
        () => n4.membership.descriptor.conversationId,
      ),
    );
    expect(yield* Queue.size(outbound)).toBe(0);
  });

/**
 * A conversation's catch-up retries run out while its member is silent, and
 * the Router then reports another discontinuity. The next recovery run asks
 * the member again with a fresh request and retries on a fresh schedule, and
 * the member's answer recovers the conversation. Fails when a new run leaves
 * a conversation whose earlier retries ran out without a request.
 */
const rearmsCatchUpOnTheNextRecoveryRun = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
      { clock: "caller" },
    );
    yield* takeCatchUpRequest(outbound);
    yield* Effect.replicateEffect(
      takeRetriedCatchUpRequest(outbound),
      catchUpRetryAttempts,
    );
    yield* TestClock.adjust("100000 seconds");
    yield* settle;
    const quiet = yield* Queue.size(outbound);

    yield* forkRecovery(fixture, "feed_gap", oldRouterInstanceId, {
      clock: "caller",
    });
    const fresh = yield* takeCatchUpRequest(outbound);
    const retried = yield* takeRetriedCatchUpRequest(outbound);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: retried,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    yield* forkSend(
      fixture.engine,
      yield* authoredMessage(fixture, [
        { type: "text", text: "recovered on the next run" },
      ]),
    );
    const proposal = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );

    expect(quiet).toBe(0);
    expect(fresh.conversationId).toBe(
      fixture.membership.descriptor.conversationId,
    );
    expect(retried).toStrictEqual(fresh);
    expect(proposal.proposal.action.conversationId).toBe(
      fixture.membership.descriptor.conversationId,
    );
  });

/**
 * A conversation's catch-up retries run out while its member is silent, and
 * the owner then sends a message into it. The message arms catch-up again with a fresh
 * request and waits; once the member answers, the conversation recovers and
 * the message goes out. Fails when a message into a paused conversation waits for
 * a recovery nothing will start, or is dropped.
 */
const rearmsCatchUpOnALocalSend = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
      { clock: "caller" },
    );
    yield* takeCatchUpRequest(outbound);
    yield* Effect.replicateEffect(
      takeRetriedCatchUpRequest(outbound),
      catchUpRetryAttempts,
    );

    const sending = yield* forkSend(
      fixture.engine,
      yield* authoredMessage(fixture, [
        { type: "text", text: "sent into a paused conversation" },
      ]),
    );
    const fresh = yield* takeCatchUpRequest(outbound);
    yield* settle;
    const heldMessages = yield* Queue.size(fixture.normalOutbound);
    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request: fresh,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const proposal = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );

    expect(fresh.conversationId).toBe(
      fixture.membership.descriptor.conversationId,
    );
    expect(heldMessages).toBe(0);
    expect(proposal.proposal.action.conversationId).toBe(
      fixture.membership.descriptor.conversationId,
    );
    yield* Fiber.interrupt(sending);
  });

/**
 * Both of the endpoint's conversations exhaust their catch-up retries while
 * every member is silent, and the Router worker then reattaches after an
 * outage. Catch-up starts again for both paused conversations, even though
 * nobody sent. Fails when a paused conversation waits for traffic that may
 * never come.
 */
const rearmsEveryPausedConversationOnReattach = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const { outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "feed_gap",
      oldRouterInstanceId,
      { clock: "caller" },
    );
    yield* Effect.replicateEffect(takeCatchUpRequest(outbound), 2);
    yield* Effect.replicateEffect(
      takeRetriedCatchUpRequest(outbound),
      2 * catchUpRetryAttempts,
    );
    yield* settle;
    const quiet = yield* Queue.size(outbound);

    yield* n4.engine.rearmCatchUp;
    const rearmed = yield* Effect.replicateEffect(
      takeCatchUpRequest(outbound),
      2,
    );

    expect(quiet).toBe(0);
    expect(
      new Set(rearmed.map(({ conversationId }) => conversationId)),
    ).toStrictEqual(
      new Set([
        fixture.membership.descriptor.conversationId,
        n4.membership.descriptor.conversationId,
      ]),
    );
  });

/**
 * Latest the first proposal resend can fire: the catch-up retry base of one
 * second with its jitter, and earlier than the second resend can.
 */
const FIRST_RESEND_DUE = "1300 millis";

/**
 * Live time a trace waits for the resent envelopes once the clock passes
 * their delay; sealing and signing them settles on real promises.
 */
const RESEND_WAIT = "20 seconds";

/**
 * The author re-anchors at a Router restart, recovers, and sends while the
 * other member still catches up from the new anchor, so that member ignores
 * the proposal as traffic for a fenced conversation and never signs it. After
 * the catch-up retry delay the author sends its head's certified record and
 * the identical proposal again, in new outer messages; the member signs that
 * copy, and the message certifies. Fails when the author sends an unchanged
 * proposal only once, so the message waits for a signature that never comes.
 */
const resendsAProposalAMemberDroppedWhileRecovering = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    yield* reanchorUntilCompletionSend(fixture);
    const sending = yield* forkSend(
      fixture.engine,
      yield* authoredMessage(fixture, [
        { type: "text", text: "sent while the member recovers" },
      ]),
    );
    const first = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );
    const actionHash = yield* hashAction(first.proposal.action);
    yield* directPacketIngressFrom({
      membership: fixture.membership,
      sender: fixture.local,
      packet: first.proposal,
      routerInstanceId: newRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const authorEvidence = yield* Queue.take(fixture.normalOutbound).pipe(
      Effect.flatMap(decodeEvidenceKind),
    );

    yield* settle;
    yield* TestClock.adjust(FIRST_RESEND_DUE);
    const resent = yield* TestServices.provideLive(
      Queue.takeN(fixture.normalOutbound, 2).pipe(
        Effect.timeoutOption(RESEND_WAIT),
      ),
    ).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.dieMessage("the author did not send its proposal again"),
          onSome: (messages) => Effect.succeed(Chunk.toReadonlyArray(messages)),
        }),
      ),
    );
    const resentBodies = yield* Effect.forEach(
      resent,
      (message) => openForwarded(message),
      { concurrency: 1 },
    ).pipe(Effect.orDie);
    yield* peerEvidenceIngress(fixture, {
      sharnessVersion: WIRE_VERSION,
      kind: "action_signature",
      signerAgentId: fixture.remote.card.agentId,
      actionHash,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const staged = (yield* fixture.store.run(
      testStore.recover(),
    )).stagedRecords.find((record) => record.actionHash === actionHash);
    if (staged === undefined) {
      return yield* Effect.dieMessage("the author staged no record");
    }
    const recordHash = yield* Schema.decodeUnknown(RecordHash)(
      staged.recordHash,
    );
    yield* peerEvidenceIngress(fixture, {
      sharnessVersion: WIRE_VERSION,
      kind: "durability_vote",
      signerAgentId: fixture.remote.card.agentId,
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      recordHash,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    const sent = yield* Fiber.join(sending);

    expect(authorEvidence).toBe(actionSignatureKind);
    expect(resentBodies).toMatchObject([
      {
        kind: "direct",
        packet: {
          kind: "certified_record",
          actionCertifiedRecord: {
            recordHash: first.proposal.action.previousRecordHash,
          },
        },
      },
      { kind: "direct", packet: first.proposal },
    ]);
    expect(resent[1]?.envelopeId).not.toBe(first.message.envelopeId);
    expect(sent.recordHash).toBe(recordHash);
  });

/**
 * A proposal stays open through catch-up on the same Router instance. Time
 * spent fenced exceeds every resend delay without consuming its attempts,
 * so recovery without a re-anchor still releases an identical proposal.
 */
const preservesResendAttemptsWhileFenced = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const sending = yield* forkSend(
      fixture.engine,
      yield* authoredMessage(fixture, [
        { type: "text", text: "waiting through a long recovery" },
      ]),
    );
    const first = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );
    const { outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
    );
    const request = yield* takeCatchUpRequest(outbound);
    yield* Effect.replicateEffect(
      TestClock.adjust("5 minutes").pipe(Effect.zipRight(settle)),
      catchUpRetryAttempts + 1,
      { discard: true },
    );
    expect(yield* Queue.size(fixture.normalOutbound)).toBe(0);

    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngressFrom({
        membership: fixture.membership,
        responder: fixture.remote,
        request,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    yield* TestClock.adjust(FIRST_RESEND_DUE);
    const resent = yield* TestServices.provideLive(
      takeActionProposalAfterEvidence(fixture.normalOutbound).pipe(
        Effect.timeoutOption(RESEND_WAIT),
      ),
    );

    expect(Option.map(resent, ({ proposal }) => proposal)).toEqual(
      Option.some(first.proposal),
    );
    yield* Fiber.interrupt(sending);
  });

/** Each conversation retains its own catch-up position and readiness threshold. */
const recoversSeveralConversationsIndependently = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const n3 = yield* addGroupFoundation(fixture, 3);
    const n4 = yield* addN4Foundation(fixture);
    const { outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "feed_gap",
      oldRouterInstanceId,
    );
    const requests = yield* Effect.replicateEffect(
      takeCatchUpRequest(outbound),
      3,
    );
    const requestFor = (membership: VerifiedMembership) => {
      const request = requests.find(
        (candidate) =>
          candidate.conversationId === membership.descriptor.conversationId,
      );
      return request === undefined
        ? Effect.dieMessage("recovery omitted a conversation")
        : Effect.succeed(request);
    };
    const direct = yield* requestFor(fixture.membership);
    const group3 = yield* requestFor(n3.membership);
    const group4 = yield* requestFor(n4.membership);
    expect(direct.knownRecordHash).toBe(
      fixture.certifiedRecord.actionCertifiedRecord.recordHash,
    );
    expect([group3.knownRecordHash, group4.knownRecordHash]).toEqual([
      null,
      null,
    ]);

    yield* Effect.forEach(
      [fixture.remote, n4.third],
      (responder) =>
        deliverRecovery(
          n4.engine,
          catchUpIncompleteIngressFrom({
            membership: n4.membership,
            responder,
            request: group4,
            routerInstanceId: oldRouterInstanceId,
          }),
        ),
      { concurrency: 1, discard: true },
    );
    const sending = yield* forkSend(
      n4.engine,
      yield* authoredMessage(fixture, [
        { type: "text", text: "wait for this conversation" },
      ]),
    );
    expect(Option.isNone(yield* Fiber.poll(sending))).toBe(true);
    yield* deliverRecovery(
      n4.engine,
      catchUpIncompleteIngress(fixture, direct),
    );
    const proposal = yield* takeActionProposalIn(
      fixture.normalOutbound,
      direct.conversationId,
    );
    expect(proposal.proposal.action.previousRecordHash).toBe(
      direct.knownRecordHash,
    );

    yield* Effect.forEach(
      n3.others,
      (responder) =>
        deliverRecovery(
          n4.engine,
          catchUpIncompleteIngressFrom({
            membership: n3.membership,
            responder,
            request: group3,
            routerInstanceId: oldRouterInstanceId,
          }),
        ).pipe(
          Effect.tap((disposition) => {
            expect(disposition).toBe(acceptedDisposition);
          }),
        ),
      { concurrency: 1, discard: true },
    );
    const recovered = yield* fixture.store.run(testStore.recover());
    expect(
      recovered.positions.find(
        (position) => position.conversationId === direct.conversationId,
      )?.headRecordHash,
    ).toBe(direct.knownRecordHash);
    expect(
      recovered.positions
        .filter((position) => position.conversationId !== direct.conversationId)
        .map((position) => position.headRecordHash),
    ).toEqual([undefined, undefined]);
    yield* Fiber.interrupt(sending);
  });

/** A certified head delivered by the local Router echo must restart spent retries. */
const retriesAnAdvancedHeadAfterItsScheduleRanOut = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { outbound } = yield* forkRecovery(
      fixture,
      "feed_gap",
      oldRouterInstanceId,
      { clock: "caller" },
    );
    yield* takeCatchUpRequest(outbound);
    yield* Effect.replicateEffect(
      takeRetriedCatchUpRequest(outbound),
      catchUpRetryAttempts,
    );
    yield* TestClock.adjust("100000 seconds");
    const accepted = yield* deliverRecovery(
      fixture.engine,
      directPacketIngressFrom({
        membership: fixture.membership,
        sender: fixture.local,
        packet: fixture.certifiedRecord,
        routerInstanceId: oldRouterInstanceId,
      }),
    );
    const advanced = yield* takeCatchUpRequest(outbound);
    yield* TestClock.adjust(FIRST_RESEND_DUE);
    const retried = yield* TestServices.provideLive(
      takeCatchUpRequest(outbound).pipe(Effect.timeoutOption("5 seconds")),
    );
    expect(accepted).toBe(acceptedDisposition);
    expect(advanced.knownRecordHash).toBe(
      fixture.certifiedRecord.actionCertifiedRecord.recordHash,
    );
    expect(retried).toEqual(Option.some(advanced));
  });

// @agent-code-guard/regression-only: these traces pin the catch-up and re-anchor work a recovery run routes between its phases and its store.
describe("catch-up and re-anchor inside a recovery run", () => {
  it(
    "recovers several conversations independently at their own durable positions",
    recoversSeveralConversationsIndependently,
  );
  itOnTestClock(
    "retries an advanced certified head after its previous schedule ran out",
    retriesAnAdvancedHeadAfterItsScheduleRanOut,
  );
  itOnTestClock(
    "preserves proposal resend attempts through a long fence without a re-anchor",
    preservesResendAttemptsWhileFenced,
  );
  itOnTestClock(
    "sends its proposal again to a member that dropped it while recovering, and the message certifies",
    resendsAProposalAMemberDroppedWhileRecovering,
  );
  it(
    "sends a recovered conversation's message while another waits on a silent member, and the held message once that one recovers",
    recoversOneConversationWhileAnotherWaitsOnASilentMember,
  );
  itOnTestClock(
    "backs off catch-up retries exponentially and stops after the last attempt",
    backsOffCatchUpRetriesAndStops,
  );
  itOnTestClock(
    "arms catch-up again when a member's catch-up request arrives after its retries ran out",
    rearmsCatchUpAfterRetriesRunOut,
  );
  itOnTestClock(
    "arms catch-up again on the next recovery run after its retries ran out",
    rearmsCatchUpOnTheNextRecoveryRun,
  );
  itOnTestClock(
    "arms catch-up again on a local send after its retries ran out",
    rearmsCatchUpOnALocalSend,
  );
  itOnTestClock(
    "arms catch-up again for every paused conversation when the Router worker reattaches",
    rearmsEveryPausedConversationOnReattach,
  );
  itOnTestClock(
    "proposes a pending message only once its conversation recovers, at the head it settles on",
    proposesPendingMessagesOnlyOnceItsConversationRecovers,
  );
  itOnTestClock(
    "stops catch-up retries once the conversation recovers",
    stopsCatchUpRetriesOnceRecovered,
  );
  it(
    "answers a catch-up request with its first certified record",
    answersCatchUpRequestWithItsFirstCertifiedRecord,
  );
  it(
    "answers a catch-up request during its own recovery",
    answersCatchUpRequestDuringItsOwnRecovery,
  );
  it(
    "asks for catch-up again when a member votes for a head it lacks",
    requestsCatchUpForAVoteAtAnUnknownHead,
  );
  it(
    "adopts a re-anchor members completed while it was down",
    adoptsReanchorCompletedWhileItWasDown,
  );
  it(
    "ignores catch-up requests for another requester, another membership, or itself",
    ignoresCatchUpRequestsItMustNotAnswer,
  );
  it(
    "answers catch-up through a stored completed re-anchor",
    answersCatchUpThroughAStoredCompletedReanchor,
  );
});

/**
 * The local endpoint sends into its direct conversation, the Router orders
 * the proposal back to it and to `orderedAt`, the remote member signs it,
 * and the local endpoint stages the record and votes it durable. Nothing the
 * local endpoint sends after its proposal reaches the remote member.
 * @param fixture Endpoint that authors the message, holding a certified head.
 * @param orderedAt Other engines the Router orders the proposal to.
 * @returns The running send, and the record the author staged.
 */
const stageMessageAtTheAuthor = (
  fixture: RecoveryFixture,
  orderedAt: readonly EndpointEngine[] = [],
) =>
  Effect.gen(function* () {
    const sending = yield* forkSend(
      fixture.engine,
      yield* authoredMessage(fixture, [
        { type: "text", text: "staged before the restart" },
      ]),
    );
    yield* fixture.engine.drainOutbound.pipe(Effect.orDie);
    const proposal = yield* takeActionProposalAfterEvidence(
      fixture.normalOutbound,
    );
    yield* Effect.forEach(
      [fixture.engine, ...orderedAt],
      (engine) =>
        directPacketIngressFrom({
          membership: fixture.membership,
          sender: fixture.local,
          packet: proposal.proposal,
          routerInstanceId: oldRouterInstanceId,
        }).pipe(
          Effect.flatMap((ingress) => engine.acceptRouterIngress(ingress)),
        ),
      { concurrency: 1, discard: true },
    );
    const actionHash = yield* hashAction(proposal.proposal.action);
    yield* peerEvidenceIngressFrom({
      membership: fixture.membership,
      responder: fixture.remote,
      statement: {
        sharnessVersion: WIRE_VERSION,
        kind: "action_signature",
        signerAgentId: fixture.remote.card.agentId,
        actionHash,
      },
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* fixture.engine.drainOutbound.pipe(Effect.orDie);
    yield* Queue.takeAll(fixture.normalOutbound);
    const staged = (yield* fixture.store.run(
      testStore.recover(),
    )).stagedRecords.find((record) => record.actionHash === actionHash);
    if (staged === undefined) {
      return yield* Effect.dieMessage("the author staged no record");
    }
    return {
      sending,
      staged,
      recordHash: yield* Schema.decodeUnknown(RecordHash)(staged.recordHash),
    };
  }).pipe(Effect.orDie);

/**
 * Takes the `count` envelopes one step sends, then any further ones until the
 * queue has been quiet for `QUIET_PERIOD` of live time, and decodes each one's
 * body. Only the check that nothing more follows reads by a pause: sealing an
 * envelope settles on real promises, so under load a late extra envelope can
 * only pass it.
 * @param outbound Queue the endpoint sends to.
 * @param count Envelopes the step must send; zero when it must send none.
 * @returns The decoded bodies, in send order.
 */
const takeSentBodies = (outbound: Queue.Queue<SignedMessage>, count: number) =>
  Queue.takeN(outbound, count).pipe(
    Effect.flatMap((expected) =>
      takeUntilQuiet(outbound, Chunk.toReadonlyArray(expected)),
    ),
    Effect.flatMap((messages) =>
      Effect.forEach(messages, (message) => openForwarded(message), {
        concurrency: 1,
      }),
    ),
    Effect.orDie,
  );

/** Live time with no new envelope after which `takeSentBodies` stops reading. */
const QUIET_PERIOD = "200 millis";

function takeUntilQuiet(
  outbound: Queue.Queue<SignedMessage>,
  taken: readonly SignedMessage[],
): Effect.Effect<readonly SignedMessage[]> {
  return TestServices.provideLive(
    Queue.take(outbound).pipe(Effect.timeoutOption(QUIET_PERIOD)),
  ).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.succeed(taken),
        onSome: (message) => takeUntilQuiet(outbound, [...taken, message]),
      }),
    ),
  );
}

/**
 * Both members of a direct conversation staged the local endpoint's message
 * and voted it durable, and each lost the other's vote when the Router
 * restarted. During recovery the local endpoint answers the member's
 * catch-up with the record and its own vote, not `incomplete`, and the
 * member's vote then certifies the message and moves its catch-up to the new
 * head. Fails when a holder answers `incomplete`, or when recovery ignores a
 * durability vote for a staged successor of its head.
 * @returns The trace, run to completion.
 */
const certifiesAStagedSuccessorOnAVoteAMemberSendsAgain = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const head = fixture.certifiedRecord.actionCertifiedRecord.recordHash;
    const anchor =
      fixture.certifiedRecord.actionCertifiedRecord.recordCore.anchorHash;
    const { recordHash } = yield* stageMessageAtTheAuthor(fixture);
    const { outbound } = yield* forkRecovery(
      {
        engine: fixture.engine,
        recoveryOutbound: fixture.recoveryOutbound,
      },
      "router_restarted",
      newRouterInstanceId,
    );
    yield* takeCatchUpRequest(outbound);

    yield* deliverRecovery(
      fixture.engine,
      directPacketIngressFrom({
        membership: fixture.membership,
        sender: fixture.remote,
        packet: peerCatchUpRequest(fixture, {
          knownRecordHash: head,
          knownAnchorHash: anchor,
        }),
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const answered = yield* takeSentBodies(fixture.normalOutbound, 2);
    const recoveryAnswers = yield* takeSentBodies(outbound, 0);
    const certified = yield* deliverRecovery(
      fixture.engine,
      peerEvidenceIngress(fixture, {
        sharnessVersion: WIRE_VERSION,
        kind: "durability_vote",
        signerAgentId: fixture.remote.card.agentId,
        conversationId: fixture.membership.descriptor.conversationId,
        membershipHash: fixture.membership.hash,
        recordHash,
      }),
    );
    const next = yield* takeCatchUpRequest(outbound);

    expect(answered).toMatchObject([
      {
        kind: "direct",
        packet: { kind: "action_certified_record", recordHash },
      },
      { kind: "evidence" },
    ]);
    expect(recoveryAnswers).toEqual([]);
    expect(certified).toBe(acceptedDisposition);
    expect(
      (yield* fixture.store.run(testStore.recover())).positions[0]
        ?.headRecordHash,
    ).toBe(recordHash);
    expect(next.knownRecordHash).toBe(recordHash);
  });

/**
 * The local endpoint holds its message staged when the Router restarts, and the
 * member answers its catch-up as `incomplete`, which makes its position
 * ready. A holder votes for no re-anchor behind its staged successor: it
 * sends the record and its durability vote again instead. Fails when a
 * holder stages a re-anchor candidate at the head its successor extends, or
 * waits without sending the successor members need to certify it.
 * @returns The trace, run to completion.
 */
const resendsItsStagedSuccessorInsteadOfVotingAtItsHead = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const { recordHash } = yield* stageMessageAtTheAuthor(fixture);
    const { outbound, request } = yield* startRestartRecovery(fixture);

    yield* deliverRecovery(
      fixture.engine,
      catchUpIncompleteIngress(fixture, request),
    );
    const resent = yield* takeSentBodies(fixture.normalOutbound, 2);
    const recoveryTraffic = yield* takeSentBodies(outbound, 0);

    expect(resent).toMatchObject([
      {
        kind: "direct",
        packet: { kind: "action_certified_record", recordHash },
      },
      { kind: "evidence" },
    ]);
    expect(recoveryTraffic).toEqual([]);
    expect(
      (yield* fixture.store.run(testStore.recover())).stagedReanchors,
    ).toEqual([]);
  });

/**
 * The local endpoint authors a message in its direct conversation, both members
 * sign it, and the author stages the record and votes it durable; the Router
 * restarts before the member receives the record. Both members recover: the
 * author answers the member's catch-up with the record and its vote instead
 * of `incomplete`, the member converts to it on that one vote, both certify
 * it, and both re-anchor at it. The message completes, and neither member stages
 * a re-anchor candidate at the head the record extends. Fails when the
 * author's `incomplete` makes the member ready at that head, or when recovery
 * ignores the record and its votes, so the conversation never recovers.
 * @returns The trace, run to completion.
 */
const certifiesAMessageOneDirectMemberStagedBeforeTheRestart = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    yield* retainCertifiedRecord(fixture);
    const peer = yield* openPeerEngine(fixture);
    const head = fixture.certifiedRecord.actionCertifiedRecord.recordHash;
    const oldAnchor =
      fixture.certifiedRecord.actionCertifiedRecord.recordCore.anchorHash;
    const { sending, staged } = yield* stageMessageAtTheAuthor(fixture, [
      peer.engine,
    ]);
    yield* Queue.takeAll(fixture.normalOutbound);
    yield* Queue.takeAll(peer.normalOutbound);

    yield* forkRecovery(
      {
        engine: fixture.engine,
        recoveryOutbound: fixture.recoveryOutbound,
      },
      "router_restarted",
      newRouterInstanceId,
    );
    yield* forkRecovery(peer, "router_restarted", newRouterInstanceId);
    yield* Effect.forEach(
      [
        [fixture.recoveryOutbound, peer.engine, fixture.local, fixture.remote],
        [fixture.normalOutbound, peer.engine, fixture.local, fixture.remote],
        [peer.recoveryOutbound, fixture.engine, fixture.remote, fixture.local],
        [peer.normalOutbound, fixture.engine, fixture.remote, fixture.local],
      ] as const,
      ([sent, engine, sender, receiver]) =>
        relayRecoveryTraffic(sent, engine, { sender, receiver }),
      { concurrency: 1, discard: true },
    );
    const localPositions = yield* awaitReanchoredFrom(fixture.store, oldAnchor);
    const peerPositions = yield* awaitReanchoredFrom(peer.store, oldAnchor);
    const sent = yield* Fiber.join(sending).pipe(Effect.orDie);
    const candidatesAtHead = (recovery: EndpointRecovery) =>
      recovery.stagedReanchors.filter(
        (candidate) => candidate.selectedRecordHash === head,
      );

    expect(localPositions[0]?.headRecordHash).toBe(staged.recordHash);
    expect(peerPositions).toStrictEqual(localPositions);
    expect(sent.recordHash).toBe(staged.recordHash);
    expect(
      candidatesAtHead(yield* fixture.store.run(testStore.recover())),
    ).toEqual([]);
    expect(
      candidatesAtHead(yield* peer.store.run(testStore.recover())),
    ).toEqual([]);
  });

/**
 * Delivers a group's certified head to its engine, starts a recovery of the
 * engine, and takes the group's first catch-up request; the direct
 * conversation's request is taken and left unanswered.
 * @param fixture Endpoint holding both conversations.
 * @param group The group conversation.
 * @param head The group's certified head.
 * @param recovery The recovery to start.
 * @param recovery.reason The discontinuity the recovery follows.
 * @param recovery.routerInstanceId The Router instance it recovers at, by
 *     default the new one after a Router restart and the old one otherwise.
 * @param recovery.clock How the recovery's clock runs, as in
 *     {@link forkRecovery}.
 * @returns The run's queue and the group's catch-up request.
 */
const recoverGroup = (
  fixture: RecoveryFixture,
  group: Pick<GroupFoundation, "engine" | "membership">,
  head: CertifiedRecord,
  recovery: {
    readonly reason?: RouterDiscontinuityReason;
    readonly routerInstanceId?: typeof RouterInstanceId.Type;
    readonly clock?: "held" | "caller";
  } = {},
) =>
  Effect.gen(function* () {
    const reason = recovery.reason ?? "router_restarted";
    yield* directPacketIngressFrom({
      membership: group.membership,
      sender: fixture.remote,
      packet: head,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => group.engine.acceptRouterIngress(ingress)),
    );
    const { outbound } = yield* forkRecovery(
      { engine: group.engine, recoveryOutbound: fixture.recoveryOutbound },
      reason,
      recovery.routerInstanceId ??
        (reason === "router_restarted"
          ? newRouterInstanceId
          : oldRouterInstanceId),
      { clock: recovery.clock ?? "held" },
    );
    const { group: request } = yield* takeN4Requests(fixture, group, outbound);
    return { outbound, request };
  });

/**
 * Delivers a member's action-certified record, or durability votes for it,
 * to a group's engine during its recovery.
 * @param group The group conversation.
 * @param routerInstanceId The Router instance the deliveries come through.
 * @returns The two deliveries.
 */
const groupSuccessorTraffic = (
  group: Pick<GroupFoundation, "engine" | "membership">,
  routerInstanceId: typeof RouterInstanceId.Type = newRouterInstanceId,
) => ({
  record: (sender: SigningIdentity, record: ActionCertifiedRecordValue) =>
    deliverRecovery(
      group.engine,
      directPacketIngressFrom({
        membership: group.membership,
        sender,
        packet: record,
        routerInstanceId,
      }),
    ),
  votes: (
    recordHash: ActionCertifiedRecordValue["recordHash"],
    voters: readonly SigningIdentity[],
  ) =>
    Effect.forEach(
      voters,
      (voter) =>
        deliverRecovery(
          group.engine,
          peerEvidenceIngressFrom({
            membership: group.membership,
            responder: voter,
            statement: {
              sharnessVersion: WIRE_VERSION,
              kind: "durability_vote",
              signerAgentId: voter.card.agentId,
              conversationId: group.membership.descriptor.conversationId,
              membershipHash: group.membership.hash,
              recordHash,
            },
            routerInstanceId,
          }),
        ),
      { concurrency: 1 },
    ),
});

/**
 * What the local store holds for a group after recovery traffic: whether it
 * staged the successor, the group's head, and the heads its re-anchor
 * candidates select.
 * @param fixture Endpoint whose store holds the group.
 * @param group The group conversation.
 * @param recordHash The successor.
 * @returns The staged flag, the head, and the candidates' selected heads.
 */
const groupState = (
  fixture: RecoveryFixture,
  group: Pick<GroupFoundation, "membership">,
  recordHash: string,
) =>
  fixture.store.run(testStore.recover()).pipe(
    Effect.map((recovery) => {
      const conversationId = group.membership.descriptor.conversationId;
      return {
        staged: recovery.stagedRecords.some(
          (record) => record.recordHash === recordHash,
        ),
        head: recovery.positions.find(
          (position) => position.conversationId === conversationId,
        )?.headRecordHash,
        candidates: recovery.stagedReanchors
          .filter((candidate) => candidate.conversationId === conversationId)
          .map((candidate) => candidate.selectedRecordHash),
      };
    }),
    Effect.orDie,
  );

/**
 * In a three-member conversation one member staged the remote member's message
 * and voted it durable before the Router restarted. The local endpoint
 * converts on that one vote, since a three-member conversation can have no
 * faulty member, stages the message and votes for it, and the third member's
 * converted vote then certifies it and moves the catch-up to it. Fails when a
 * re-anchoring member never converts, so a lone holder blocks the
 * conversation.
 * @returns The trace, run to completion.
 */
const convertsToAThreeMemberSuccessorOnOneVote = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const group = yield* addGroupFoundation(fixture, 3);
    const history = yield* buildGroupHistory(fixture, group);
    const { recordHash } = history.successor;
    const [holder, third] = group.others;
    if (holder === undefined || third === undefined) {
      return yield* Effect.dieMessage("the group lacks members");
    }
    const { outbound } = yield* recoverGroup(
      fixture,
      group,
      history.certifiedHead,
    );
    const traffic = groupSuccessorTraffic(group);

    yield* traffic.record(holder, history.successor);
    yield* traffic.votes(recordHash, [holder]);
    const converted = yield* groupState(fixture, group, recordHash);
    const ownCopyAndVote = yield* takeSentBodies(fixture.normalOutbound, 2);
    yield* traffic.votes(recordHash, [third]);
    const next = yield* takeCatchUpRequest(outbound);
    const certified = yield* groupState(fixture, group, recordHash);

    expect(converted.staged).toBe(true);
    expect(ownCopyAndVote).toMatchObject([
      { kind: "direct", packet: { kind: "action_certified_record" } },
      { kind: "evidence" },
    ]);
    expect(certified.head).toBe(recordHash);
    expect(next.knownRecordHash).toBe(recordHash);
    expect(certified.candidates).toEqual([]);
  });

/**
 * In the N4 conversation only the message's author staged it before the Router
 * restarted, which `n − q(n)` members can be. Its record and one vote do not
 * make the local endpoint convert, and the other two members' `incomplete`
 * answers make its position ready at the head: it votes to re-anchor there
 * without the author, and stages nothing for the message. Fails when a member
 * converts on no more votes than the members that can be faulty.
 * @returns The trace, run to completion.
 */
const votesAtTheHeadOverASuccessorOnlyItsAuthorStaged = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordHash } = history.stagedSuccessor;
    const head = history.certifiedHead.actionCertifiedRecord.recordHash;
    const { request } = yield* recoverGroup(fixture, n4, history.certifiedHead);
    const traffic = groupSuccessorTraffic(n4);

    yield* traffic.record(n4.fourth, history.stagedSuccessor);
    yield* traffic.votes(recordHash, [n4.fourth]);
    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [fixture.remote, n4.third],
    });
    const state = yield* groupState(fixture, n4, recordHash);

    expect(state.staged).toBe(false);
    expect(state.candidates).toEqual([head]);
  });

/**
 * In the N4 conversation two members staged the message and voted it durable
 * before the Router restarted, more than can be faulty, and a faulty member
 * answers the local endpoint's catch-up as `incomplete`. One answer does not
 * make the position ready; the two holders' votes make the endpoint convert,
 * and its own vote completes the message's durability certificate. With an
 * earlier re-anchor candidate at the head, the store refuses the record
 * instead. Fails when a member converts after voting to leave the anchor, or
 * when two holders' votes do not move a member to their successor.
 * @param earlier Whether the endpoint staged a candidate at the head first.
 * @returns The trace, run to completion.
 */
const convertsOnTwoN4HoldersUnlessItLeftTheAnchor = (
  earlier: "none" | "candidate",
) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordHash } = history.stagedSuccessor;
    const head = history.certifiedHead.actionCertifiedRecord.recordHash;
    const { request } = yield* recoverGroup(fixture, n4, history.certifiedHead);
    const traffic = groupSuccessorTraffic(n4);

    yield* answerN4Incomplete(fixture, n4, request, {
      responders:
        earlier === "candidate" ? [fixture.remote, n4.third] : [fixture.remote],
    });
    yield* traffic.record(n4.fourth, history.stagedSuccessor);
    yield* traffic.votes(recordHash, [n4.third, n4.fourth]);
    const state = yield* groupState(fixture, n4, recordHash);

    expect(state).toStrictEqual(
      earlier === "candidate"
        ? { staged: false, head, candidates: [head] }
        : { staged: true, head: recordHash, candidates: [] },
    );
  });

/**
 * In a seven-member conversation three members staged the message and voted it
 * durable before the Router restarted, and two others voted to re-anchor at
 * the head. Two holders' votes, as many as can be faulty, do not make the
 * local endpoint convert; the third does. With its vote and the last
 * member's, five votes certify the message. Fails when a member converts on as
 * many votes as can be faulty, or never converts above that.
 * @returns The trace, run to completion.
 */
const convertsToASevenMemberSuccessorAboveTheFaultBound = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const group = yield* addGroupFoundation(fixture, 7);
    const history = yield* buildGroupHistory(fixture, group);
    const { recordHash } = history.successor;
    const [first, second, third, , , last] = group.others;
    if (
      first === undefined ||
      second === undefined ||
      third === undefined ||
      last === undefined
    ) {
      return yield* Effect.dieMessage("the group lacks members");
    }
    yield* recoverGroup(fixture, group, history.certifiedHead);
    const traffic = groupSuccessorTraffic(group);

    yield* traffic.record(first, history.successor);
    yield* traffic.votes(recordHash, [first, second]);
    const onTwoVotes = yield* groupState(fixture, group, recordHash);
    yield* traffic.votes(recordHash, [third]);
    const onThreeVotes = yield* groupState(fixture, group, recordHash);
    yield* traffic.votes(recordHash, [last]);
    const certified = yield* groupState(fixture, group, recordHash);

    expect(onTwoVotes.staged).toBe(false);
    expect(onThreeVotes.staged).toBe(true);
    expect(onThreeVotes.head).not.toBe(recordHash);
    expect(certified.head).toBe(recordHash);
  });

/**
 * After a feed gap, the N4 conversation's successor reaches the local
 * endpoint while it recovers. No re-anchor is running, so the record alone
 * is enough: the endpoint stages it and votes for it at once. Fails when
 * recovery ignores an action-certified record that extends its head.
 * @returns The trace, run to completion.
 */
const stagesASuccessorOnItsRecordAloneAfterAFeedGap = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordHash } = history.stagedSuccessor;
    yield* recoverGroup(fixture, n4, history.certifiedHead, {
      reason: "feed_gap",
    });

    yield* groupSuccessorTraffic(n4, oldRouterInstanceId).record(
      n4.fourth,
      history.stagedSuccessor,
    );
    const state = yield* groupState(fixture, n4, recordHash);
    const ownCopyAndVote = yield* takeSentBodies(fixture.normalOutbound, 2);

    expect(state.staged).toBe(true);
    expect(ownCopyAndVote).toMatchObject([
      { kind: "direct", packet: { kind: "action_certified_record" } },
      { kind: "evidence" },
    ]);
  });

/**
 * The local endpoint staged an N4 member's message and voted it durable before
 * the Router restarted, but holds no certificate for it. A member's catch-up
 * request at the head gets that record and its vote, not `incomplete`, so no
 * member can count it among the answers that settle the position at the head
 * while another member holds the message certified. Fails when a holder of a
 * staged successor answers `incomplete` during a re-anchor.
 * @returns The trace, run to completion.
 */
const answersWithTheSuccessorItStagedInsteadOfIncomplete = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordHash } = history.stagedSuccessor;
    const head = history.certifiedHead.actionCertifiedRecord;
    yield* Effect.forEach(
      [history.certifiedHead, history.stagedSuccessor],
      (packet) =>
        directPacketIngressFrom({
          membership: n4.membership,
          sender: n4.fourth,
          packet,
          routerInstanceId: oldRouterInstanceId,
        }).pipe(
          Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
        ),
      { concurrency: 1, discard: true },
    );
    yield* n4.engine.drainOutbound.pipe(Effect.orDie);
    yield* Queue.takeAll(fixture.normalOutbound);
    const { outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      newRouterInstanceId,
    );
    yield* Effect.replicateEffect(takeCatchUpRequest(outbound), 2);

    yield* deliverRecovery(
      n4.engine,
      directPacketIngressFrom({
        membership: n4.membership,
        sender: fixture.remote,
        packet: peerCatchUpRequest(
          fixture,
          {
            knownRecordHash: head.recordHash,
            knownAnchorHash: head.recordCore.anchorHash,
          },
          n4.membership,
        ),
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const answered = yield* takeSentBodies(fixture.normalOutbound, 2);
    const recoveryAnswers = yield* takeSentBodies(outbound, 0);

    expect(answered).toMatchObject([
      {
        kind: "direct",
        packet: { kind: "action_certified_record", recordHash },
      },
      { kind: "evidence" },
    ]);
    expect(recoveryAnswers).toEqual([]);
  });

/**
 * Opens the fourth N4 member as a second endpoint with its own store, holding
 * the N4 conversation's certified head and the staged successor it votes
 * durable, as a member holds a record whose certificate a Router restart cut
 * off.
 * @param fixture Endpoint whose N4 conversation the member shares.
 * @param n4 The N4 conversation and its members.
 * @param history The N4 head and its staged successor.
 * @returns The member's engine, store, and the queues it sends to.
 */
const openN4Holder = (
  fixture: RecoveryFixture,
  n4: N4Foundation,
  history: N4PartialHistory,
) =>
  Effect.gen(function* () {
    const store = yield* openFirstMessageStore(
      n4.membership,
      history.certifiedHead,
      "sharness-recovery-holder-",
    );
    const recoveryOutbound = yield* Queue.unbounded<SignedMessage>();
    const normalOutbound = yield* Queue.unbounded<SignedMessage>();
    yield* store.run(saveLocalIdentity(n4.fourth.card));
    const engine = yield* makeEndpointEngine({
      ...fixture.input,
      localAgentCard: n4.fourth.card,
      signingAuthority: n4.fourth.authority,
      routerWorker: makeFixtureRouter({
        store,
        local: n4.fourth,
        normalOutbound,
        recoveryOutbound,
      }),
    }).pipe(Effect.provideService(SqlClient.SqlClient, store.sql));
    yield* Effect.forEach(
      [history.certifiedHead, history.stagedSuccessor],
      (packet) =>
        directPacketIngressFrom({
          membership: n4.membership,
          sender: fixture.remote,
          packet,
          routerInstanceId: oldRouterInstanceId,
        }).pipe(
          Effect.flatMap((ingress) => engine.acceptRouterIngress(ingress)),
        ),
      { concurrency: 1, discard: true },
    );
    yield* engine.drainOutbound;
    yield* Queue.takeAll(normalOutbound);
    return { engine, store, recoveryOutbound, normalOutbound };
  }).pipe(Effect.provide(NodeFileSystem.layer), Effect.orDie);

/**
 * Delivers the local endpoint's N4 catch-up request to another member's
 * engine, as the Router does.
 * @param fixture Endpoint whose request it is.
 * @param n4 The N4 conversation.
 * @param request The local endpoint's catch-up request.
 * @param to The member's engine, and the Router instance that delivers it.
 * @param to.engine The member's engine.
 * @param to.routerInstanceId The Router instance that delivers it.
 * @returns How the member's engine disposed of it.
 */
const deliverRequestTo = (
  fixture: RecoveryFixture,
  n4: N4Foundation,
  request: CatchUpRequest,
  to: {
    readonly engine: EndpointEngine;
    readonly routerInstanceId: typeof RouterInstanceId.Type;
  },
) =>
  directPacketIngressFrom({
    membership: n4.membership,
    sender: fixture.local,
    packet: request,
    routerInstanceId: to.routerInstanceId,
  }).pipe(Effect.flatMap((ingress) => to.engine.acceptRouterIngress(ingress)));

/**
 * A Router restart cuts off a successor's certificate. The fourth N4 member
 * holds the successor of the head staged, the remote member holds it
 * certified but is slow, and the faulty third member answers `incomplete`. The holder answers
 * the local endpoint's catch-up with the record and its vote, so one
 * `incomplete` is all the endpoint counts: it stages no re-anchor candidate
 * at the head, and the remote member's late page moves it to the successor.
 * Fails when a holder answers `incomplete`, so the endpoint settles at the
 * head behind a certified successor.
 * @returns The trace, run to completion.
 */
const waitsBehindASuccessorAHolderAnswersWith = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordHash } = history.stagedSuccessor;
    const holder = yield* openN4Holder(fixture, n4, history);
    const { request } = yield* recoverGroup(fixture, n4, history.certifiedHead);
    yield* forkRecovery(holder, "router_restarted", newRouterInstanceId);
    const holderAnswers = yield* relayRecoveryTraffic(
      holder.normalOutbound,
      n4.engine,
      { sender: n4.fourth, receiver: fixture.local },
    );
    yield* relayRecoveryTraffic(holder.recoveryOutbound, n4.engine, {
      sender: n4.fourth,
      receiver: fixture.local,
    });

    yield* deliverRequestTo(fixture, n4, request, {
      engine: holder.engine,
      routerInstanceId: newRouterInstanceId,
    });
    yield* Queue.takeN(holderAnswers, 2);
    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [n4.third],
    });
    const beforeThePage = yield* groupState(fixture, n4, recordHash);
    yield* deliverRecovery(
      n4.engine,
      catchUpPageIngressFrom({
        membership: n4.membership,
        responder: fixture.remote,
        request,
        item: history.certifiedSuccessor,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const afterThePage = yield* groupState(fixture, n4, recordHash);

    expect(beforeThePage.candidates).toEqual([]);
    expect(afterThePage.head).toBe(recordHash);
    expect(afterThePage.candidates).toEqual([]);
  });

/**
 * A feed gap with a silent certified holder. The fourth N4 member, not
 * recovering, holds the successor of the head staged; the remote member
 * holds it certified and stays silent past the retry window; the faulty
 * third member answers `incomplete`. The holder answers with the record and
 * its vote, so the local endpoint stages the record on it alone and never
 * settles at the head: a message into the conversation stays held after the
 * retries run out, and the remote member's vote then certifies the
 * successor. The recovery's two conversations, the direct one with the silent
 * remote member and the N4 group, each retry until their attempts run out, so
 * the trace takes `2 * catchUpRetryAttempts` retried requests. Fails when a
 * responder outside recovery answers `incomplete` over its staged successor.
 * @returns The trace, run to completion.
 */
const neverSettlesBehindASuccessorAfterAFeedGap = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordHash } = history.stagedSuccessor;
    const holder = yield* openN4Holder(fixture, n4, history);
    const { outbound, request } = yield* recoverGroup(
      fixture,
      n4,
      history.certifiedHead,
      { reason: "feed_gap", clock: "caller" },
    );
    const holderAnswers = yield* relayRecoveryTraffic(
      holder.normalOutbound,
      n4.engine,
      { sender: n4.fourth, receiver: fixture.local },
      oldRouterInstanceId,
    );

    yield* deliverRequestTo(fixture, n4, request, {
      engine: holder.engine,
      routerInstanceId: oldRouterInstanceId,
    });
    yield* holder.engine.drainOutbound.pipe(Effect.orDie);
    yield* Queue.takeN(holderAnswers, 2);
    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [n4.third],
      routerInstanceId: oldRouterInstanceId,
    });
    const sending = yield* forkSend(
      n4.engine,
      yield* authoredMessage(
        fixture,
        [{ type: "text", text: "held while the certified holder is silent" }],
        {
          recipients: [fixture.remote, n4.third, n4.fourth].map((member) =>
            agentViewOf(member.card),
          ),
        },
      ),
    );
    yield* Effect.replicateEffect(
      takeRetriedCatchUpRequest(outbound),
      2 * catchUpRetryAttempts,
    );
    const settledAtTheHead = yield* TestServices.provideLive(
      takeActionProposalAfterEvidence(fixture.normalOutbound).pipe(
        Effect.timeoutOption("300 millis"),
      ),
    );
    const staged = yield* groupState(fixture, n4, recordHash);
    yield* groupSuccessorTraffic(n4, oldRouterInstanceId).votes(recordHash, [
      fixture.remote,
    ]);
    const certified = yield* groupState(fixture, n4, recordHash);

    expect(Option.isNone(settledAtTheHead)).toBe(true);
    expect(staged.staged).toBe(true);
    expect(certified.head).toBe(recordHash);
    yield* Fiber.interrupt(sending);
  });

/**
 * After one Router restart with nothing in flight and one N4 member offline,
 * the two other members' `incomplete` answers make the local endpoint's
 * position ready at once, with no wait for the retries, and it votes to
 * re-anchor at the head. Fails when catch-up waits for every member where no
 * earlier re-anchor vote calls for it.
 * @returns The trace, run to completion.
 */
const settlesOnAQuorumWithNoEarlierReanchor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const head = history.certifiedHead.actionCertifiedRecord.recordHash;
    const { request } = yield* recoverGroup(fixture, n4, history.certifiedHead);

    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [fixture.remote, n4.third],
    });
    const state = yield* groupState(
      fixture,
      n4,
      history.stagedSuccessor.recordHash,
    );

    expect(state.candidates).toEqual([head]);
  });

/**
 * The two-restart trace. A re-anchor from the N4 head completed for the
 * first restarted Router instance at the fourth member only, and the Router
 * restarts again. The local endpoint holds a vote for that earlier
 * re-anchor, its own or one an `incomplete` answer carries, so the remote
 * and third members' `incomplete` answers do not settle its position: it
 * waits for the fourth member, whose page carries the completed re-anchor,
 * and adopts it instead of voting for a re-anchor at the later instance.
 * Fails when a quorum settles the position behind an earlier instance's
 * re-anchor, so members end on different anchors.
 * @param earlierVote Whose earlier-instance vote the endpoint holds.
 * @returns The trace, run to completion.
 */
const waitsBehindAnEarlierInstanceReanchor = (earlierVote: "own" | "member") =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordCore, recordHash } =
      history.certifiedHead.actionCertifiedRecord;
    const earlierBody: ReanchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_body",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      previousAnchorHash: recordCore.anchorHash,
      selectedRecordHash: recordHash,
      routerInstanceId: newRouterInstanceId,
    };
    if (earlierVote === "own") {
      const first = yield* recoverGroup(fixture, n4, history.certifiedHead);
      yield* answerN4Incomplete(fixture, n4, first.request, {
        responders: [fixture.remote, n4.third],
      });
      yield* takeReanchorVote(fixture.recoveryOutbound);
    } else {
      yield* directPacketIngressFrom({
        membership: n4.membership,
        sender: fixture.remote,
        packet: history.certifiedHead,
        routerInstanceId: oldRouterInstanceId,
      }).pipe(
        Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
      );
    }
    const { outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      laterRouterInstanceId,
    );
    const { group: request } = yield* takeN4Requests(fixture, n4, outbound);
    if (earlierVote === "member") {
      yield* deliverRecovery(
        n4.engine,
        peerEvidenceIngressFrom({
          membership: n4.membership,
          responder: n4.third,
          statement: {
            sharnessVersion: WIRE_VERSION,
            kind: "reanchor_vote",
            signerAgentId: n4.third.card.agentId,
            anchorHash: yield* hashAnchor(earlierBody),
            reanchor: earlierBody,
          },
          routerInstanceId: laterRouterInstanceId,
        }),
      );
    }
    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [n4.third, fixture.remote],
      routerInstanceId: laterRouterInstanceId,
    });
    const laterCandidates = (yield* groupState(fixture, n4, recordHash))
      .candidates;
    const completed = yield* completedReanchorBy(earlierBody, [
      fixture.remote,
      n4.third,
      n4.fourth,
    ]);
    const adopted = yield* deliverRecovery(
      n4.engine,
      catchUpPageIngressFrom({
        membership: n4.membership,
        responder: n4.fourth,
        request,
        item: completed,
        routerInstanceId: laterRouterInstanceId,
      }),
    );
    const anchor = (yield* fixture.store.run(
      testStore.recover(),
    )).positions.find(
      (position) =>
        position.conversationId === n4.membership.descriptor.conversationId,
    )?.currentAnchorHash;

    expect(laterCandidates).toEqual(earlierVote === "own" ? [recordHash] : []);
    expect(adopted).toBe(acceptedDisposition);
    expect(anchor).toBe(completed.anchorHash);
  });

/**
 * The local endpoint voted to re-anchor the N4 conversation at its head for
 * the first restarted Router instance, and the Router restarts again before
 * that re-anchor completes. A member's catch-up request at the head gets the
 * endpoint's earlier-instance vote ahead of its `incomplete` answer, so the
 * member waits for every member, as a member that completed the earlier
 * re-anchor may not have answered yet. Fails when an `incomplete` answer
 * leaves out the responder's earlier-instance re-anchor vote.
 * @returns The trace, run to completion.
 */
const answersIncompleteWithItsEarlierInstanceVote = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const head = history.certifiedHead.actionCertifiedRecord;
    const first = yield* recoverGroup(fixture, n4, history.certifiedHead);
    yield* answerN4Incomplete(fixture, n4, first.request, {
      responders: [fixture.remote, n4.third],
    });
    yield* takeReanchorVote(fixture.recoveryOutbound);
    const { outbound } = yield* forkRecovery(
      { engine: n4.engine, recoveryOutbound: fixture.recoveryOutbound },
      "router_restarted",
      laterRouterInstanceId,
    );
    yield* takeN4Requests(fixture, n4, outbound);

    yield* deliverRecovery(
      n4.engine,
      directPacketIngressFrom({
        membership: n4.membership,
        sender: fixture.remote,
        packet: peerCatchUpRequest(
          fixture,
          {
            knownRecordHash: head.recordHash,
            knownAnchorHash: head.recordCore.anchorHash,
          },
          n4.membership,
        ),
        routerInstanceId: laterRouterInstanceId,
      }),
    );
    const answered = yield* takeSentBodies(outbound, 2);
    const statements = yield* Effect.forEach(
      answered.filter((body) => body.kind === "evidence"),
      (body) => decodeCanonical(EvidenceStatement, body.message.body),
      { concurrency: 1 },
    ).pipe(Effect.orDie);

    expect(answered).toMatchObject([
      { kind: "evidence" },
      { kind: "direct", packet: { kind: "catch_up_incomplete" } },
    ]);
    expect(statements).toMatchObject([
      {
        kind: "reanchor_vote",
        signerAgentId: fixture.local.card.agentId,
        reanchor: {
          selectedRecordHash: head.recordHash,
          routerInstanceId: newRouterInstanceId,
        },
      },
    ]);
  });

/**
 * After a second Router restart, a member's `incomplete` answer carries its
 * vote to re-anchor the N4 conversation at the head for the first restarted
 * instance, and the fourth member stays silent. The remote and third
 * members' answers do not settle the position while the catch-up retries
 * last; once they run out, those answers settle it and the local endpoint
 * votes to re-anchor at the head for the later instance. Fails when a
 * position waiting behind an earlier-instance vote never settles.
 * @returns The trace, run to completion.
 */
const settlesBehindAnEarlierInstanceVoteOnceTheRetriesRunOut = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordCore, recordHash } =
      history.certifiedHead.actionCertifiedRecord;
    const earlierBody: ReanchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_body",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      previousAnchorHash: recordCore.anchorHash,
      selectedRecordHash: recordHash,
      routerInstanceId: newRouterInstanceId,
    };
    const { outbound, request } = yield* recoverGroup(
      fixture,
      n4,
      history.certifiedHead,
      { routerInstanceId: laterRouterInstanceId, clock: "caller" },
    );
    yield* deliverRecovery(
      n4.engine,
      peerEvidenceIngressFrom({
        membership: n4.membership,
        responder: n4.third,
        statement: {
          sharnessVersion: WIRE_VERSION,
          kind: "reanchor_vote",
          signerAgentId: n4.third.card.agentId,
          anchorHash: yield* hashAnchor(earlierBody),
          reanchor: earlierBody,
        },
        routerInstanceId: laterRouterInstanceId,
      }),
    );
    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [n4.third, fixture.remote],
      routerInstanceId: laterRouterInstanceId,
    });
    const whileRetrying = (yield* groupState(fixture, n4, recordHash))
      .candidates;
    yield* whileTheClockRuns(takeNextCarrying(outbound, "reanchor_vote"));
    const settled = (yield* groupState(fixture, n4, recordHash)).candidates;

    expect(whileRetrying).toEqual([]);
    expect(settled).toEqual([recordHash]);
  });

/**
 * Starts a recovery of the N4 engine at the new Router instance and has it
 * adopt the members' completed re-anchor at its head, as a page answering
 * its catch-up request.
 * @param fixture Endpoint holding the N4 conversation.
 * @param n4 The N4 conversation.
 * @param history The N4 head.
 * @returns The adopted re-anchor and the catch-up request the endpoint sends
 *     from it.
 */
const adoptAReanchorAtTheN4Head = (
  fixture: RecoveryFixture,
  n4: N4Foundation,
  history: N4PartialHistory,
) =>
  Effect.gen(function* () {
    const { recordCore, recordHash } =
      history.certifiedHead.actionCertifiedRecord;
    const { outbound, request } = yield* recoverGroup(
      fixture,
      n4,
      history.certifiedHead,
    );
    const completed = yield* completedReanchorBy(
      {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_body",
        conversationId: n4.membership.descriptor.conversationId,
        membershipHash: n4.membership.hash,
        previousAnchorHash: recordCore.anchorHash,
        selectedRecordHash: recordHash,
        routerInstanceId: newRouterInstanceId,
      },
      [fixture.remote, n4.third, n4.fourth],
    );
    yield* deliverRecovery(
      n4.engine,
      catchUpPageIngressFrom({
        membership: n4.membership,
        responder: n4.fourth,
        request,
        item: completed,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    return { completed, request: yield* takeCatchUpRequest(outbound) };
  });

/**
 * After the local endpoint adopts a re-anchor at the N4 head, a member sends
 * the successor of that head it staged under the anchor the conversation
 * left, with two members' votes for it. The endpoint holds nothing for it,
 * and still converts to the successor of the head under the new anchor once
 * two members vote for that one. Fails when a record from a left anchor
 * takes the endpoint's one conversion attempt at the head.
 * @returns The trace, run to completion.
 */
const convertsUnderTheNewAnchorPastASuccessorFromTheOldOne = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { completed } = yield* adoptAReanchorAtTheN4Head(
      fixture,
      n4,
      history,
    );
    const current = yield* certifiedN4MessageAt(fixture, n4, {
      routerAnchor: completed,
      previousRecordHash:
        history.certifiedHead.actionCertifiedRecord.recordHash,
    });
    const traffic = groupSuccessorTraffic(n4);

    yield* traffic.record(n4.fourth, history.stagedSuccessor);
    yield* traffic.votes(history.stagedSuccessor.recordHash, [
      fixture.remote,
      n4.fourth,
    ]);
    yield* traffic.record(n4.fourth, current.actionCertifiedRecord);
    yield* traffic.votes(current.actionCertifiedRecord.recordHash, [
      fixture.remote,
      n4.fourth,
    ]);
    const left = yield* groupState(
      fixture,
      n4,
      history.stagedSuccessor.recordHash,
    );
    const converted = yield* groupState(
      fixture,
      n4,
      current.actionCertifiedRecord.recordHash,
    );

    expect(left.staged).toBe(false);
    expect(converted.staged).toBe(true);
  });

/**
 * During a re-anchor the local endpoint holds the remote member's vote for
 * the successor of the N4 head, and a catch-up page then certifies that
 * successor. The remote member's vote for the next successor counts toward
 * converting to it, so the remote and third members' votes convert the
 * endpoint. Fails when what the endpoint held at the old head survives a
 * head that catch-up moved.
 * @returns The trace, run to completion.
 */
const convertsAtAHeadCatchUpMovedPastItsHold = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { completed, request } = yield* adoptAReanchorAtTheN4Head(
      fixture,
      n4,
      history,
    );
    const next = yield* certifiedN4MessageAt(fixture, n4, {
      routerAnchor: completed,
      previousRecordHash:
        history.certifiedHead.actionCertifiedRecord.recordHash,
    });
    const after = yield* certifiedN4MessageAt(fixture, n4, {
      routerAnchor: completed,
      previousRecordHash: next.actionCertifiedRecord.recordHash,
    });
    const traffic = groupSuccessorTraffic(n4);

    yield* traffic.votes(next.actionCertifiedRecord.recordHash, [
      fixture.remote,
    ]);
    yield* deliverRecovery(
      n4.engine,
      catchUpPageIngressFrom({
        membership: n4.membership,
        responder: n4.third,
        request,
        item: next,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    yield* traffic.record(n4.fourth, after.actionCertifiedRecord);
    yield* traffic.votes(after.actionCertifiedRecord.recordHash, [
      fixture.remote,
      n4.third,
    ]);
    const state = yield* groupState(
      fixture,
      n4,
      after.actionCertifiedRecord.recordHash,
    );

    expect(state.staged).toBe(true);
  });

/**
 * The local endpoint voted to re-anchor the N4 conversation at its head, so
 * the store refuses its conversion to the successor two holders vote for
 * under the old anchor. The members' re-anchor then completes and the
 * endpoint adopts it. A successor of the head under the new anchor, with two
 * members' votes, converts it. Fails when a refused conversion under one
 * anchor holds the endpoint at the same head under the next.
 * @returns The trace, run to completion.
 */
const convertsUnderAnAdoptedAnchorAfterARefusedConversion = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordCore, recordHash } =
      history.certifiedHead.actionCertifiedRecord;
    const { request } = yield* recoverGroup(fixture, n4, history.certifiedHead);
    const traffic = groupSuccessorTraffic(n4);
    const completed = yield* completedReanchorBy(
      {
        sharnessVersion: WIRE_VERSION,
        kind: "reanchor_body",
        conversationId: n4.membership.descriptor.conversationId,
        membershipHash: n4.membership.hash,
        previousAnchorHash: recordCore.anchorHash,
        selectedRecordHash: recordHash,
        routerInstanceId: newRouterInstanceId,
      },
      [fixture.remote, n4.third, n4.fourth],
    );
    const current = yield* certifiedN4MessageAt(fixture, n4, {
      routerAnchor: completed,
      previousRecordHash: recordHash,
    });

    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [fixture.remote, n4.third],
    });
    yield* traffic.record(n4.fourth, history.stagedSuccessor);
    yield* traffic.votes(history.stagedSuccessor.recordHash, [
      n4.third,
      n4.fourth,
    ]);
    yield* deliverRecovery(
      n4.engine,
      catchUpPageIngressFrom({
        membership: n4.membership,
        responder: n4.fourth,
        request,
        item: completed,
        routerInstanceId: newRouterInstanceId,
      }),
    );
    yield* traffic.record(n4.fourth, current.actionCertifiedRecord);
    yield* traffic.votes(current.actionCertifiedRecord.recordHash, [
      n4.third,
      n4.fourth,
    ]);
    const refused = yield* groupState(
      fixture,
      n4,
      history.stagedSuccessor.recordHash,
    );
    const converted = yield* groupState(
      fixture,
      n4,
      current.actionCertifiedRecord.recordHash,
    );

    expect(refused.staged).toBe(false);
    expect(converted.staged).toBe(true);
  });

/**
 * After one Router restart, a faulty N4 member sends a vote for a re-anchor
 * at the head for another Router instance whose signed anchor hash is not
 * the hash of the re-anchor it names. The vote makes the local endpoint wait
 * for no one: the remote and third members' `incomplete` answers make its
 * position ready, and it votes to re-anchor at the head. Fails when a vote
 * that does not hash to its anchor holds a position for every member.
 * @returns The trace, run to completion.
 */
const settlesPastAnEarlierInstanceVoteForAnotherAnchor = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const { recordCore, recordHash } =
      history.certifiedHead.actionCertifiedRecord;
    const { request } = yield* recoverGroup(fixture, n4, history.certifiedHead);
    const earlierBody: ReanchorBody = {
      sharnessVersion: WIRE_VERSION,
      kind: "reanchor_body",
      conversationId: n4.membership.descriptor.conversationId,
      membershipHash: n4.membership.hash,
      previousAnchorHash: recordCore.anchorHash,
      selectedRecordHash: recordHash,
      routerInstanceId: oldRouterInstanceId,
    };

    yield* deliverRecovery(
      n4.engine,
      peerEvidenceIngressFrom({
        membership: n4.membership,
        responder: n4.third,
        statement: {
          sharnessVersion: WIRE_VERSION,
          kind: "reanchor_vote",
          signerAgentId: n4.third.card.agentId,
          anchorHash: yield* hashAnchor({
            ...earlierBody,
            routerInstanceId: laterRouterInstanceId,
          }),
          reanchor: earlierBody,
        },
        routerInstanceId: newRouterInstanceId,
      }),
    );
    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [fixture.remote, n4.third],
    });
    const state = yield* groupState(fixture, n4, recordHash);

    expect(state.candidates).toEqual([recordHash]);
  });

/**
 * A member's vote for an earlier Router instance's re-anchor at the pending
 * position is still verifying when a catch-up retry replaces the pending
 * request with an equal one. Once the vote verifies, the position's
 * readiness is decided: it waits for every member. Fails when the wait
 * checks the request it started with by identity, so an identical retry
 * drops it.
 * @returns The trace, run to completion.
 */
const keepsAnEarlierInstanceWaitAcrossAnIdenticalRetry = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { recordCore, recordHash } =
      fixture.certifiedRecord.actionCertifiedRecord;
    const request = peerCatchUpRequest(fixture, {
      knownRecordHash: recordHash,
      knownAnchorHash: recordCore.anchorHash,
    });
    const { conversationId } = request;
    const state = makeCatchUpState();
    state.pendingRequests.set(conversationId, request);
    const verifying = yield* Deferred.make<undefined>();
    const waiting = yield* Effect.fork(
      fixture.store.run(
        waitBehindEarlierReanchor(
          { state },
          {
            conversationId,
            previousAnchorHash: recordCore.anchorHash,
            selectedRecordHash: recordHash,
          },
          Deferred.await(verifying),
        ),
      ),
    );

    state.pendingRequests.set(conversationId, { ...request });
    yield* Deferred.succeed(verifying, undefined);
    yield* Fiber.join(waiting);

    expect(state.readiness.has(conversationId)).toBe(true);
  });

/**
 * After a Router restart the local endpoint votes to re-anchor the N4
 * conversation at its head for the new Router instance. A member's catch-up
 * request at the head, delivered by that same instance, gets `incomplete`
 * alone: the endpoint's vote for the delivering instance is that instance's
 * re-anchor traffic, not partial evidence of the answer. Fails when an
 * `incomplete` answer carries a vote for the Router instance that delivers
 * the request.
 * @returns The trace, run to completion.
 */
const answersIncompleteWithoutItsVoteForTheDeliveringInstance = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    const head = history.certifiedHead.actionCertifiedRecord;
    const { outbound, request } = yield* recoverGroup(
      fixture,
      n4,
      history.certifiedHead,
    );
    yield* answerN4Incomplete(fixture, n4, request, {
      responders: [fixture.remote, n4.third],
    });
    yield* takeReanchorVote(outbound);

    yield* deliverRecovery(
      n4.engine,
      directPacketIngressFrom({
        membership: n4.membership,
        sender: fixture.remote,
        packet: peerCatchUpRequest(
          fixture,
          {
            knownRecordHash: head.recordHash,
            knownAnchorHash: head.recordCore.anchorHash,
          },
          n4.membership,
        ),
        routerInstanceId: newRouterInstanceId,
      }),
    );
    const answered = yield* takeSentBodies(outbound, 1);

    expect(answered).toMatchObject([
      { kind: "direct", packet: { kind: "catch_up_incomplete" } },
    ]);
  });

describe("staged successors in recovery", () => {
  it(
    "certifies a message one direct member staged before the restart, and re-anchors at it",
    certifiesAMessageOneDirectMemberStagedBeforeTheRestart,
  );
  it(
    "certifies a staged successor on the vote a member sends again, after answering with it",
    certifiesAStagedSuccessorOnAVoteAMemberSendsAgain,
  );
  it(
    "sends its staged successor again instead of voting at the head it extends",
    resendsItsStagedSuccessorInsteadOfVotingAtItsHead,
  );
  it(
    "converts to a three-member successor on one holder's vote",
    convertsToAThreeMemberSuccessorOnOneVote,
  );
  it(
    "votes at the head over an N4 successor only its author staged",
    votesAtTheHeadOverASuccessorOnlyItsAuthorStaged,
  );
  it("converts on two N4 holders' votes", () =>
    convertsOnTwoN4HoldersUnlessItLeftTheAnchor("none"));
  it("refuses to convert after staging a re-anchor candidate at the head", () =>
    convertsOnTwoN4HoldersUnlessItLeftTheAnchor("candidate"));
  it(
    "converts to a seven-member successor only above the fault bound",
    convertsToASevenMemberSuccessorAboveTheFaultBound,
  );
  it(
    "stages a successor on its record alone after a feed gap",
    stagesASuccessorOnItsRecordAloneAfterAFeedGap,
  );
  it(
    "answers with the successor it staged instead of incomplete during a re-anchor",
    answersWithTheSuccessorItStagedInsteadOfIncomplete,
  );
  it(
    "stages no candidate at the head while a holder answers with its successor",
    waitsBehindASuccessorAHolderAnswersWith,
  );
  itOnTestClock(
    "never settles behind a successor a holder answers with after a feed gap",
    neverSettlesBehindASuccessorAfterAFeedGap,
  );
  it(
    "settles on a quorum at once with no earlier re-anchor vote",
    settlesOnAQuorumWithNoEarlierReanchor,
  );
  it("waits behind its own earlier-instance re-anchor vote and adopts that re-anchor", () =>
    waitsBehindAnEarlierInstanceReanchor("own"));
  it("waits behind a member's earlier-instance re-anchor vote and adopts that re-anchor", () =>
    waitsBehindAnEarlierInstanceReanchor("member"));
  it(
    "settles past an earlier-instance vote whose anchor hash names another re-anchor",
    settlesPastAnEarlierInstanceVoteForAnotherAnchor,
  );
  it(
    "answers incomplete with its earlier-instance re-anchor vote",
    answersIncompleteWithItsEarlierInstanceVote,
  );
  it(
    "answers incomplete without its vote for the Router instance that delivers the request",
    answersIncompleteWithoutItsVoteForTheDeliveringInstance,
  );
  it(
    "keeps an earlier-instance wait across an identical catch-up retry",
    keepsAnEarlierInstanceWaitAcrossAnIdenticalRetry,
  );
  itOnTestClock(
    "settles behind an earlier-instance re-anchor vote once its retries run out",
    settlesBehindAnEarlierInstanceVoteOnceTheRetriesRunOut,
  );
  it(
    "converts under a new anchor past a successor from the anchor it left",
    convertsUnderTheNewAnchorPastASuccessorFromTheOldOne,
  );
  it(
    "converts at a head catch-up moved past what it held",
    convertsAtAHeadCatchUpMovedPastItsHold,
  );
  it(
    "converts under an adopted anchor after a refused conversion at the same head",
    convertsUnderAnAdoptedAnchorAfterARefusedConversion,
  );
});

/**
 * Catch-up accepts a held page without consuming attestations or advancing its
 * request. The certification port deliberately holds the record until the
 * test promotes it through the real certification path.
 */
const retriesWhileARecordPageAwaitsPromotion = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const n4 = yield* addN4Foundation(fixture);
    const history = yield* buildN4PartialHistory(fixture, n4);
    yield* directPacketIngressFrom({
      membership: n4.membership,
      sender: fixture.remote,
      packet: history.certifiedHead,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => n4.engine.acceptRouterIngress(ingress)),
    );
    const recovered = yield* recoverEngineState(
      fixture.input,
      yield* fixture.store.run(testStore.recover()),
    );
    const queued = yield* Queue.unbounded<DecodedOuterBody>();
    const unexpected = () =>
      Effect.dieMessage("unexpected phase in held-page trace");
    const runtime: EngineRuntime = {
      wireVersion: WIRE_VERSION,
      input: fixture.input,
      conversations: recovered.conversations,
      intents: new Map(),
      completedMessages: recovered.completedMessages,
      actionFolds: recovered.actionFolds,
      recordFolds: recovered.recordFolds,
      gate: yield* Effect.makeSemaphore(1),
      scope: yield* Effect.scope,
      outbox: yield* makeOutbox(fixture.input, []),
      phases: {
        proposeIntent: unexpected,
        acceptIngress: unexpected,
        acceptRecoveryIngress: () => Effect.succeed(acceptedDisposition),
        resumeFolds: unexpected,
        rearmCatchUp: unexpected,
        isRecovering: () => true,
        headAdvanced: advanceHead,
      },
    };
    const run: CatchUpRun = {
      runtime,
      state: makeCatchUpState(),
      isActive: () => true,
      membership: () => n4.membership,
      routerInstanceId: newRouterInstanceId,
      onPositionReady: unexpected,
      queue: (membership, body) =>
        membership.hash === n4.membership.hash
          ? Queue.offer(queued, body).pipe(Effect.asVoid)
          : unexpected(),
    };
    function advanceHead() {
      return fixture.store.run(
        requestCertifiedHistory(run, n4.membership.descriptor.conversationId),
      );
    }
    const conversationId = n4.membership.descriptor.conversationId;
    yield* fixture.store.run(requestCertifiedHistory(run, conversationId));
    const pending = run.state.pendingRequests.get(conversationId);
    if (pending === undefined) {
      return yield* Effect.dieMessage("catch-up did not retain its request");
    }
    yield* Queue.takeAll(queued);
    run.state.incompleteResponders.set(
      conversationId,
      new Set([n4.third.card.agentId]),
    );
    const ingress = yield* catchUpPageIngressFrom({
      membership: n4.membership,
      responder: fixture.remote,
      request: pending,
      item: history.certifiedSuccessor,
      routerInstanceId: newRouterInstanceId,
    });
    if (
      ingress.payload.kind !== "direct" ||
      ingress.payload.packet.kind !== "catch_up_page"
    ) {
      return yield* Effect.dieMessage("fixture did not deliver a page");
    }
    expect(
      yield* fixture.store.run(
        acceptCatchUpPage(run, ingress, ingress.payload.packet),
      ),
    ).toBe(acceptedDisposition);
    expect(yield* Queue.size(queued)).toBe(0);
    expect(run.state.incompleteResponders.get(conversationId)).toEqual(
      new Set([n4.third.card.agentId]),
    );
    yield* fixture.store.run(
      resendCertifiedHistoryRequest(run, conversationId),
    );
    expect(yield* Queue.take(queued)).toEqual({
      kind: "direct",
      packet: pending,
    });
    const before = yield* groupState(
      fixture,
      n4,
      history.stagedSuccessor.recordHash,
    );
    expect(before.head).toBe(pending.knownRecordHash);
    yield* fixture.store.run(
      acceptEngineRecoveryIngress(runtime, {
        ...ingress,
        payload: { kind: "direct", packet: history.certifiedSuccessor },
      }),
    );
    const after = run.state.pendingRequests.get(conversationId);
    expect(after?.knownRecordHash).toBe(history.stagedSuccessor.recordHash);
    expect(yield* Queue.take(queued)).toEqual({
      kind: "direct",
      packet: after,
    });
  });

describe("held catch-up pages", () => {
  it(
    "retries at its head while a record page awaits promotion",
    retriesWhileARecordPageAwaitsPromotion,
  );
});

/* eslint-enable max-lines, max-lines-per-function -- Restore repository defaults. */

/**
 * The remote member's MESSAGE proposal at the fixture's retained head.
 * @param fixture Endpoint whose retained FIRST_MESSAGE record is the head.
 * @returns A proposal the remote member authors, extending that head.
 */
const remoteProposalAtTheHead = (
  fixture: RecoveryFixture,
): Effect.Effect<ActionProposal> =>
  Effect.gen(function* () {
    const { recordCore, recordHash } =
      fixture.certifiedRecord.actionCertifiedRecord;
    const messageIntent: MessageIntent = {
      sharnessVersion: WIRE_VERSION,
      kind: "message_intent",
      conversationId: fixture.membership.descriptor.conversationId,
      membershipHash: fixture.membership.hash,
      authorAgentId: fixture.remote.card.agentId,
      msgId: yield* mintMsgId(),
      content: [{ type: "text", text: "proposed before the discontinuity" }],
    };
    const action: MessageActionCore = {
      sharnessVersion: WIRE_VERSION,
      kind: "MESSAGE",
      conversationId: messageIntent.conversationId,
      membershipHash: fixture.membership.hash,
      anchorHash: recordCore.anchorHash,
      previousRecordHash: recordHash,
      messageIntent,
      messageIntentHash: yield* hashMessageIntent(messageIntent),
    };
    return {
      sharnessVersion: WIRE_VERSION,
      kind: "action_proposal",
      action,
    } satisfies ActionProposal;
  }).pipe(Effect.orDie);

/**
 * Takes every envelope `outbound` holds and reads the action hash each action
 * signature among them names.
 * @param outbound Queue a fixture Router forwarded to.
 * @returns The signed action hashes, in send order.
 */
const takeSignedActionHashes = (outbound: Queue.Queue<SignedMessage>) =>
  Queue.takeAll(outbound).pipe(
    Effect.flatMap((messages) =>
      Effect.forEach(
        messages,
        (message) =>
          openForwarded(message).pipe(
            Effect.flatMap((body) =>
              body.kind === "evidence"
                ? decodeCanonical(EvidenceStatement, body.message.body)
                : Effect.succeed(undefined),
            ),
          ),
        { concurrency: 1 },
      ),
    ),
    Effect.map((statements) =>
      statements.flatMap((statement) =>
        statement?.kind === "action_signature" ? [statement.actionHash] : [],
      ),
    ),
    Effect.orDie,
  );

/**
 * Retains the fixture's FIRST_MESSAGE record, has the local endpoint lock and sign
 * the remote member's proposal at it, and sends that signature.
 * @param fixture Endpoint that signs.
 * @returns The proposal's action hash, and the action hashes the endpoint's
 *     sent signatures name.
 */
const signARemoteProposal = (fixture: RecoveryFixture) =>
  Effect.gen(function* () {
    yield* retainCertifiedRecord(fixture);
    const proposal = yield* remoteProposalAtTheHead(fixture);
    yield* directPacketIngressFrom({
      membership: fixture.membership,
      sender: fixture.remote,
      packet: proposal,
      routerInstanceId: oldRouterInstanceId,
    }).pipe(
      Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    );
    yield* fixture.engine.drainOutbound;
    return {
      actionHash: yield* hashAction(proposal.action),
      signed: yield* takeSignedActionHashes(fixture.normalOutbound),
    };
  }).pipe(Effect.orDie);

/**
 * The remote member asks for catch-up from first message, and the local endpoint's
 * answer is sent. The answer queues behind every envelope the endpoint queued
 * before it and one outbound loop sends them in order, so once the answer is
 * sent, so are they.
 * @param fixture Endpoint that answers, its outbound loop running.
 * @returns Completion once the answer reaches the fixture Router.
 */
const awaitAnswerToACatchUpRequest = (fixture: RecoveryFixture) =>
  directPacketIngressFrom({
    membership: fixture.membership,
    sender: fixture.remote,
    packet: peerCatchUpRequest(fixture),
    routerInstanceId: oldRouterInstanceId,
  }).pipe(
    Effect.flatMap((ingress) => fixture.engine.acceptRouterIngress(ingress)),
    Effect.zipRight(Queue.take(fixture.recoveryOutbound)),
    Effect.flatMap((message) => openForwarded(message)),
    Effect.filterOrDieMessage(
      (body) => body.kind === "direct" && body.packet.kind === "catch_up_page",
      "expected the answer to the catch-up request",
    ),
    Effect.asVoid,
  );

/**
 * The local endpoint signs the remote member's proposal at its head and sends
 * the signature. A discontinuity of `reason` follows, and recovery at the
 * same Router instance completes by catch-up. Only a Router restart loses
 * what the Router held, so only then does the endpoint send that signature
 * again; after a feed gap it sends none. Fails when abandoning volatile folds
 * stops re-arming their evidence on a Router restart, or re-arms it for every
 * reason.
 * @param reason Discontinuity the endpoint recovers from.
 * @param resent Signatures for the proposal the endpoint sends again.
 * @returns The trace, run to completion.
 */
const resendsItsSignatureForAHeldProposal = (
  reason: RouterDiscontinuityReason,
  resent: number,
) =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const { actionHash, signed } = yield* signARemoteProposal(fixture);

    yield* runSameInstanceRecovery(fixture, reason, Effect.void);
    yield* awaitAnswerToACatchUpRequest(fixture);
    const signedAgain = yield* takeSignedActionHashes(fixture.normalOutbound);

    expect(signed).toEqual([actionHash]);
    expect(
      signedAgain.filter((hash) => hash === actionHash),
      "signatures for the proposal sent after the recovery",
    ).toHaveLength(resent);
  });

/**
 * An outbox row the store holds at startup goes out once the restarted
 * engine's outbound loop runs, with no send, ingress or recovery to wake the
 * loop. Fails when the outbox ignores the rows the store retains, or does not
 * wake its loop for them, so the row waits for unrelated traffic.
 * @returns The trace, run to completion.
 */
const sendsARetainedOutboxRowAfterAColdStart = () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture;
    const retained = yield* stageCatchUpOutbound(fixture);
    const restarted = yield* makeEndpointEngine(fixture.input).pipe(
      Effect.provideService(SqlClient.SqlClient, fixture.store.sql),
    );

    yield* restarted.runOutbound.pipe(Effect.orDie, Effect.forkScoped);
    const sent = yield* Queue.take(fixture.recoveryOutbound);

    expect(sent.envelopeId).toBe(retained.envelopeId);
    expect(
      (yield* fixture.store.run(testStore.recover())).outboundMessages,
    ).toEqual([]);
  });

describe("evidence and outbox rows an engine resumes", () => {
  it("sends its signature for a proposal it signed again after a Router restart recovery", () =>
    resendsItsSignatureForAHeldProposal("router_restarted", 1));
  it("sends no second signature for a proposal it signed after a feed-gap recovery", () =>
    resendsItsSignatureForAHeldProposal("feed_gap", 0));
  it(
    "sends an outbox row the store retained once a restarted engine's loop runs",
    sendsARetainedOutboxRowAfterAColdStart,
  );
});
