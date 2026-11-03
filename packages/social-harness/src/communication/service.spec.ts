/** @file Communication rejects malformed ingress while delivering later verified messages. */

import { SqlClient } from "@effect/sql/SqlClient";
import { scopedLive as it } from "@effect/vitest";
import {
  Context,
  Data,
  Deferred,
  Effect,
  Fiber,
  Layer,
  Option,
  Queue,
  Ref,
  Schema,
} from "effect";
import { expect } from "vitest";
import { identifier } from "../__tests__/agents.js";
import { encodeCanonical } from "../encoding/canonical-json.js";
import {
  agentCardOf,
  AgentName,
  type VerifiedAgentIdentityCard,
} from "../identity/agent-cards.js";
import {
  EnvelopeId,
  type LiveSignedMessage,
  SealedBody,
  SignedMessage,
} from "../identity/message-security.js";
import { Registry, RegistryError } from "../identity/registry.js";
import {
  conversationIdentityFixture,
  openOuterBody,
} from "./__tests__/conversation-fixtures.js";
import {
  appendCertifiedMessage,
  buildCertifiedFirstMessage,
  type SigningIdentity,
  storeCertifiedFirstMessage,
} from "./__tests__/records-fixtures.js";
import { batch, emptyBatch, pollCursor } from "./__tests__/router-fixtures.js";
import {
  encodeCollectiveContent,
  type Message,
  messageFromIntent,
  messageFromParticipants,
  mintMsgId,
} from "./messages.js";
import { DirectPacket, signOuterPacket } from "./records.js";
import { Router } from "./router/client.js";
import {
  Answer,
  Communication,
  CommunicationError,
  type Conversation,
  type ReceivedRequest,
  Request,
  RequestedSchema,
} from "./service.js";

class ConsumerFailure extends Data.TaggedError("ConsumerFailure") {}

interface Members {
  readonly local: SigningIdentity;
  readonly peer: SigningIdentity;
  readonly outsider: SigningIdentity;
}

interface RefusedEnvelope {
  readonly sealedTo?: (
    members: Members,
  ) => readonly VerifiedAgentIdentityCard[];
  readonly sealedAs: number;
  readonly signer: (members: Members) => SigningIdentity;
  readonly signedAs: number;
}

const envelopeId = (byte: number) =>
  Schema.decodeUnknownSync(EnvelopeId)(identifier("env_", byte));

function refusedEnvelope(
  refused: RefusedEnvelope,
  members: Members,
  plaintext: Uint8Array,
) {
  return Effect.gen(function* () {
    const signer = refused.signer(members);
    const body =
      refused.sealedTo === undefined
        ? plaintext
        : yield* SealedBody.seal({
            senderAgentId: members.peer.card.agentId,
            recipientIdentityCards: refused.sealedTo(members),
            envelopeId: envelopeId(refused.sealedAs),
            plaintext,
          });
    return yield* SignedMessage.sign({
      identityCard: signer.card,
      signingAuthority: signer.authority,
      recipientAgentIds: new Set([
        members.local.card.agentId,
        members.peer.card.agentId,
      ]),
      envelopeId: envelopeId(refused.signedAs),
      body,
    });
  });
}

/** The network supplies one batch after activation and holds subsequent cursor polls. */
function feedingRouter(input: {
  readonly subject: Effect.Effect.Success<typeof conversationIdentityFixture>;
  readonly feed: readonly LiveSignedMessage[];
  readonly release: Deferred.Deferred<undefined>;
}) {
  return Effect.gen(function* () {
    const fed = yield* Ref.make(false);
    return Router.of({
      poll: ({ request }) =>
        request.pollCursor === undefined
          ? Effect.succeed(
              emptyBatch(
                input.subject.anchor.routerInstanceId,
                input.subject.anchor.pollCursor,
              ),
            )
          : Deferred.await(input.release).pipe(
              Effect.zipRight(Ref.getAndSet(fed, true)),
              Effect.flatMap((alreadyFed) =>
                alreadyFed
                  ? Effect.never
                  : Effect.succeed(
                      batch(
                        input.subject.anchor.routerInstanceId,
                        pollCursor(25),
                        input.feed,
                      ),
                    ),
              ),
            ),
      send: () => Effect.never,
    });
  });
}

type Subject = Effect.Effect.Success<typeof conversationIdentityFixture>;

function prepareIngress(refused: RefusedEnvelope) {
  return Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const members: Members = {
      local: subject.local,
      peer: subject.remote,
      outsider: subject.third,
    };
    const discarded = yield* buildCertifiedFirstMessage(
      subject.local,
      subject.remote,
      subject.membership,
      subject.anchor.routerInstanceId,
    );
    const accepted = yield* buildCertifiedFirstMessage(
      subject.local,
      subject.remote,
      subject.membership,
      subject.anchor.routerInstanceId,
    );
    const validEnvelope = yield* signOuterPacket({
      packet: accepted,
      membership: subject.membership,
      identityCard: subject.remote.card,
      signingAuthority: subject.remote.authority,
    });
    const release = yield* Deferred.make<undefined>();
    const router = yield* feedingRouter({
      subject,
      release,
      feed: [
        yield* refusedEnvelope(
          refused,
          members,
          yield* encodeCanonical(DirectPacket, discarded),
        ),
        validEnvelope,
      ],
    });
    return { subject, accepted, release, router };
  });
}

function openCommunication(
  subject: Subject,
  router: Context.Tag.Service<typeof Router>,
  registry?: Context.Tag.Service<typeof Registry>,
) {
  return Effect.gen(function* () {
    const dependencies = Layer.mergeAll(
      Layer.succeed(SqlClient, subject.sql),
      Layer.succeed(Registry, registry ?? registryFor(subject)),
      Layer.succeed(Router, router),
    );
    const context = yield* Layer.build(
      Communication.layer({
        agentKey: subject.directory,
        identityCard: subject.local.card,
        signingAuthority: subject.local.authority,
        registrySignerPublicKey: subject.signer,
      }).pipe(Layer.provide(dependencies)),
    );
    return { communication: Context.get(context, Communication) };
  });
}

function registryFor(subject: Subject) {
  return Registry.of({
    register: () => Effect.dieMessage("The consumer is already registered"),
    lookup: (request) => {
      const card = [
        subject.local.card,
        subject.remote.card,
        subject.third.card,
      ].find((candidate) =>
        "agentName" in request
          ? candidate.agentName === request.agentName
          : candidate.agentId === request.agentId,
      );
      return Effect.succeed(
        card === undefined
          ? { kind: "not_found" as const }
          : { kind: "found" as const, identityCard: card },
      );
    },
  });
}

function deliversAfterRefusedBody(refused: RefusedEnvelope) {
  return Effect.gen(function* () {
    const { subject, accepted, release, router } =
      yield* prepareIngress(refused);
    const { communication } = yield* openCommunication(subject, router);
    const received = yield* Queue.unbounded<Message>();
    yield* communication
      .consume((incoming) =>
        incoming.kind === "message"
          ? Queue.offer(received, incoming.message).pipe(Effect.asVoid)
          : Effect.void,
      )
      .pipe(Effect.forkScoped);
    yield* Deferred.succeed(release, undefined);
    const message = yield* Queue.take(received).pipe(
      Effect.timeout("10 seconds"),
    );
    const expected = yield* messageFromIntent(
      [agentCardOf(subject.local.card), agentCardOf(subject.remote.card)],
      accepted.actionCertifiedRecord.recordCore.action.messageIntent,
    );
    expect(message).toEqual(expected);
  });
}

// eslint-disable-next-line agent-code-guard/no-example-only-tests -- Restart and rollback regressions require scoped SQL and Router fixtures.
it.each<{ readonly body: string; readonly refused: RefusedEnvelope }>([
  {
    body: "a plaintext body",
    refused: { sealedAs: 31, signer: ({ peer }) => peer, signedAs: 31 },
  },
  {
    body: "a body sealed to other recipients",
    refused: {
      sealedTo: ({ peer, outsider }) => [peer.card, outsider.card],
      sealedAs: 32,
      signer: ({ peer }) => peer,
      signedAs: 32,
    },
  },
  {
    body: "a sealed body re-signed by another sender",
    refused: {
      sealedTo: ({ local, peer }) => [local.card, peer.card],
      sealedAs: 33,
      signer: ({ outsider }) => outsider,
      signedAs: 33,
    },
  },
  {
    body: "a sealed body re-signed under another EnvelopeId",
    refused: {
      sealedTo: ({ local, peer }) => [local.card, peer.card],
      sealedAs: 34,
      signer: ({ peer }) => peer,
      signedAs: 35,
    },
  },
])("delivers the next certified message after refusing $body", ({ refused }) =>
  deliversAfterRefusedBody(refused),
);

it("retains offline messages across cancelled waits and rolls acceptance back with the caller transaction", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const { communication } = yield* openCommunication(
      subject,
      Router.of({ poll: () => Effect.never, send: () => Effect.never }),
    );
    const message = yield* messageFromParticipants(
      agentCardOf(subject.local.card),
      [agentCardOf(subject.remote.card)],
      {
        msgId: yield* mintMsgId(),
        content: [{ type: "text", text: "accepted offline" }],
      },
    );
    const members: Conversation = [
      agentCardOf(subject.local.card),
      agentCardOf(subject.remote.card),
    ];
    expect(
      yield* communication.readHistory({ members, order: "newest", limit: 10 }),
    ).toEqual({
      entries: [],
      earlierMessagesOmitted: false,
      pendingOutgoing: false,
    });
    yield* communication.send(message).pipe(Effect.timeout("1 second"));
    yield* communication.send(message);
    expect(
      yield* communication.readHistory({ members, order: "newest", limit: 10 }),
    ).toEqual({
      entries: [],
      earlierMessagesOmitted: false,
      pendingOutgoing: true,
    });
    expect(
      yield* communication
        .readHistory({
          members,
          order: "newest",
          limit: 10,
          beforeMessage: {
            authorAgentId: message.author.agentId,
            msgId: message.msgId,
          },
        })
        .pipe(Effect.flip),
    ).toMatchObject({ reason: "history-gap" });
    const conflict = yield* communication
      .send({ ...message, content: [{ type: "text", text: "different" }] })
      .pipe(Effect.flip);
    expect(conflict).toEqual(
      new CommunicationError({ reason: "message-conflict" }),
    );
    const rolledBack = { ...message, msgId: yield* mintMsgId() };
    yield* subject.sql
      .withTransaction(
        communication
          .send(rolledBack)
          .pipe(Effect.zipRight(Effect.fail("cancel operation"))),
      )
      .pipe(Effect.flip);
    const missing = yield* communication
      .waitForMessage({
        authorAgentId: rolledBack.author.agentId,
        msgId: rolledBack.msgId,
      })
      .pipe(Effect.flip);
    expect(missing).toEqual(
      new CommunicationError({ reason: "unknown-message" }),
    );
    const reference = {
      authorAgentId: message.author.agentId,
      msgId: message.msgId,
    };
    expect(
      yield* communication
        .waitForMessage(reference)
        .pipe(Effect.timeoutOption("150 millis")),
    ).toEqual(Option.none());
  }));

it("returns the retained Message after certified history is reopened without Router", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const record = yield* buildCertifiedFirstMessage(
      subject.remote,
      subject.local,
      subject.membership,
      subject.anchor.routerInstanceId,
    );
    yield* subject.run(
      storeCertifiedFirstMessage(
        subject.store,
        subject.local.card,
        subject.membership,
        record,
      ),
    );
    const expected = yield* messageFromIntent(
      subject.membership.members.map(agentCardOf),
      record.actionCertifiedRecord.recordCore.action.messageIntent,
    );
    const offline = Router.of({
      poll: () => Effect.never,
      send: () => Effect.never,
    });
    const read = Effect.scoped(
      openCommunication(subject, offline).pipe(
        Effect.flatMap(({ communication }) =>
          communication.waitForMessage({
            authorAgentId: expected.author.agentId,
            msgId: expected.msgId,
          }),
        ),
      ),
    );
    expect(yield* read).toEqual(expected);
    expect(yield* read).toEqual(expected);
  }));

it("retains a terminal preparation failure across reopen", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const reference = {
      authorAgentId: subject.local.card.agentId,
      msgId: yield* mintMsgId(),
    };
    const peer = {
      ...agentCardOf(subject.remote.card),
      agentName: AgentName.make("wrong-peer-name"),
    };
    const message = yield* messageFromParticipants(
      agentCardOf(subject.local.card),
      [peer],
      {
        msgId: reference.msgId,
        content: [{ type: "text", text: "check the retained failure" }],
      },
    );
    const attached = Router.of({
      poll: ({ request }) =>
        request.pollCursor === undefined
          ? Effect.succeed(
              emptyBatch(
                subject.anchor.routerInstanceId,
                subject.anchor.pollCursor,
              ),
            )
          : Effect.never,
      send: () => Effect.never,
    });
    const first = yield* Effect.scoped(
      openCommunication(subject, attached).pipe(
        Effect.flatMap(({ communication }) =>
          communication
            .send(message)
            .pipe(
              Effect.zipRight(communication.waitForMessage(reference)),
              Effect.flip,
            ),
        ),
      ),
    ).pipe(Effect.timeout("3 seconds"));
    expect(first).toEqual(
      new CommunicationError({ reason: "membership-invalid" }),
    );
    const repeated = yield* Effect.scoped(
      openCommunication(
        subject,
        Router.of({ poll: () => Effect.never, send: () => Effect.never }),
      ).pipe(
        Effect.flatMap(({ communication }) =>
          communication.waitForMessage(reference).pipe(Effect.flip),
        ),
      ),
    );
    expect(repeated).toEqual(
      new CommunicationError({ reason: "membership-invalid" }),
    );
  }));

it("offers the same certified Message again after its consumer fails", () =>
  Effect.gen(function* () {
    const { subject, release, router } = yield* prepareIngress({
      sealedAs: 31,
      signer: ({ peer }) => peer,
      signedAs: 31,
    });
    const { communication } = yield* openCommunication(subject, router);
    const expectedFailure = new ConsumerFailure();
    const failed = yield* communication
      .consume(() => Effect.fail(expectedFailure))
      .pipe(Effect.forkScoped);
    yield* Deferred.succeed(release, undefined);
    const failure = yield* Fiber.join(failed).pipe(Effect.flip);
    expect(failure._tag).toBe(expectedFailure._tag);
    const replayed = yield* Queue.unbounded<Message>();
    yield* communication
      .consume((incoming) =>
        incoming.kind === "message"
          ? Queue.offer(replayed, incoming.message).pipe(Effect.asVoid)
          : Effect.void,
      )
      .pipe(Effect.forkScoped);
    const message = yield* Queue.take(replayed).pipe(
      Effect.timeout("3 seconds"),
    );
    expect(message.author).toEqual(agentCardOf(subject.remote.card));
    expect(message.recipients).toEqual([agentCardOf(subject.local.card)]);
  }));

it("keeps accepted messages in order when the first preparation temporarily cannot reach Registry", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const failLookup = yield* Ref.make(true);
    const available = registryFor(subject);
    const registry = Registry.of({
      ...available,
      lookup: (request) =>
        Ref.getAndSet(failLookup, false).pipe(
          Effect.flatMap((fail) =>
            fail
              ? Effect.fail(new RegistryError({ reason: "connection" }))
              : available.lookup(request),
          ),
        ),
    });
    const firstProposal = yield* Deferred.make<Message["msgId"]>();
    const router = Router.of({
      poll: ({ request }) =>
        request.pollCursor === undefined
          ? Effect.succeed(
              emptyBatch(
                subject.anchor.routerInstanceId,
                subject.anchor.pollCursor,
              ),
            )
          : Effect.never,
      send: ({ request }) =>
        openOuterBody(request.signedMessage, subject.local).pipe(
          Effect.flatMap((body) =>
            body.kind === "direct" && body.packet.kind === "action_proposal"
              ? Deferred.succeed(
                  firstProposal,
                  body.packet.action.messageIntent.msgId,
                )
              : Effect.void,
          ),
          Effect.zipRight(Effect.never),
          Effect.orDie,
        ),
    });
    const { communication } = yield* openCommunication(
      subject,
      router,
      registry,
    );
    const first = yield* messageFromParticipants(
      agentCardOf(subject.local.card),
      [agentCardOf(subject.remote.card)],
      {
        msgId: yield* mintMsgId(),
        content: [{ type: "text", text: "request" }],
      },
    );
    const later: Message = {
      ...first,
      msgId: yield* mintMsgId(),
      content: [{ type: "text", text: "close" }],
    };
    yield* subject.sql.withTransaction(
      communication
        .send(first)
        .pipe(Effect.zipRight(communication.send(later))),
    );
    expect(
      yield* Deferred.await(firstProposal).pipe(Effect.timeout("3 seconds")),
    ).toBe(first.msgId);
  }));

it(
  "pages a fixed history head and excludes the candidate from a preceding window",
  () =>
    Effect.gen(function* () {
      const subject = yield* conversationIdentityFixture;
      let record = yield* buildCertifiedFirstMessage(
        subject.local,
        subject.remote,
        subject.membership,
        subject.anchor.routerInstanceId,
      );
      yield* subject.run(
        storeCertifiedFirstMessage(
          subject.store,
          subject.local.card,
          subject.membership,
          record,
        ),
      );
      const endpoint = {
        store: subject.store,
        local: subject.local,
        remote: subject.remote,
        membership: subject.membership,
      };
      for (let index = 1; index < 53; index += 1) {
        record = yield* subject.run(
          appendCertifiedMessage(endpoint, record, `message ${index}`),
        );
      }
      const { communication } = yield* openCommunication(
        subject,
        Router.of({ poll: () => Effect.never, send: () => Effect.never }),
      );
      const members = [
        agentCardOf(subject.local.card),
        agentCardOf(subject.remote.card),
      ] satisfies Conversation;
      expect((yield* communication.listConversations()).conversations).toEqual([
        members,
      ]);
      expect(
        yield* communication.getConversation([
          members[1].agentName,
          members[0].agentName,
        ]),
      ).toEqual(members);
      expect(
        yield* communication.getConversation([
          members[0].agentName,
          subject.third.card.agentName,
        ]),
      ).toBeUndefined();
      const first = yield* communication.readHistory({ members });
      expect(first.entries).toHaveLength(50);
      if (first.nextCursor === undefined) {
        return yield* Effect.dieMessage("history omitted its continuation");
      }
      yield* subject.run(
        appendCertifiedMessage(endpoint, record, "later message"),
      );
      const second = yield* communication.readHistory({
        members,
        cursor: first.nextCursor,
      });
      expect(second.entries.map((entry) => entry.message.content)).toEqual(
        [50, 51, 52].map((index) => [
          { type: "text", text: `message ${index}` },
        ]),
      );
      expect(second.nextCursor).toBeUndefined();
      const candidate = first.entries.at(-1)?.message;
      if (candidate === undefined) {
        return yield* Effect.dieMessage("missing boundary message");
      }
      const preceding = yield* communication.readHistory({
        members,
        order: "newest",
        limit: 2,
        beforeMessage: {
          authorAgentId: candidate.author.agentId,
          msgId: candidate.msgId,
        },
      });
      expect(preceding.entries.map((entry) => entry.message.content)).toEqual(
        [47, 48].map((index) => [{ type: "text", text: `message ${index}` }]),
      );
      expect(preceding.earlierMessagesOmitted).toBe(true);
      expect(
        yield* communication
          .listConversations({ cursor: first.nextCursor })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "invalid-cursor" });
      expect(
        yield* communication
          .readHistory({
            members: [members[0], agentCardOf(subject.third.card)],
            cursor: first.nextCursor,
          })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "invalid-cursor" });
    }),
  20_000,
);

it("claims one answer when the reply is submitted as an ordinary Message", () =>
  Effect.gen(function* () {
    const subject = yield* conversationIdentityFixture;
    const record = yield* buildCertifiedFirstMessage(
      subject.local,
      subject.remote,
      subject.membership,
      subject.anchor.routerInstanceId,
    );
    yield* subject.run(
      storeCertifiedFirstMessage(
        subject.store,
        subject.local.card,
        subject.membership,
        record,
      ),
    );
    const request = yield* Request.create({
      author: agentCardOf(subject.remote.card),
      recipients: [agentCardOf(subject.local.card)],
      question: "Which day?",
      op: "gather",
      deadline: 30,
      requestedSchema: Schema.decodeUnknownSync(RequestedSchema)({
        type: "object",
        properties: { day: { type: "string" } },
        required: ["day"],
      }),
    });
    yield* subject.run(
      appendCertifiedMessage(
        {
          store: subject.store,
          local: subject.local,
          remote: subject.remote,
          membership: subject.membership,
        },
        record,
        yield* encodeCollectiveContent(
          {
            kind: "operation",
            op: request.op,
            operationId: request.operationId,
            requestedSchema: request.requestedSchema,
            deadlineAt: request.deadlineAt,
          },
          request.question,
        ),
      ),
    );
    const { communication } = yield* openCommunication(
      subject,
      Router.of({ poll: () => Effect.never, send: () => Effect.never }),
    );
    const received = yield* Queue.unbounded<ReceivedRequest>();
    yield* communication
      .consume((incoming) =>
        incoming.kind === "request"
          ? Queue.offer(received, incoming.message).pipe(Effect.asVoid)
          : Effect.void,
      )
      .pipe(Effect.forkScoped);
    const captured = yield* Queue.take(received);
    expect(yield* communication.getRequest(request.operationId)).toEqual(
      captured,
    );
    const first = yield* Answer.create({
      author: agentCardOf(subject.local.card),
      request: captured,
      response: { action: "accept", content: { day: "monday" } },
    });
    yield* communication.send(first);
    yield* communication.send(first);
    const second = yield* Answer.create({
      author: agentCardOf(subject.local.card),
      request: captured,
      response: { action: "decline" },
    });
    expect(yield* communication.send(second).pipe(Effect.flip)).toMatchObject({
      failure: { kind: "request-answered" },
    });
    expect(
      yield* communication.getRequest(request.operationId).pipe(Effect.flip),
    ).toMatchObject({ failure: { kind: "request-answered" } });
  }));
