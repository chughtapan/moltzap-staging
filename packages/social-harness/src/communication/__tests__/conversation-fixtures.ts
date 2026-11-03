/** @file Verified participants, SQL and protocol traffic for conversation scenarios. */

import {
  Duration,
  Effect,
  type ParseResult,
  Queue,
  Schedule,
  Schema,
  TestClock,
  TestServices,
} from "effect";
import { generateKeyPairSync } from "node:crypto";
import type { SigningIdentity } from "./records-fixtures.js";
import {
  identifier,
  issueTestCard,
  makeTestAuthority,
} from "../../__tests__/agents.js";
import {
  type ClientRepresentationError,
  decodeCanonical,
} from "../../encoding/canonical-json.js";
import {
  type AgentCard,
  agentCardOf,
  AgentIdentityCard,
} from "../../identity/agent-cards.js";
import { AgentPublicKey } from "../../identity/keys.js";
import {
  SignedMessage,
  type SignedMessageVerificationError,
} from "../../identity/message-security.js";
import { WIRE_VERSION } from "../../version.js";
import {
  type Content,
  messageFromIntent,
  mintMsgId,
  type MsgId,
} from "../messages.js";
import {
  type DecodedOuterBody,
  decodeOuterBody,
  deriveConversationId,
  MembershipDescriptor,
  verifyMembershipDescriptor,
} from "../records.js";
import { RouterInstanceId } from "../router/client.js";
import { pollCursor } from "./router-fixtures.js";
import { openTestDatabase, type TestDatabase } from "./store-fixtures.js";

const anchor = {
  routerInstanceId: Schema.decodeUnknownSync(RouterInstanceId)(
    identifier("rti_", 10),
  ),
  pollCursor: pollCursor(11),
};

/** Verified signers share a Registry authority and one isolated durable store. */
export const conversationIdentityFixture = Effect.gen(function* () {
  const registryKeys = generateKeyPairSync("ed25519");
  const local = yield* signingIdentity(1, "alice", registryKeys);
  const remote = yield* signingIdentity(2, "bob", registryKeys);
  const third = yield* signingIdentity(3, "carol", registryKeys);
  const signer = yield* Schema.decodeUnknown(AgentPublicKey)(
    registryKeys.publicKey.export({ format: "jwk" }),
  );
  const membership = yield* directMembership([local.card, remote.card], signer);
  const { sql, run, store, directory } = yield* openTestDatabase();
  yield* run(store.initializeCommunication(local.card));
  return {
    local,
    remote,
    third,
    membership,
    directory,
    agentKey: directory,
    store,
    sql,
    run,
    registryKeys,
    signer,
    anchor,
  };
}).pipe(Effect.withSpan("conversationIdentityFixture"));

/** The acquired identities and storage remain live for the test scope. */
export type Fixture = Effect.Effect.Success<typeof conversationIdentityFixture>;

/** Each call creates a distinct send unless a retry supplies the same message id. */
export function authoredMessage(
  subject: Pick<Fixture, "local" | "remote">,
  content: Content,
  options: {
    readonly recipients?: readonly AgentCard[];
    readonly msgId?: MsgId;
  } = {},
) {
  return Effect.gen(function* () {
    return yield* messageFromIntent(
      [
        agentCardOf(subject.local.card),
        ...(options.recipients ?? [agentCardOf(subject.remote.card)]),
      ],
      {
        authorAgentId: subject.local.card.agentId,
        msgId: options.msgId ?? (yield* mintMsgId()),
        content,
      },
    );
  }).pipe(Effect.orDie, Effect.withSpan("authoredMessage"));
}

const STEP = Duration.millis(250);

/**
 * Advance the TestClock in 250 ms steps, yielding between them so every fiber
 * a step wakes registers its next sleep before the following step. A yield
 * does not wait for work that settles on real promises, such as sealing and
 * signing an outer envelope; wait for its result with `untilLive` before
 * stepping the clock past a timer that work starts.
 * @param span Total virtual time to pass.
 * @returns Completion once the clock has moved by `span`.
 */
export function advanceClock(
  span: Duration.DurationInput,
): Effect.Effect<void> {
  const steps = Math.ceil(
    Duration.toMillis(Duration.decode(span)) / Duration.toMillis(STEP),
  );
  return Effect.forEach(
    Array.from({ length: steps }),
    () => TestClock.adjust(STEP).pipe(Effect.zipRight(Effect.yieldNow())),
    { concurrency: 1, discard: true },
  );
}

/**
 * Wait in live time until `ready` reads true, polling every 5 ms. A condition
 * that never holds ends at the test's timeout.
 * @param ready Condition the system under test reaches on its own.
 * @returns Completion once `ready` reads true.
 */
export function untilLive(ready: Effect.Effect<boolean>): Effect.Effect<void> {
  return TestServices.provideLive(
    ready.pipe(
      Effect.repeat({
        until: (met) => met,
        schedule: Schedule.spaced("5 millis"),
      }),
    ),
  ).pipe(Effect.asVoid);
}

/**
 * Transmit one outbox row the way the Router worker does: begin it, complete
 * it, and hand its envelope to the Router. A row another transmit already
 * began is inactive and forwards nothing.
 * @param store Store that owns the outbox row.
 * @param outbound Receives each envelope the Router accepts.
 * @param envelopeId Outbox row to transmit.
 * @param transit Time the envelope spends between beginning and completing.
 * @returns Completion once the row is handled.
 */
export function forwardStoredOutbound(
  store: TestDatabase,
  outbound: Queue.Queue<SignedMessage>,
  envelopeId: string,
  transit?: Duration.DurationInput,
): Effect.Effect<void> {
  return store.run(
    store.store.beginOutbound(envelopeId).pipe(
      Effect.flatMap((attempt) => {
        switch (attempt.kind) {
          case "inactive":
            return Effect.void;
          case "pending":
            return decodeCanonical(
              SignedMessage,
              attempt.outbound.canonicalSignedMessage,
            ).pipe(
              Effect.tap(() =>
                transit === undefined ? Effect.void : Effect.sleep(transit),
              ),
              Effect.flatMap((message) =>
                store.store
                  .completeOutbound(attempt.outbound)
                  .pipe(Effect.zipRight(Queue.offer(outbound, message))),
              ),
              Effect.asVoid,
            );
          default: {
            const exhaustive: never = attempt;
            return exhaustive;
          }
        }
      }),
      Effect.orDie,
    ),
  );
}

/**
 * Verify an envelope against its sender's card and open its sealed body as
 * one of its recipients.
 * @param message Envelope as the Router carried it.
 * @param sender Member whose card signed the envelope.
 * @param reader Recipient that opens the body; the sender when omitted, since
 *   every outer body is sealed to its own sender too.
 * @returns The Client value inside the body.
 */
export const openOuterBody = (
  message: SignedMessage,
  sender: SigningIdentity,
  reader: SigningIdentity = sender,
): Effect.Effect<
  DecodedOuterBody,
  | ClientRepresentationError
  | SignedMessageVerificationError
  | ParseResult.ParseError
> =>
  Schema.encode(SignedMessage)(message).pipe(
    Effect.flatMap(Schema.decodeUnknown(SignedMessage.live)),
    Effect.flatMap((signedMessage) =>
      SignedMessage.verify({ signedMessage, identityCard: sender.card }),
    ),
    Effect.flatMap((verified) =>
      decodeOuterBody({
        message: verified,
        identityCard: reader.card,
        signingAuthority: reader.authority,
      }),
    ),
  );

/** One test agent's signing authority and the card the test Registry issued it. */
function signingIdentity(
  byte: number,
  name: string,
  registryKeys: Parameters<typeof issueTestCard>[0]["registryKeys"],
) {
  return Effect.gen(function* () {
    const authority = yield* makeTestAuthority();
    return {
      authority,
      card: yield* issueTestCard({ byte, name, authority, registryKeys }),
    };
  });
}

/** The verified membership of the direct conversation between two cards. */
function directMembership(
  cards: readonly [AgentIdentityCard, AgentIdentityCard],
  signer: AgentPublicKey,
) {
  return Effect.gen(function* () {
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptor)({
      sharnessVersion: WIRE_VERSION,
      kind: "membership_descriptor",
      conversationId: yield* deriveConversationId([
        cards[0].agentId,
        cards[1].agentId,
      ]),
      members: [
        yield* Schema.encode(AgentIdentityCard)(cards[0]),
        yield* Schema.encode(AgentIdentityCard)(cards[1]),
      ],
    });
    return yield* verifyMembershipDescriptor(descriptor, signer);
  });
}
