/** @file Identity entry points fail with their typed error when a member of their input record throws on read. */

import { Effect, Either } from "effect";
import { expect, it } from "vitest";
import {
  envelopeIdOf,
  type Group,
  makeGroup,
  plaintext,
  sealFrom,
  signBody,
} from "../envelope/__tests__/sealed-body-fixtures.js";
import { SealedBody } from "../envelope/sealed-body.js";
import { SignedMessage } from "../envelope/signed-message.js";

/** A view of `value` whose `member` throws on read. */
const throwingOn = <Value extends object>(
  value: Value,
  member: string,
): Value =>
  new Proxy(value, {
    get: (target, key): unknown => {
      if (key === member) {
        throw new Error(`unreadable ${member}`);
      }
      return Reflect.get(target, key);
    },
  });

/** The tag of the entry point's failure, or `success`. */
const failureTag = (
  outcome: Either.Either<unknown, { readonly _tag: string }>,
): string =>
  Either.match(outcome, {
    onLeft: (failure) => failure._tag,
    onRight: () => "success",
  });

/** One entry point, called with an input whose `member` throws on read. */
interface EntryPointCase {
  readonly entryPoint: string;
  readonly member: string;
  readonly expected: string;
  readonly run: (
    group: Group,
    member: string,
  ) => Effect.Effect<unknown, { readonly _tag: string }>;
}

it.each<EntryPointCase>([
  {
    entryPoint: "SealedBody.seal",
    member: "recipientIdentityCards",
    expected: "SealedBodySealingError",
    run: (group: Group, member: string) =>
      SealedBody.seal(
        throwingOn(
          {
            senderAgentId: group.sender.identityCard.agentId,
            recipientIdentityCards: group.recipients.map((r) => r.identityCard),
            envelopeId: envelopeIdOf(1),
            plaintext,
          },
          member,
        ),
      ),
  },
  {
    entryPoint: "SealedBody.open",
    member: "signedMessage",
    expected: "SealedBodyOpeningError",
    run: (group: Group, member: string) =>
      Effect.gen(function* () {
        const sealed = yield* sealFrom(
          group.sender,
          group.recipients,
          plaintext,
        );
        const signedMessage = yield* signBody(
          group.sender,
          group.recipients,
          sealed,
        );
        return yield* SealedBody.open(
          throwingOn(
            {
              identityCard: group.peer.identityCard,
              signingAuthority: group.peer.authority,
              signedMessage,
            },
            member,
          ),
        );
      }),
  },
  {
    entryPoint: "SignedMessage.sign",
    member: "body",
    expected: "SignedMessageSigningError",
    run: (group: Group, member: string) =>
      SignedMessage.sign(
        throwingOn(
          {
            identityCard: group.sender.identityCard,
            signingAuthority: group.sender.authority,
            recipientAgentIds: new Set([group.peer.identityCard.agentId]),
            envelopeId: envelopeIdOf(1),
            body: plaintext,
          },
          member,
        ),
      ),
  },
  {
    entryPoint: "SignedMessage.verify",
    member: "identityCard",
    expected: "SignedMessageVerificationError",
    run: (group: Group, member: string) =>
      Effect.gen(function* () {
        const signedMessage = yield* signBody(
          group.sender,
          group.recipients,
          plaintext,
        );
        return yield* SignedMessage.verify(
          throwingOn(
            { signedMessage, identityCard: group.sender.identityCard },
            member,
          ),
        );
      }),
  },
])(
  "$entryPoint fails with $expected when its input's $member throws on read",
  ({ member, expected, run }) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(2);

        const outcome = yield* run(group, member).pipe(Effect.either);

        expect(failureTag(outcome)).toBe(expected);
      }),
    ),
);
