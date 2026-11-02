/** @file SealedBody refuses AgentCards and SignedMessages it cannot read. */

import { Effect, Either } from "effect";
import { expect, it } from "vitest";
import {
  SealedBodyOpeningError,
  SealedBodySealingError,
} from "../sealed-body.js";
import {
  makeGroup,
  openAs,
  plaintext,
  sealFrom,
  signBody,
} from "./sealed-body-fixtures.js";

const throwOnRead = (): never => {
  throw new Error("unreadable");
};

it.each(["AgentIdentityCard", "SignedMessage"] as const)(
  "refuses to open when the %s cannot be read",
  (unreadable) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const group = yield* makeGroup(2);
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
        const throwing = { get: throwOnRead };

        const outcome = yield* (
          unreadable === "AgentIdentityCard"
            ? openAs(
                {
                  ...group.peer,
                  identityCard: new Proxy(group.peer.identityCard, throwing),
                },
                signedMessage,
              )
            : openAs(group.peer, new Proxy(signedMessage, throwing))
        ).pipe(Effect.either);

        expect(outcome).toStrictEqual(
          Either.left(new SealedBodyOpeningError()),
        );
      }),
    ),
);

it("refuses to seal to an AgentIdentityCard that cannot be read", () =>
  Effect.runPromise(
    Effect.gen(function* () {
      const group = yield* makeGroup(2);
      const identityCard = new Proxy(group.peer.identityCard, {
        get: () => {
          throw new Error("unreadable card");
        },
      });

      const outcome = yield* sealFrom(
        group.sender,
        [group.sender, { ...group.peer, identityCard }],
        plaintext,
      ).pipe(Effect.either);

      expect(outcome).toStrictEqual(Either.left(new SealedBodySealingError()));
    }),
  ));
