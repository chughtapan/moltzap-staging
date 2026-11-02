/** @file Opens the sealed outer body of an envelope a test endpoint sent or received. */

import { Effect, type ParseResult, Schema } from "effect";
import type { ClientRepresentationError } from "../encoding/canonical-json.js";
import type { SigningIdentity } from "./certified-history-fixtures.js";
import {
  type DecodedOuterBody,
  decodeOuterBody,
} from "../communication/records/encoding.js";
import {
  SignedMessage,
  type SignedMessageVerificationError,
} from "../identity/envelope/signed-message.js";

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
