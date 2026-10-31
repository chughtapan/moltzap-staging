/** @file Host send results identify accepted work and carry local advice. */

import { Schema } from "effect";
import { CollectiveId } from "../communication/collectives/contracts.js";
import {
  AgentAddress,
  GroupAddress,
  MsgId,
} from "../communication/records/values.js";
import { checkedGuidanceSchema } from "../firewall/check.js";
/* eslint-disable @typescript-eslint/naming-convention -- Schema values share their boundary type names. */

const guidance = {
  guidance: Schema.optionalWith(checkedGuidanceSchema, { exact: true }),
};

/** Owned fields from a different result kind cannot accompany this result. */
const absentResultField = Schema.Never.annotations({
  identifier: "AbsentSendResultField",
});

/**
 * A successful invocation identifies its operation explicitly. Multicasts
 * and responses name their accepted message and canonical conversation address.
 * Collecting operations name the operation whose result arrives in the inbox.
 * A response also names the request it answered. Refusals use the error channel.
 * Readers may ignore future fields, but fields owned by another result kind
 * cannot describe this result.
 */
export const SendResultSchema = Schema.Union(
  Schema.Struct({
    ...guidance,
    kind: Schema.Literal("multicast"),
    msgId: MsgId,
    address: Schema.Union(AgentAddress, GroupAddress),
    operationId: Schema.optionalWith(absentResultField, { exact: true }),
  }),
  Schema.Struct({
    ...guidance,
    kind: Schema.Literal("response"),
    operationId: CollectiveId,
    msgId: MsgId,
    address: Schema.Union(AgentAddress, GroupAddress),
  }),
  Schema.Struct({
    ...guidance,
    kind: Schema.Literal("collective"),
    operationId: CollectiveId,
    msgId: Schema.optionalWith(absentResultField, { exact: true }),
    address: Schema.optionalWith(absentResultField, { exact: true }),
  }),
).annotations({ identifier: "SendResult" });
/**
 * Successful sends identify accepted work and any local advice.
 * @useDeclaredType
 */
export type SendResult = typeof SendResultSchema.Type;

/* eslint-enable @typescript-eslint/naming-convention -- Restore naming after boundary schema declarations. */
