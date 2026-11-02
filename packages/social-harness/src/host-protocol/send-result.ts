/** @file A host receipt identifies accepted work and its canonical conversation. */

import { Schema } from "effect";
import { CollectiveId } from "../communication/index.js";
import {
  AgentAddress,
  GroupAddress,
  MsgId,
} from "../communication/records/values.js";
import { checkedGuidanceSchema } from "../firewall/check.js";

/* eslint-disable @typescript-eslint/naming-convention -- Effect schemas share the public names of their derived value types. */

/** Accepted ordinary messages, replies and questions share one receipt shape. */
export const SendResultSchema = Schema.Struct({
  id: Schema.Union(MsgId, CollectiveId),
  address: Schema.Union(AgentAddress, GroupAddress),
  guidance: Schema.optionalWith(checkedGuidanceSchema, { exact: true }),
}).annotations({ identifier: "SendResult" });

/** Accepted work continues independently of the caller's lifetime. */
export type SendResult = typeof SendResultSchema.Type;

/* eslint-enable @typescript-eslint/naming-convention -- Restore ordinary symbol naming. */
