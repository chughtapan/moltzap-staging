/** @file A host receipt identifies accepted work and its canonical conversation. */
import { Schema } from "effect";
import {
  AgentAddress,
  GroupAddress,
  MsgId,
} from "../communication/records/values.js";
import { CollectiveId } from "../communication/requests/contracts.js";
import { checkedGuidanceSchema } from "../firewall/check.js";

/** Accepted ordinary messages, replies and questions share one receipt shape. */
export const SendResultSchema = Schema.Struct({
  id: Schema.Union(MsgId, CollectiveId),
  address: Schema.Union(AgentAddress, GroupAddress),
  guidance: Schema.optionalWith(checkedGuidanceSchema, { exact: true }),
}).annotations({ identifier: "SendResult" });

/** Accepted work continues independently of the caller's lifetime. */
export type SendResult = typeof SendResultSchema.Type;
