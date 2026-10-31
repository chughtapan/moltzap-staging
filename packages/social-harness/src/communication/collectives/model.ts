/** @file Questions and result sources retain the messages selected by one collective operation. */

import { Schema } from "effect";
import { exactStruct } from "../../encoding/exact.js";
import { Message, MessageParticipants } from "../message.js";
import { maximumMembers, wellFormedString } from "../records/values.js";
import { DeliveryToken } from "../store/types.js";
import { CollectiveId, EpochMillis } from "./contracts.js";
import { InboundItem } from "./inbound.js";
import { FormModeSchema } from "./wire.js";

/* eslint-disable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Effect Schemas share their names with their decoded types. */

/** The operation is captured before assessment, including its absolute deadline and participants. */
export const Question = MessageParticipants.pipe(
  Schema.extend(
    exactStruct({
      operationId: CollectiveId,
      op: Schema.Literal("gather", "all_gather"),
      text: wellFormedString,
      requestedSchema: FormModeSchema,
      deadlineAt: EpochMillis,
    }),
  ),
);
/** A gather sends a separate request to each recipient; an all_gather shares one group request. */
export type Question = typeof Question.Type;

/** Original request and selected answer messages supply the peer-controlled result text. */
export const ResultSources = exactStruct({
  answers: Schema.Array(Message).pipe(Schema.maxItems(maximumMembers - 1)),
  request: Schema.optionalWith(Message, { exact: true }),
});
/** Own answers remain sources but do not need an inbound policy check. */
export type ResultSources = typeof ResultSources.Type;

/** The item and its sources use one durable result identity through retries and restart. */
export const ResultDelivery = exactStruct({
  operationId: CollectiveId,
  deliveryToken: DeliveryToken,
  item: InboundItem.pipe(
    Schema.filter(
      (
        item,
      ): item is Extract<
        InboundItem,
        { readonly kind: "collectiveResult" | "operationFailed" }
      > => item.kind === "collectiveResult" || item.kind === "operationFailed",
    ),
  ),
  sources: ResultSources,
});
/** Public item bytes and private source bytes are bounded separately. */
export type ResultDelivery = typeof ResultDelivery.Type;

/* eslint-enable @typescript-eslint/naming-convention, @typescript-eslint/no-redeclare -- Restore naming after schema/type pairs. */
