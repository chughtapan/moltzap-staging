/** @file Checks one direct message against the contracts an agent holds, from the conversation's history and this firewall's saved checks of it. */

import { Clock, Effect, Schema } from "effect";
import type { Message } from "../communication/index.js";
import type { Outgoing } from "../communication/service.js";
import type { FirewallEvidence } from "./chain.js";
import type { FirewallStore } from "./store.js";
import { decodeJson, sameJson } from "../encoding.js";
import {
  type CheckedMessage,
  checkStructure,
  type CompiledContract,
  ContractProblem,
  formatAct,
  type ProtocolRecord,
  protocolRecordSchema,
  readAct,
} from "./contract.js";
import {
  checkProtocol,
  projectProtocol,
  type ProtocolView,
} from "./protocol.js";

/**
 * What the contracts say of one message. `unconfigured` means none governs
 * its conversation. `ready` means its structure and its place in the protocol
 * hold, and gives the record its check saves. `refused` gives the problem.
 * Each carries the conversation's protocol view and the contracts consulted;
 * `selected` is the contract the message was read under, once one was found.
 */
export type ContractCheck = {
  readonly view: ProtocolView;
  readonly contracts: readonly CompiledContract[];
  readonly selected?: CompiledContract;
} & (
  | { readonly kind: "unconfigured" }
  | {
      readonly kind: "ready";
      readonly contract: CompiledContract;
      readonly record: ProtocolRecord;
    }
  | { readonly kind: "refused"; readonly problem: ContractProblem }
);

/** The key a check record is saved under for its message: the author's agent id and the message id. */
export function messageKey(message: Outgoing): string {
  return `${message.author.agentId}/${message.msgId}`;
}

/**
 * Reads the conversation's protocol from `evidence` and this firewall's saved
 * checks, then checks `message` against the instance in progress or, when none
 * is, against the held contract its act names. History that is incomplete, or
 * a message of it that has no saved check, refuses for unresolved evidence.
 */
export const prepareContract = Effect.fn("prepareContract")(function* (
  contracts: readonly CompiledContract[],
  store: FirewallStore,
  message: Outgoing,
  evidence: FirewallEvidence,
) {
  let view: ProtocolView = {};
  let selected: CompiledContract | undefined;
  const checked = yield* Effect.gen(function* () {
    const history =
      evidence.protocolHistory?.messages ??
      evidence.history.map((entry) => entry.message);
    if (
      evidence.protocolHistory?.complete === false ||
      (evidence.protocolHistory === undefined &&
        (evidence.earlierHistoryOmitted || evidence.gaps.length !== 0))
    ) {
      return yield* missing("The preceding protocol history is incomplete.");
    }
    const checkedHistory = yield* checkedMessages(store, history);
    const now = yield* Clock.currentTimeMillis;
    view = yield* projectProtocol([
      ...checkedHistory,
      ...(evidence.references ?? []).map((reference) => ({
        message: reference,
        at: now,
      })),
    ]);
    if (view.instance === undefined && contracts.length === 0) {
      return { kind: "unconfigured" as const };
    }
    const act = yield* readAct(message);
    const contract =
      view.instance?.contract ??
      contracts.find((held) => held.name === act.norm);
    if (contract === undefined) {
      return yield* new ContractProblem({
        stage: "structural",
        reason: "rule-violation",
        detail: "ontology must name an installed norm.",
        field: "ontology",
      });
    }
    selected = contract;
    const form = yield* checkStructure(contract, act);
    const record = yield* checkProtocol(view, contract, form, {
      ...("replyToMsgId" in message && message.replyToMsgId !== undefined
        ? { replyToMsgId: message.replyToMsgId }
        : {}),
      act,
      author: message.author.agentId,
      now,
    });
    return { kind: "ready" as const, contract, record };
  }).pipe(
    Effect.catchAll((problem) =>
      Effect.succeed({ kind: "refused" as const, problem }),
    ),
  );
  return {
    ...checked,
    view,
    contracts,
    ...(selected === undefined ? {} : { selected }),
  } satisfies ContractCheck;
});

/**
 * The act that tells the sender of a withheld message why, when the contract
 * in force declares one in `:on-refusal` with a `reason` field.
 */
export function contractReply(
  check: ContractCheck,
  message: Outgoing,
  detail: string,
): string | undefined {
  const contract =
    check.kind === "ready" ? check.contract : check.view.instance?.contract;
  const act = contract?.onRefusal;
  if (contract === undefined || act === undefined || "kind" in message) {
    return undefined;
  }
  const form = contract.forms.find((declared) => declared.act === act);
  if (form === undefined || !Object.hasOwn(form.fields.properties, "reason")) {
    return undefined;
  }
  return formatAct({
    name: act,
    norm: contract.name,
    fields: { reason: detail },
  });
}

/**
 * Each message of `history` with this firewall's saved check of exactly that
 * message. A check that allowed it wins over one that did not, since a draft
 * the firewall refused can share its id with the message later sent.
 */
function checkedMessages(store: FirewallStore, history: readonly Message[]) {
  return Effect.gen(function* () {
    const rows = yield* store
      .readCheckRecords(history.map((message) => messageKey(message)))
      .pipe(
        Effect.mapError(() =>
          missing("The saved firewall assessments could not be read."),
        ),
      );
    const checked: CheckedMessage[] = [];
    for (const message of history) {
      const candidates = rows.filter(
        (row) =>
          row.messageKey === messageKey(message) &&
          sameJson(row.message, JSON.stringify(message)),
      );
      const saved =
        candidates.find((row) => row.decision === "allowed") ?? candidates[0];
      if (saved === undefined) {
        return yield* missing(
          "An earlier conversation message has no retained local admission assessment.",
        );
      }
      const protocol =
        saved.decision === "allowed"
          ? yield* savedProtocolRecord(saved.result)
          : undefined;
      checked.push({
        message,
        at: saved.at,
        ...(protocol === undefined ? {} : { protocol }),
      });
    }
    return checked;
  });
}

/** The protocol record a saved check result carries, if its message was an act. */
function savedProtocolRecord(resultJson: string) {
  return Effect.gen(function* () {
    const result = yield* decodeJson(
      Schema.Record({ key: Schema.String, value: Schema.Unknown }),
      resultJson,
    ).pipe(
      Effect.mapError(() =>
        missing("An earlier assessment cannot be decoded."),
      ),
    );
    return result.protocol === undefined
      ? undefined
      : yield* Schema.decodeUnknown(protocolRecordSchema)(result.protocol).pipe(
          Effect.mapError(() =>
            missing("An earlier contract assessment cannot be decoded."),
          ),
        );
  });
}

function missing(detail: string) {
  return new ContractProblem({
    stage: "protocol",
    reason: "unresolved-evidence",
    detail,
  });
}
