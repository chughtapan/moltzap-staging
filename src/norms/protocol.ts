/** @file A conversation's protocol as its firewall saw it: the instance its allowed acts leave running, and whether a further act may follow. */

import { Effect } from "effect";
import type { Message } from "../communication/index.js";
import type { EarlierMessage } from "../firewall/index.js";
import {
  type Act,
  checkStructure,
  type CompiledContract,
  ContractProblem,
  type MessageForm,
  readAct,
} from "./contract.js";

/** One act the firewall allowed that fits the protocol: its message, the time of that check and the act its text states. */
interface CheckedMessage {
  readonly message: Message;
  readonly at: number;
  readonly act: Act;
}

/**
 * What one firewall's checks say of a conversation. `referenceActs`
 * gives the performative of each message a reply may name. `instance` is the
 * instance in progress: its contract, who holds each role, the message that
 * set each current fact, how often each act was sent, and the reply reference
 * of every message that belongs to it.
 */
export interface ProtocolView {
  readonly referenceActs?: ReadonlyMap<string, string | undefined>;
  readonly instance?: {
    readonly contract: CompiledContract;
    readonly initiator: string;
    readonly participant: string;
    readonly facts: ReadonlyMap<string, CheckedMessage>;
    readonly counts: ReadonlyMap<string, number>;
    readonly acts: readonly CheckedMessage[];
    readonly references: ReadonlyMap<string, string | undefined>;
  };
}

type Instance = NonNullable<ProtocolView["instance"]>;
interface Candidate {
  readonly replyToMsgId?: string;
  readonly act: Act;
  readonly author: string;
  readonly now: number;
}

/** Fails with why the view does not permit the act next. It changes nothing. */
export const checkProtocol = Effect.fn("checkProtocol")(function* (
  view: ProtocolView,
  contract: CompiledContract,
  form: MessageForm,
  candidate: Candidate,
) {
  const { act, author, now } = candidate;
  const transitionError = () => invalidTransition(view, contract, candidate);
  yield* checkReference(view, contract, form, candidate);
  const instance = view.instance;
  if (instance === undefined) {
    if (!form.starts) {
      return yield* transitionError();
    }
    return;
  }
  if (form.starts) {
    return yield* transitionError();
  }
  const role = roleOf(instance, author);
  const ancestry = referenceAncestry(instance, candidate.replyToMsgId);
  const anchor =
    "recipient" in form.by
      ? instance.facts.get(form.by.recipient.fact)
      : undefined;
  if (
    "recipient" in form.by &&
    (anchor === undefined || !ancestry.has(anchor.message.msgId))
  ) {
    return yield* transitionError();
  }
  if (!senderPermitted(form, author, role, anchor)) {
    return yield* violation(
      `Error: ${act.name} is not permitted for ${contract.name} role ${role ?? "unassigned"}.`,
    );
  }
  if (!enabled(form, instance)) {
    return yield* transitionError();
  }
  if (form.delay !== undefined) {
    yield* checkDelay(instance, form.delay, now).pipe(
      Effect.mapError((error) =>
        error.reason === "rule-violation" ? transitionError() : error,
      ),
    );
  }
  yield* checkRelations(instance, form, act, ancestry).pipe(
    Effect.mapError((error) =>
      error.reason === "rule-violation" ? transitionError() : error,
    ),
  );
});

/**
 * Reads `message` as an act of the instance in progress or, when none is, of
 * the held contract it names.
 */
export const readHeldAct = Effect.fn("readHeldAct")(function* (
  view: ProtocolView,
  contracts: readonly CompiledContract[],
  message: Message,
) {
  const act = yield* readAct(message);
  const contract =
    view.instance?.contract ?? contracts.find((held) => held.name === act.norm);
  if (contract === undefined) {
    return yield* new ContractProblem({
      stage: "structural",
      reason: "rule-violation",
      detail: "ontology must name an installed norm.",
      field: "ontology",
    });
  }
  return { act, contract };
});

/** The form of an act of `message` whose structure fits its contract and that the view permits at `now`. */
export const fitAct = Effect.fn("fitAct")(function* (
  view: ProtocolView,
  { act, contract }: Effect.Effect.Success<ReturnType<typeof readHeldAct>>,
  message: Message,
  now: number,
) {
  const form = yield* checkStructure(contract, act);
  yield* checkProtocol(view, contract, form, {
    ...(message.replyToMsgId === undefined
      ? {}
      : { replyToMsgId: message.replyToMsgId }),
    act,
    author: message.author.agentId,
    now,
  });
  return form;
});

/**
 * Replays a conversation's earlier messages in order under the held
 * `contracts`. Only a message the firewall allowed that reads as an act and
 * fits the protocol at the time of its check advances the instance.
 */
export const projectProtocol = Effect.fn("projectProtocol")(function* (
  contracts: readonly CompiledContract[],
  earlier: readonly EarlierMessage[],
) {
  let view: ProtocolView = {};
  const seen = new Set<string>();
  for (const { message, checkedAt, allowed } of earlier) {
    const key = `${message.author.agentId}/${message.msgId}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    const current = view;
    const fitted =
      !allowed || checkedAt === undefined
        ? undefined
        : yield* readHeldAct(current, contracts, message).pipe(
            Effect.flatMap((held) =>
              Effect.map(fitAct(current, held, message, checkedAt), (form) => ({
                ...held,
                form,
                message,
                at: checkedAt,
              })),
            ),
            Effect.orElseSucceed(() => undefined),
          );
    if (fitted !== undefined) {
      view = advance(view, fitted.contract, fitted.form, fitted);
    }
    view = retainInteractionReference(view, message);
    view = {
      ...view,
      referenceActs: new Map([
        ...(view.referenceActs ?? []),
        [
          message.msgId,
          fitted?.act.name ?? view.referenceActs?.get(message.msgId),
        ],
      ]),
    };
  }
  return view;
});

function senderPermitted(
  form: MessageForm,
  author: string,
  role?: string,
  anchor?: CheckedMessage,
) {
  if (role === undefined) {
    return false;
  }
  return "recipient" in form.by
    ? anchor?.message.recipients[0]?.agentId === author
    : form.by.includes(role);
}

/** A message that replies into the instance belongs to it, whether or not it was allowed, so a later reply may name it. */
function retainInteractionReference(view: ProtocolView, message: Message) {
  const instance = view.instance;
  if (instance === undefined) {
    return view;
  }
  const reference = message.replyToMsgId;
  const belongs =
    instance.acts[0]?.message.msgId === message.msgId ||
    (reference !== undefined && instance.references.has(reference));
  return belongs
    ? {
        ...view,
        instance: {
          ...instance,
          references: new Map([
            ...instance.references,
            [message.msgId, reference],
          ]),
        },
      }
    : view;
}

/** The refusal of an act that may not follow, naming the act its reply reference points at when that is known. */
function invalidTransition(
  view: ProtocolView,
  contract: CompiledContract,
  candidate: Candidate,
) {
  const preceding =
    candidate.replyToMsgId === undefined
      ? undefined
      : view.referenceActs?.get(candidate.replyToMsgId);
  const after = preceding === undefined ? "" : ` after ${preceding}`;
  return violation(
    `Error: ${candidate.act.name} is not valid${after} for ${contract.name}.`,
  );
}

function checkReference(
  view: ProtocolView,
  contract: CompiledContract,
  form: MessageForm,
  candidate: Candidate,
) {
  return Effect.gen(function* () {
    const instance = view.instance;
    const { replyToMsgId } = candidate;
    if (replyToMsgId === undefined) {
      if (!form.starts) {
        return yield* violation(
          "Error: replyToMsgId is required for a continuation act.",
          "replyToMsgId",
        );
      }
      return;
    }
    const known =
      (view.referenceActs?.has(replyToMsgId) ?? false) ||
      instance?.references.has(replyToMsgId);
    if (!known) {
      return yield* violation(
        "Error: replyToMsgId does not identify a preceding message in this conversation.",
        "replyToMsgId",
      );
    }
    if (
      !form.starts &&
      instance !== undefined &&
      !instance.references.has(replyToMsgId)
    ) {
      return yield* invalidTransition(view, contract, candidate);
    }
  });
}

function checkDelay(
  instance: Instance,
  delay: NonNullable<MessageForm["delay"]>,
  now: number,
): Effect.Effect<void, ContractProblem> {
  const permittedAt = permittedTime(instance, delay);
  if (permittedAt === undefined) {
    return Effect.fail(
      unavailable("The declared delay has no retained numeric anchor."),
    );
  }
  return now < permittedAt
    ? Effect.fail(
        violation(
          `This act is permitted at or after ${permittedAt} Unix milliseconds. The host may schedule the next attempt.`,
        ),
      )
    : Effect.void;
}

function checkRelations(
  instance: Instance,
  form: MessageForm,
  act: Act,
  ancestry: ReadonlySet<string>,
) {
  return Effect.gen(function* () {
    for (const relation of form.relations) {
      const expected = yield* sourceValue(instance, relation.source, ancestry);
      const actual = act.fields[relation.field];
      if (
        actual === undefined ||
        !related(relation.operator, actual, expected)
      ) {
        return yield* violation(
          `The field does not satisfy its declared ${relation.operator} relation to preceding messages.`,
          relation.field,
        );
      }
    }
  });
}

/** The view once `entry`, an act the protocol permits, is applied: it opens an instance, changes the one in progress or ends it. */
function advance(
  view: ProtocolView,
  contract: CompiledContract,
  form: MessageForm,
  entry: CheckedMessage,
): ProtocolView {
  if (form.ends) {
    return { ...view, instance: undefined };
  }
  const prior: Instance = view.instance ?? {
    contract,
    initiator: entry.message.author.agentId,
    participant: entry.message.recipients[0].agentId,
    facts: new Map(),
    counts: new Map(),
    acts: [],
    references: new Map(),
  };
  const facts = new Map(prior.facts);
  const counts = new Map(prior.counts);
  for (const fact of form.terminates) {
    facts.delete(fact);
  }
  for (const fact of form.initiates) {
    facts.set(fact, entry);
  }
  counts.set(form.act, (counts.get(form.act) ?? 0) + 1);
  return {
    ...view,
    instance: { ...prior, facts, counts, acts: [...prior.acts, entry] },
  };
}

function roleOf(instance: Instance, author: string) {
  if (author === instance.initiator) {
    return instance.contract.roles.initiator;
  }
  return author === instance.participant
    ? instance.contract.roles.participant
    : undefined;
}

function enabled(form: MessageForm, instance: Instance) {
  const facts = form.when.every((fact) => instance.facts.has(fact));
  const count =
    form.maximum === undefined ||
    (instance.counts.get(form.act) ?? 0) < form.maximum;
  return facts && count;
}

/** The messages a reply reference leads back through, within the instance. */
function referenceAncestry(instance: Instance, reference?: string) {
  const ancestry = new Set<string>();
  let current = reference;
  while (
    current !== undefined &&
    !ancestry.has(current) &&
    instance.references.has(current)
  ) {
    ancestry.add(current);
    current = instance.references.get(current);
  }
  return ancestry;
}

function referencedAct(
  instance: Instance,
  act: MessageForm["relations"][number]["source"]["act"],
  ancestry: ReadonlySet<string>,
) {
  if (typeof act !== "string") {
    const anchor = instance.facts.get(act.fact);
    return anchor === undefined || !ancestry.has(anchor.message.msgId)
      ? undefined
      : anchor;
  }
  return [...instance.acts]
    .reverse()
    .find(
      (entry) => ancestry.has(entry.message.msgId) && entry.act.name === act,
    );
}

function permittedTime(
  instance: Instance,
  delay: NonNullable<MessageForm["delay"]>,
) {
  const fact = instance.facts.get(delay.fact);
  const duration = fact?.act.fields[delay.field];
  return fact !== undefined && typeof duration === "number"
    ? fact.at + duration * 1000
    : undefined;
}

function related(
  operator: MessageForm["relations"][number]["operator"],
  actual: Act["fields"][string],
  expected: Act["fields"][string],
) {
  switch (operator) {
    case "equals":
      return JSON.stringify(actual) === JSON.stringify(expected);
    case "member":
      return (
        Array.isArray(expected) && expected.some((item) => item === actual)
      );
    case "subset":
      return (
        Array.isArray(actual) &&
        Array.isArray(expected) &&
        actual.every((item) => expected.includes(item))
      );
    default: {
      const exhaustive: never = operator;
      return exhaustive;
    }
  }
}

function sourceValue(
  instance: Instance,
  source: MessageForm["relations"][number]["source"],
  ancestry: ReadonlySet<string>,
): Effect.Effect<Act["fields"][string], ContractProblem> {
  return Effect.gen(function* () {
    const fields = referencedAct(instance, source.act, ancestry)?.act.fields;
    if (fields === undefined) {
      return yield* violation(
        "The referenced exchange does not contain the declared source act.",
      );
    }
    if (source.kind === "field") {
      const value = fields[source.field];
      if (value === undefined) {
        return yield* unavailable(
          "A declared relation needs an earlier field that is missing.",
        );
      }
      return value;
    }
    if (source.kind === "present") {
      return source.fields.filter((field) => Object.hasOwn(fields, field));
    }
    const keys =
      source.kind === "field-values"
        ? source.fields.filter((field) => Object.hasOwn(fields, field))
        : yield* sourceValue(instance, source.keys, ancestry);
    if (typeof keys === "string" || typeof keys === "number") {
      return yield* unavailable(
        "A selected-values relation needs a list of field names.",
      );
    }
    return yield* Effect.forEach(
      keys,
      (key) => {
        const value = fields[key];
        return typeof value === "string"
          ? Effect.succeed(value)
          : Effect.fail(
              unavailable("A selected value is missing or is not a string."),
            );
      },
      { concurrency: 1 },
    );
  });
}

function violation(detail: string, field?: string) {
  return new ContractProblem({
    stage: "protocol",
    reason: "rule-violation",
    detail,
    ...(field === undefined ? {} : { field }),
  });
}

function unavailable(detail: string) {
  return new ContractProblem({
    stage: "protocol",
    reason: "unresolved-evidence",
    detail,
  });
}
