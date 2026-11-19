/** @file A conversation's protocol as this firewall saw it: the instance its allowed acts leave running, and whether a further act may follow. */

import { Effect } from "effect";
import {
  type Act,
  type CheckedMessage,
  checkStructure,
  type CompiledContract,
  ContractProblem,
  type MessageForm,
} from "./contract.js";

/**
 * What one firewall's saved checks say of a conversation. `referenceActs`
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

/** Replays a conversation's checked messages in order, each at the time of its own check. Only an act that was allowed and still fits advances the instance. */
export const projectProtocol = Effect.fn("projectProtocol")(function* (
  history: readonly CheckedMessage[],
) {
  let view: ProtocolView = {};
  const seen = new Set<string>();
  const contracts = new Map<string, CompiledContract>();
  for (const entry of history) {
    const key = `${entry.message.author.agentId}/${entry.message.msgId}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    view = yield* replayRecord(view, contracts, entry);
    view = retainInteractionReference(view, entry);
    view = {
      ...view,
      referenceActs: new Map([
        ...(view.referenceActs ?? []),
        [
          entry.message.msgId,
          entry.protocol?.act.name ??
            view.referenceActs?.get(entry.message.msgId),
        ],
      ]),
    };
  }
  return view;
});

/** The protocol record of an act the view permits next, or why the act may not follow. It changes nothing. */
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
    return form.starts
      ? { act, digest: contract.digest, opening: contract }
      : yield* transitionError();
  }
  if (instance.contract.digest !== contract.digest || form.starts) {
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
  return { act, digest: contract.digest };
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
function retainInteractionReference(view: ProtocolView, entry: CheckedMessage) {
  const instance = view.instance;
  if (instance === undefined) {
    return view;
  }
  const reference = entry.message.replyToMsgId;
  const belongs =
    instance.acts[0]?.message.msgId === entry.message.msgId ||
    (reference !== undefined && instance.references.has(reference));
  return belongs
    ? {
        ...view,
        instance: {
          ...instance,
          references: new Map([
            ...instance.references,
            [entry.message.msgId, reference],
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

function replayRecord(
  view: ProtocolView,
  contracts: Map<string, CompiledContract>,
  entry: CheckedMessage,
): Effect.Effect<ProtocolView, ContractProblem> {
  return Effect.gen(function* () {
    const protocol = entry.protocol;
    if (protocol === undefined) {
      return view;
    }
    if (protocol.opening !== undefined) {
      contracts.set(protocol.opening.digest, protocol.opening);
    }
    const contract = contracts.get(protocol.digest);
    const form = contract?.forms.find((item) => item.act === protocol.act.name);
    if (
      contract === undefined ||
      form === undefined ||
      (form.starts && protocol.opening === undefined)
    ) {
      return yield* unavailable(
        "The retained interaction does not contain a consistent opening contract.",
      );
    }
    return yield* checkStructure(contract, protocol.act).pipe(
      Effect.flatMap((shape) =>
        checkProtocol(view, contract, shape, {
          act: protocol.act,
          ...(entry.message.replyToMsgId === undefined
            ? {}
            : { replyToMsgId: entry.message.replyToMsgId }),
          author: entry.message.author.agentId,
          now: entry.at,
        }),
      ),
      Effect.match({
        onFailure: () => view,
        onSuccess: () => advance(view, contract, form, entry),
      }),
    );
  });
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
  const facts =
    form.when.every((fact) => instance.facts.has(fact)) &&
    form.unless.every((fact) => !instance.facts.has(fact));
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
  if (typeof act !== "string" && "fact" in act) {
    const anchor = instance.facts.get(act.fact);
    return anchor === undefined || !ancestry.has(anchor.message.msgId)
      ? undefined
      : anchor;
  }
  const acts = typeof act === "string" ? [act] : act;
  return [...instance.acts]
    .reverse()
    .find(
      (entry) =>
        entry.protocol !== undefined &&
        ancestry.has(entry.message.msgId) &&
        acts.includes(entry.protocol.act.name),
    );
}

function permittedTime(
  instance: Instance,
  delay: NonNullable<MessageForm["delay"]>,
) {
  const fact = instance.facts.get(delay.fact);
  const duration = fact?.protocol?.act.fields[delay.field];
  return fact !== undefined && typeof duration === "number"
    ? fact.at + duration * delay.millisecondsPerUnit
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
    const fields = referencedAct(instance, source.act, ancestry)?.protocol?.act
      .fields;
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
    if (
      typeof keys === "string" ||
      typeof keys === "number" ||
      typeof keys === "boolean"
    ) {
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
