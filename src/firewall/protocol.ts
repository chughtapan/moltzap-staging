/** @file Projects locally admitted conversations and checks declared transitions. */

import { Effect, Either } from "effect";
import {
  type AdmittedAct,
  type CompiledContract,
  ContractProblem,
  type MessageForm,
  type StructuredAct,
} from "./contract.js";
import { checkStructure } from "./structure.js";

/** The projection is local policy context, not shared delivery or agent execution state. */
export interface ProtocolView {
  readonly referenceActs?: ReadonlyMap<string, string | undefined>;
  readonly active?: {
    readonly contract: CompiledContract;
    readonly initiator: string;
    readonly participant: string;
    readonly facts: ReadonlyMap<string, AdmittedAct>;
    readonly counts: ReadonlyMap<string, number>;
    readonly acts: readonly AdmittedAct[];
    readonly references: ReadonlyMap<string, string | undefined>;
  };
}

type Active = NonNullable<ProtocolView["active"]>;
interface Candidate {
  readonly replyToMsgId?: string;
  readonly act: StructuredAct;
  readonly author: string;
  readonly now: number;
}

/** Retained order resolves concurrent assessments using their original times, without new inference. */
export const projectProtocol = Effect.fn("projectProtocol")(function* (
  history: readonly AdmittedAct[],
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

/** An allowed transition describes a candidate; it does not mutate the retained conversation. */
export const checkProtocol = Effect.fn("checkProtocol")(function* (
  view: ProtocolView,
  contract: CompiledContract,
  form: MessageForm,
  candidate: Candidate,
) {
  const { act, author, now } = candidate;
  const transitionError = () => invalidTransition(view, contract, candidate);
  yield* checkReference(view, contract, form, candidate);
  const active = view.active;
  if (active === undefined) {
    yield* checkOpening(contract, form).pipe(
      Effect.mapError((error) =>
        error.reason === "rule-violation" ? transitionError() : error,
      ),
    );
    return { act, digest: contract.digest, opening: contract };
  }
  if (active.contract.digest !== contract.digest || form.starts) {
    return yield* transitionError();
  }
  const role = roleOf(active, author);
  const ancestry = referenceAncestry(active, candidate.replyToMsgId);
  const anchor =
    "recipient" in form.by
      ? active.facts.get(form.by.recipient.fact)
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
  if (!enabled(form, active)) {
    return yield* transitionError();
  }
  if (form.delay !== undefined) {
    yield* checkDelay(active, form.delay, now).pipe(
      Effect.mapError((error) =>
        error.reason === "rule-violation" ? transitionError() : error,
      ),
    );
  }
  yield* checkRelations(active, form, act, ancestry).pipe(
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
  anchor?: AdmittedAct,
) {
  if (role === undefined) {
    return false;
  }
  return "recipient" in form.by
    ? anchor?.message.recipients[0]?.agentId === author
    : form.by.includes(role);
}

/** Withheld messages inherit their referenced interaction without advancing it. */
function retainInteractionReference(view: ProtocolView, entry: AdmittedAct) {
  const active = view.active;
  if (active === undefined) {
    return view;
  }
  const reference = entry.message.replyToMsgId;
  const belongs =
    active.acts[0]?.message.msgId === entry.message.msgId ||
    (reference !== undefined && active.references.has(reference));
  return belongs
    ? {
        ...view,
        active: {
          ...active,
          references: new Map([
            ...active.references,
            [entry.message.msgId, reference],
          ]),
        },
      }
    : view;
}

/** Describes the supplied reference without selecting a replacement. */
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
  entry: AdmittedAct,
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
    const checked = yield* checkStructure(contract, protocol.act).pipe(
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
      Effect.either,
    );
    return yield* Either.match(checked, {
      onLeft: () => Effect.succeed(view),
      onRight: () => advance(view, contract, form, entry),
    });
  });
}

function checkReference(
  view: ProtocolView,
  contract: CompiledContract,
  form: MessageForm,
  candidate: Candidate,
) {
  return Effect.gen(function* () {
    const active = view.active;
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
      active?.references.has(replyToMsgId);
    if (!known) {
      return yield* violation(
        "Error: replyToMsgId does not identify a preceding message in this conversation.",
        "replyToMsgId",
      );
    }
    if (
      !form.starts &&
      active !== undefined &&
      !active.references.has(replyToMsgId)
    ) {
      return yield* invalidTransition(view, contract, candidate);
    }
  });
}

function checkOpening(
  contract: CompiledContract,
  form: MessageForm,
): Effect.Effect<void, ContractProblem> {
  if (
    !form.starts ||
    "recipient" in form.by ||
    !form.by.includes(contract.roles.initiator)
  ) {
    return Effect.fail(
      violation(`Error: ${form.act} is not valid for ${contract.name}.`),
    );
  }
  const needsHistory =
    form.when.length !== 0 ||
    form.delay !== undefined ||
    form.relations.length !== 0;
  return needsHistory
    ? Effect.fail(
        violation("This opening act requires earlier interaction evidence."),
      )
    : Effect.void;
}

function checkDelay(
  active: Active,
  delay: NonNullable<MessageForm["delay"]>,
  now: number,
): Effect.Effect<void, ContractProblem> {
  const permittedAt = permittedTime(active, delay);
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
  active: Active,
  form: MessageForm,
  act: StructuredAct,
  ancestry: ReadonlySet<string>,
) {
  return Effect.gen(function* () {
    for (const relation of form.relations) {
      const expected = yield* sourceValue(active, relation.source, ancestry);
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

function advance(
  view: ProtocolView,
  contract: CompiledContract,
  form: MessageForm,
  entry: AdmittedAct,
): Effect.Effect<ProtocolView, ContractProblem> {
  const peer = entry.message.recipients[0];
  const act = entry.protocol?.act;
  if (
    peer === undefined ||
    act === undefined ||
    (!form.starts && view.active === undefined)
  ) {
    return Effect.fail(
      unavailable("The retained interaction has no opening participants."),
    );
  }
  if (form.ends) {
    return Effect.succeed({ ...view, active: undefined });
  }
  const prior: Active = view.active ?? {
    contract,
    initiator: entry.message.author.agentId,
    participant: peer.agentId,
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
  return Effect.succeed({
    ...view,
    active: { ...prior, facts, counts, acts: [...prior.acts, entry] },
  });
}

function roleOf(active: Active, author: string) {
  if (author === active.initiator) {
    return active.contract.roles.initiator;
  }
  return author === active.participant
    ? active.contract.roles.participant
    : undefined;
}

function enabled(form: MessageForm, active: Active) {
  const facts =
    form.when.every((fact) => active.facts.has(fact)) &&
    form.unless.every((fact) => !active.facts.has(fact));
  const count =
    form.maximum === undefined ||
    (active.counts.get(form.act) ?? 0) < form.maximum;
  return facts && count;
}

/** The supplied reply determines the exchange; no conversation-wide parent is selected. */
function referenceAncestry(active: Active, reference?: string) {
  const ancestry = new Set<string>();
  let current = reference;
  while (
    current !== undefined &&
    !ancestry.has(current) &&
    active.references.has(current)
  ) {
    ancestry.add(current);
    current = active.references.get(current);
  }
  return ancestry;
}

function referencedAct(
  active: Active,
  act: MessageForm["relations"][number]["source"]["act"],
  ancestry: ReadonlySet<string>,
) {
  if (typeof act !== "string" && "fact" in act) {
    const anchor = active.facts.get(act.fact);
    return anchor === undefined || !ancestry.has(anchor.message.msgId)
      ? undefined
      : anchor;
  }
  const acts = typeof act === "string" ? [act] : act;
  return [...active.acts]
    .reverse()
    .find(
      (entry) =>
        entry.protocol !== undefined &&
        ancestry.has(entry.message.msgId) &&
        acts.includes(entry.protocol.act.name),
    );
}

function permittedTime(
  active: Active,
  delay: NonNullable<MessageForm["delay"]>,
) {
  const fact = active.facts.get(delay.fact);
  const duration = fact?.protocol?.act.fields[delay.field];
  return fact !== undefined && typeof duration === "number"
    ? fact.at + duration * delay.millisecondsPerUnit
    : undefined;
}

function related(
  operator: MessageForm["relations"][number]["operator"],
  actual: StructuredAct["fields"][string],
  expected: StructuredAct["fields"][string],
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
  active: Active,
  source: MessageForm["relations"][number]["source"],
  ancestry: ReadonlySet<string>,
): Effect.Effect<StructuredAct["fields"][string], ContractProblem> {
  return Effect.gen(function* () {
    const fields = referencedAct(active, source.act, ancestry)?.protocol?.act
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
        : yield* sourceValue(active, source.keys, ancestry);
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
