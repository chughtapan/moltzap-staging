/**
 * @file The structure check of a firewall whose agent holds norms. It replays
 * a conversation's earlier messages to find the instance in progress, then
 * says whether a further message is an act that may follow.
 */

import { Clock, Effect, Either, Struct } from "effect";
import type { Message } from "../communication/index.js";
import {
  type EarlierMessage,
  type StructureCheck,
  TextRule,
} from "../firewall/index.js";
import {
  type Act,
  checkStructure,
  formatAct,
  type InstalledNorms,
  type MessageForm,
  type Norm,
  type NormProblem,
  readAct,
} from "./norm.js";

/** One act the firewall allowed that fits the protocol: its message, the time of that check and the act its text states. */
interface CheckedMessage {
  readonly message: Message;
  readonly at: number;
  readonly act: Act;
}

/**
 * What one firewall's checks say of a conversation. `referenceActs`
 * gives the performative of each message a reply may name. `instance` is the
 * instance in progress: its norm, who holds each role, the message that
 * set each current fact, how often each act was sent, and the reply reference
 * of every message that belongs to it.
 */
interface ProtocolView {
  readonly referenceActs?: ReadonlyMap<string, string | undefined>;
  readonly instance?: {
    readonly norm: Norm;
    readonly initiator: string;
    readonly participant: string;
    readonly facts: ReadonlyMap<string, CheckedMessage>;
    readonly counts: ReadonlyMap<string, number>;
    readonly acts: readonly CheckedMessage[];
    readonly references: ReadonlyMap<string, string | undefined>;
  };
}

type Instance = NonNullable<ProtocolView["instance"]>;
type Checked<Value> = Either.Either<Value, NormProblem>;

/** An act read from a message, with the norm it is held to. */
interface HeldAct {
  readonly act: Act;
  readonly norm: Norm;
}

/** A held act whose fields fit `form`, the form its norm declares for it. */
interface FittedAct extends HeldAct {
  readonly form: MessageForm;
}

/**
 * The structure check of a firewall whose agent holds `installed`. It replays
 * the earlier messages the firewall allowed to find the instance in progress,
 * then reads the message as an act of that instance or, when none is, of the
 * norm it names. An act that fits passes with its norm's English rules for
 * it. Any other message is refused under a rule named for the norm of the
 * instance in progress and the stage that failed. Without norms every message
 * passes.
 */
export function structureCheck(installed: InstalledNorms): StructureCheck {
  return Effect.fn("structureCheck")(function* (message, earlier) {
    return installed.length === 0
      ? { kind: "passed", textRules: [] }
      : checkMessage(
          installed,
          message,
          earlier,
          yield* Clock.currentTimeMillis,
        );
  });
}

/** What the structure check says of `message` at `now`, after the `earlier` messages of its conversation. */
function checkMessage(
  installed: InstalledNorms,
  message: Message,
  earlier: readonly EarlierMessage[],
  now: number,
): Effect.Effect.Success<ReturnType<StructureCheck>> {
  const view = projectProtocol(installed, earlier);
  const held = readHeldAct(view, installed, message);
  return Either.match(
    Either.flatMap(held, (read) => fitAct(view, read, message, now)),
    {
      onLeft: (problem) => {
        const selected = Either.getOrUndefined(held)?.norm;
        const reply = refusalReply(view.instance?.norm)?.(problem.detail);
        return {
          kind: "refused",
          reason: problem.reason,
          ruleId: `${view.instance?.norm.name ?? "norm"}-${problem.stage}`,
          feedback: problem.detail,
          detail: {
            ...(selected === undefined
              ? {}
              : { norm: selected.name, digest: selected.digest }),
            stage: problem.stage,
            explanation: problem.detail,
            ...(problem.field === undefined ? {} : { field: problem.field }),
          },
          ...(reply === undefined ? {} : { reply }),
        };
      },
      onRight: ({ act, norm, form }) => {
        const named = { norm: norm.name, digest: norm.digest };
        const replyTo = refusalReply(norm);
        return {
          kind: "passed",
          textRules: norm.textRules
            .filter((rule) => rule.act === undefined || rule.act === act.name)
            .map(
              (rule) =>
                new TextRule({ ...Struct.omit(rule, "act"), applies: "both" }),
            ),
          protocol: {
            ...named,
            act: act.name,
            description: norm.description,
            state: [...(view.instance?.facts.keys() ?? [])],
            form: form.fields,
          },
          detail: { ...named, act: act.name, stage: "semantic" },
          ...(replyTo === undefined ? {} : { replyTo }),
        };
      },
    },
  );
}

/**
 * Phrases the act that tells the sender of a withheld message why, when
 * `norm` declares one in `:on-refusal` with a `reason` field.
 */
function refusalReply(norm?: Norm) {
  const act = norm?.onRefusal;
  const form = norm?.forms.find((declared) => declared.act === act);
  if (
    norm === undefined ||
    act === undefined ||
    form?.fields.every((field) => field.name !== "reason") !== false
  ) {
    return undefined;
  }
  return (reason: string) =>
    formatAct({ name: act, norm: norm.name, fields: { reason } });
}

/**
 * Replays a conversation's earlier messages in order under the held norms.
 * Only a message the firewall allowed that reads as an act and fits the
 * protocol at the time of its check advances the instance.
 */
function projectProtocol(
  installed: InstalledNorms,
  earlier: readonly EarlierMessage[],
): ProtocolView {
  let view: ProtocolView = {};
  const seen = new Set<string>();
  for (const entry of earlier) {
    const { message } = entry;
    const key = `${message.author.agentId}/${message.msgId}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    const act = replayed(view, installed, entry);
    if (act !== undefined) {
      view = advance(view, act);
    }
    view = retainInteractionReference(view, message);
    view = {
      ...view,
      referenceActs: new Map([
        ...(view.referenceActs ?? []),
        [
          message.msgId,
          act?.act.name ?? view.referenceActs?.get(message.msgId),
        ],
      ]),
    };
  }
  return view;
}

/**
 * The act of an earlier message, as the view permitted it at the time of the
 * firewall's check, or nothing when the firewall did not allow the message or
 * the message is no such act.
 */
function replayed(
  view: ProtocolView,
  installed: InstalledNorms,
  { message, checkedAt, allowed }: EarlierMessage,
) {
  return !allowed || checkedAt === undefined
    ? undefined
    : readHeldAct(view, installed, message).pipe(
        Either.flatMap((held) => fitAct(view, held, message, checkedAt)),
        Either.map((fitted) => ({ ...fitted, message, at: checkedAt })),
        Either.getOrUndefined,
      );
}

/**
 * Reads `message` as an act of the instance in progress or, when none is, of
 * the held norm it names.
 */
function readHeldAct(
  view: ProtocolView,
  installed: InstalledNorms,
  message: Message,
): Checked<HeldAct> {
  return Either.flatMap(readAct(message), (act) => {
    const norm =
      view.instance?.norm ?? installed.find((held) => held.name === act.norm);
    return norm === undefined
      ? Either.left({
          stage: "structural",
          reason: "rule-violation",
          detail: "ontology must name an installed norm.",
          field: "ontology",
        })
      : Either.right({ act, norm });
  });
}

/** The act of `message` with its form, once its structure fits its norm and the view permits it at `now`. */
function fitAct(
  view: ProtocolView,
  held: HeldAct,
  message: Message,
  now: number,
): Checked<FittedAct> {
  return Either.flatMap(checkStructure(held.norm, held.act), (form) => {
    const fitted = { ...held, form };
    return Either.map(checkProtocol(view, fitted, message, now), () => fitted);
  });
}

/** Why the view does not permit `act`, stated by `message`, next. */
function checkProtocol(
  view: ProtocolView,
  { act, norm, form }: FittedAct,
  message: Message,
  now: number,
): Either.Either<void, NormProblem> {
  const invalid = invalidTransition(view, norm, message, act);
  const orInvalid = (problem: NormProblem) =>
    problem.reason === "rule-violation" ? invalid : problem;
  return Either.gen(function* () {
    yield* checkReference(view, form, message, invalid);
    const instance = view.instance;
    if (instance === undefined || form.starts) {
      return instance === undefined && form.starts
        ? undefined
        : yield* Either.left(invalid);
    }
    const author = message.author.agentId;
    const role = roleOf(instance, author);
    const ancestry = referenceAncestry(instance, message.replyToMsgId);
    const anchor =
      "recipient" in form.by
        ? instance.facts.get(form.by.recipient.fact)
        : undefined;
    if (
      "recipient" in form.by &&
      (anchor === undefined || !ancestry.has(anchor.message.msgId))
    ) {
      return yield* Either.left(invalid);
    }
    if (!senderPermitted(form, author, role, anchor)) {
      return yield* Either.left(
        violation(
          `Error: ${act.name} is not permitted for ${norm.name} role ${role ?? "unassigned"}.`,
        ),
      );
    }
    if (!enabled(form, instance)) {
      return yield* Either.left(invalid);
    }
    if (form.delay !== undefined) {
      yield* Either.mapLeft(checkDelay(instance, form.delay, now), orInvalid);
    }
    return yield* Either.mapLeft(
      checkRelations(instance, form, act, ancestry),
      orInvalid,
    );
  });
}

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
  norm: Norm,
  message: Message,
  act: Act,
) {
  const preceding =
    message.replyToMsgId === undefined
      ? undefined
      : view.referenceActs?.get(message.replyToMsgId);
  const after = preceding === undefined ? "" : ` after ${preceding}`;
  return violation(`Error: ${act.name} is not valid${after} for ${norm.name}.`);
}

/** Whether the message's reply reference suits `form`; `invalid` refuses a continuation that replies outside the instance. */
function checkReference(
  view: ProtocolView,
  form: MessageForm,
  { replyToMsgId }: Message,
  invalid: NormProblem,
): Either.Either<void, NormProblem> {
  const instance = view.instance;
  if (replyToMsgId === undefined) {
    return form.starts
      ? Either.void
      : Either.left(
          violation(
            "Error: replyToMsgId is required for a continuation act.",
            "replyToMsgId",
          ),
        );
  }
  const known =
    (view.referenceActs?.has(replyToMsgId) ?? false) ||
    instance?.references.has(replyToMsgId);
  if (!known) {
    return Either.left(
      violation(
        "Error: replyToMsgId does not identify a preceding message in this conversation.",
        "replyToMsgId",
      ),
    );
  }
  return !form.starts &&
    instance !== undefined &&
    !instance.references.has(replyToMsgId)
    ? Either.left(invalid)
    : Either.void;
}

function checkDelay(
  instance: Instance,
  delay: NonNullable<MessageForm["delay"]>,
  now: number,
): Either.Either<void, NormProblem> {
  const permittedAt = permittedTime(instance, delay);
  if (permittedAt === undefined) {
    return Either.left(
      unavailable("The declared delay has no retained numeric anchor."),
    );
  }
  return now < permittedAt
    ? Either.left(
        violation(
          `This act is permitted at or after ${permittedAt} Unix milliseconds. The host may schedule the next attempt.`,
        ),
      )
    : Either.void;
}

function checkRelations(
  instance: Instance,
  form: MessageForm,
  act: Act,
  ancestry: ReadonlySet<string>,
): Either.Either<void, NormProblem> {
  return Either.gen(function* () {
    for (const relation of form.relations) {
      const expected = yield* sourceValue(instance, relation.source, ancestry);
      const actual = act.fields[relation.field];
      if (
        actual === undefined ||
        !related(relation.operator, actual, expected)
      ) {
        return yield* Either.left(
          violation(
            `The field does not satisfy its declared ${relation.operator} relation to preceding messages.`,
            relation.field,
          ),
        );
      }
    }
  });
}

/** The view once `entry`, an act the protocol permits, is applied: it opens an instance, changes the one in progress or ends it. */
function advance(
  view: ProtocolView,
  entry: CheckedMessage & FittedAct,
): ProtocolView {
  const { norm, form } = entry;
  if (form.ends) {
    return { ...view, instance: undefined };
  }
  const prior: Instance = view.instance ?? {
    norm,
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
    return instance.norm.roles.initiator;
  }
  return author === instance.participant
    ? instance.norm.roles.participant
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
): Checked<Act["fields"][string]> {
  const fields = referencedAct(instance, source.act, ancestry)?.act.fields;
  if (fields === undefined) {
    return Either.left(
      violation(
        "The referenced exchange does not contain the declared source act.",
      ),
    );
  }
  if (source.kind === "field") {
    const value = fields[source.field];
    return value === undefined
      ? Either.left(
          unavailable(
            "A declared relation needs an earlier field that is missing.",
          ),
        )
      : Either.right(value);
  }
  const present = (names: readonly string[]) =>
    names.filter((field) => Object.hasOwn(fields, field));
  if (source.kind === "present") {
    return Either.right(present(source.fields));
  }
  const keys =
    source.kind === "field-values"
      ? Either.right(present(source.fields))
      : sourceValue(instance, source.keys, ancestry);
  return Either.flatMap(keys, (selected) => {
    if (typeof selected !== "object") {
      return Either.left(
        unavailable("A selected-values relation needs a list of field names."),
      );
    }
    const values = selected.flatMap((key) => {
      const value = fields[key];
      return typeof value === "string" ? [value] : [];
    });
    return values.length === selected.length
      ? Either.right(values)
      : Either.left(
          unavailable("A selected value is missing or is not a string."),
        );
  });
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

function violation(detail: string, field?: string): NormProblem {
  return {
    stage: "protocol",
    reason: "rule-violation",
    detail,
    ...(field === undefined ? {} : { field }),
  };
}

function unavailable(detail: string): NormProblem {
  return { stage: "protocol", reason: "unresolved-evidence", detail };
}
