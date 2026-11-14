/** @file Message and collective request contracts preserve public values, forms and decoded content. */

import { effect as it } from "@effect/vitest";
import {
  Array as Arr,
  Effect,
  Either,
  Encoding,
  Exit,
  FastCheck as fc,
  Option,
  Order,
  Schema,
  Struct,
} from "effect";
import { expect, it as vitestIt } from "vitest";
import { digest, testAgentCard } from "../__tests__/agents.js";
import {
  CollectivePartInvalidError,
  collectivePartOf,
  CollectiveRequest,
  collectiveRequestContent,
  CollectiveResponse,
  Content,
  encodeCollectiveContent,
  GroupAddress,
  isCollectiveRequestMessage,
  Message,
  MessageAddressInput,
  messageWithId,
  MsgId,
  RequestedSchema,
  type ResponseContent,
  validateResponseContent,
} from "./messages.js";
import { Incoming } from "./service.js";

const alice = testAgentCard("alice");

const bob = testAgentCard("bob");

const carol = testAgentCard("carol");

const msgId = Schema.decodeUnknownSync(MsgId)(digest("msg_", 1));

const content = [{ type: "text", text: "Choose a meeting time." }] as const;

it.each([
  { reason: "the author as a recipient", recipients: [alice] },
  { reason: "a repeated recipient", recipients: [bob, bob] },
  {
    reason: "one identity under two recipient names",
    recipients: [bob, { ...carol, agentId: bob.agentId }],
  },
])("rejects $reason", ({ recipients }) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknown(Message)({
      msgId,
      author: alice,
      recipients,
      content,
    }).pipe(Effect.exit);

    expect(Exit.isFailure(decoded)).toBe(true);
  }),
);

it("captures nested content independently of the caller's objects", () =>
  Effect.gen(function* () {
    const data = { times: ["monday"] };
    const message = yield* messageWithId(alice, [bob], msgId, [
      { type: "data", value: data },
    ]);
    data.times.push("tuesday");
    expect(message.content).toEqual([
      { type: "data", value: { times: ["monday"] } },
    ]);
  }));

it("captures a collective request and a response while keeping their decoded meaning out of stored message bytes", () =>
  Effect.gen(function* () {
    const request = yield* CollectiveRequest.create({
      author: alice,
      recipients: [bob, carol],
      op: "all_gather",
      question: "Which day works?",
      deadline: 30,
      requestedSchema: Schema.decodeUnknownSync(RequestedSchema)({
        type: "object",
        properties: { day: { type: "string" } },
        required: ["day"],
      }),
    });
    const received = yield* messageWithId(
      request.author,
      request.recipients,
      request.msgId,
      yield* collectiveRequestContent(request),
    );
    if (!isCollectiveRequestMessage(received)) {
      return yield* Effect.dieMessage("the collective request did not decode");
    }
    expect(received.collectiveRequest).toEqual({
      op: request.op,
      question: request.question,
      requestedSchema: request.requestedSchema,
      deadlineAt: request.deadlineAt,
    });
    const response = yield* CollectiveResponse.create({
      author: bob,
      request: received,
      response: {
        requestId: received.msgId,
        action: "accept",
        content: { day: "monday" },
      },
    });
    expect(response.recipients).toEqual([alice, carol]);
    expect(response.collectiveResponse).toEqual({
      requestId: request.msgId,
      action: "accept",
      content: { day: "monday" },
    });
    const incoming: Incoming = { kind: "message", message: response };
    const stored = yield* Schema.encode(Incoming)(incoming);
    expect(stored).toMatchObject({ kind: "message", message: { author: bob } });
    if (stored.kind !== "message") {
      return yield* Effect.dieMessage("stored handoff changed kind");
    }
    expect(stored.message).not.toHaveProperty("collectiveResponse");
    expect(yield* Schema.decodeUnknown(Incoming)(stored)).toEqual(incoming);
  }));

const fieldSchema = (field: object) =>
  Schema.decodeUnknownSync(RequestedSchema)({
    type: "object",
    properties: { field },
  });

/** The keyword of each issue a response field's value raises against a field; none when it is accepted. */
const keywordsOf = (field: object, value: ResponseContent[string]) =>
  Either.match(
    Effect.runSync(
      Effect.either(
        validateResponseContent(fieldSchema(field), { field: value }),
      ),
    ),
    {
      onRight: () => [],
      onLeft: (failures) =>
        failures.flatMap(({ detail }) =>
          (detail ?? "").split(", ").map((issue) => issue.split(":")[0]),
        ),
    },
  );

const accepts = (format: string, text: string) =>
  keywordsOf({ type: "string", format }, text).length === 0;

const padded = (value: number, width = 2) => String(value).padStart(width, "0");

vitestIt("accepts a date exactly when the calendar holds that day", () => {
  fc.assert(
    fc.property(
      fc.integer({ min: 0, max: 9999 }),
      fc.integer({ min: 0, max: 13 }),
      fc.integer({ min: 0, max: 32 }),
      (year, month, day) => {
        const calendar = new Date(0);
        calendar.setUTCFullYear(year, month - 1, day);
        const held =
          calendar.getUTCFullYear() === year &&
          calendar.getUTCMonth() === month - 1 &&
          calendar.getUTCDate() === day;

        expect(
          accepts("date", `${padded(year, 4)}-${padded(month)}-${padded(day)}`),
        ).toBe(held);
      },
    ),
  );
});

vitestIt(
  "accepts an instant's ISO text and refuses it without its zone",
  () => {
    fc.assert(
      fc.property(
        fc.date({
          min: new Date("0000-01-01T00:00:00Z"),
          max: new Date("9999-12-31T23:59:59Z"),
          noInvalidDate: true,
        }),
        (instant) => {
          const text = instant.toISOString();

          expect(accepts("date-time", text)).toBe(true);
          expect(accepts("date-time", text.slice(0, -1))).toBe(false);
        },
      ),
    );
  },
);

vitestIt("accepts an email only when its host name has a dot", () => {
  const label = fc.stringMatching(/^[a-z0-9]{1,8}$/);
  fc.assert(
    fc.property(label, label, label, (local, host, top) => {
      expect(accepts("email", `${local}@${host}.${top}`)).toBe(true);
      expect(accepts("email", `${local}@${host}`)).toBe(false);
    }),
  );
});

vitestIt(
  "accepts every web URL as a uri and refuses text without a scheme",
  () => {
    fc.assert(
      fc.property(
        fc.webUrl({ withQueryParameters: true, withFragments: true }),
        fc.string().filter((text) => !text.includes(":")),
        (url, bare) => {
          expect(accepts("uri", url)).toBe(true);
          expect(accepts("uri", bare)).toBe(false);
        },
      ),
    );
  },
);

const fieldNames = ["slot", "hours", "note", "constructor", "toString"];

vitestIt(
  "names exactly the fields an answer is missing, adds or gets wrong",
  () => {
    const field = fc.record({
      declared: fc.boolean(),
      required: fc.boolean(),
      answer: fc.constantFrom("absent", "valid", "invalid"),
    });
    fc.assert(
      fc.property(fc.tuple(field, field, field, field, field), (fields) => {
        const named = fields.map((state, index) => ({
          ...state,
          name: fieldNames[index] ?? "",
        }));
        const declared = named.filter((state) => state.declared);
        const schema = Schema.decodeUnknownSync(RequestedSchema)({
          type: "object",
          properties: Object.fromEntries(
            declared.map(({ name }) => [
              name,
              { type: "string", enum: ["yes"] },
            ]),
          ),
          required: declared
            .filter((state) => state.required)
            .map(({ name }) => name),
        });
        const content = Object.fromEntries(
          named
            .filter((state) => state.answer !== "absent")
            .map(({ name, answer }) => [
              name,
              answer === "valid" ? "yes" : "no",
            ]),
        );
        const reasonOf = (state: (typeof named)[number]) => {
          if (!state.declared) {
            return state.answer === "absent" ? [] : ["unexpected"];
          }
          if (state.answer === "absent") {
            return state.required ? ["missing"] : [];
          }
          return state.answer === "valid" ? [] : ["invalid"];
        };
        const expected = named.flatMap((state) =>
          reasonOf(state).map((reason) => `${state.name} ${reason}`),
        );
        const failures = Either.match(
          Effect.runSync(
            Effect.either(validateResponseContent(schema, content)),
          ),
          {
            onRight: () => [],
            onLeft: (fields) =>
              fields.map(({ field, reason }) => `${field} ${reason}`),
          },
        );

        expect(failures).toHaveLength(expected.length);
        expect(new Set(failures)).toEqual(new Set(expected));
      }),
    );
  },
);

vitestIt("names the type of a value another type of field is given", () => {
  const kinds = {
    string: { field: { type: "string" }, value: "a" },
    number: { field: { type: "number" }, value: 1.5 },
    boolean: { field: { type: "boolean" }, value: false },
    array: {
      field: { type: "array", items: { type: "string", enum: ["a"] } },
      value: ["a"],
    },
  } as const;
  const kind = fc.constantFrom(...Struct.keys(kinds));
  fc.assert(
    fc.property(kind, kind, (asked, given) => {
      expect(keywordsOf(kinds[asked].field, kinds[given].value)).toEqual(
        asked === given ? [] : ["type"],
      );
    }),
  );
});

vitestIt("holds a number to its bounds and an integer to whole values", () => {
  fc.assert(
    fc.property(
      fc.constantFrom("number", "integer"),
      fc.integer({ min: -100, max: 100 }),
      fc.integer({ min: 0, max: 50 }),
      fc.oneof(
        fc.integer({ min: -200, max: 200 }),
        fc.double({ min: -200, max: 200, noNaN: true }),
      ),
      (type, minimum, span, value) => {
        const maximum = minimum + span;

        expect(keywordsOf({ type, minimum, maximum }, value)).toEqual([
          ...(type === "integer" && !Number.isInteger(value) ? ["type"] : []),
          ...(value < minimum ? ["minimum"] : []),
          ...(value > maximum ? ["maximum"] : []),
        ]);
      },
    ),
  );
});

vitestIt("counts a string's length in code points against its bounds", () => {
  fc.assert(
    fc.property(
      fc.string({ unit: "binary", maxLength: 8 }),
      fc.integer({ min: 0, max: 4 }),
      fc.integer({ min: 0, max: 4 }),
      (text, minLength, span) => {
        const maxLength = minLength + span;
        const length = Array.from(text).length;

        expect(
          keywordsOf({ type: "string", minLength, maxLength }, text),
        ).toEqual([
          ...(length < minLength ? ["minLength"] : []),
          ...(length > maxLength ? ["maxLength"] : []),
        ]);
      },
    ),
  );
});

vitestIt("accepts a single selection exactly when one option holds it", () => {
  fc.assert(
    fc.property(
      fc.array(fc.constantFrom("a", "b", "c"), { minLength: 1, maxLength: 4 }),
      fc.constantFrom("a", "b", "c", "d"),
      fc.boolean(),
      (options, value, titled) => {
        const holders = options.filter((option) => option === value).length;
        const field = titled
          ? {
              type: "string",
              oneOf: options.map((option) => ({
                const: option,
                title: option,
              })),
            }
          : { type: "string", enum: options };
        const refusal = titled ? ["oneOf"] : ["enum"];
        const held = titled ? holders === 1 : holders > 0;

        expect(keywordsOf(field, value)).toEqual(held ? [] : refusal);
      },
    ),
  );
});

vitestIt(
  "checks each selected item against the options and their count against its bounds",
  () => {
    fc.assert(
      fc.property(
        fc.array(fc.constantFrom("a", "b", "x"), { maxLength: 5 }),
        fc.integer({ min: 0, max: 3 }),
        fc.integer({ min: 0, max: 3 }),
        fc.boolean(),
        (selected, minItems, span, titled) => {
          const maxItems = minItems + span;
          const items = titled
            ? {
                anyOf: [
                  { const: "a", title: "A" },
                  { const: "b", title: "B" },
                ],
              }
            : { type: "string", enum: ["a", "b"] };

          expect(
            keywordsOf({ type: "array", minItems, maxItems, items }, selected),
          ).toEqual([
            ...(selected.length < minItems ? ["minItems"] : []),
            ...(selected.length > maxItems ? ["maxItems"] : []),
            ...selected.flatMap((item, index) =>
              item === "x"
                ? [`items[${index}].${titled ? "anyOf" : "enum"}`]
                : [],
            ),
          ]);
        },
      ),
    );
  },
);

const requestId = digest("msg_", 3);

const requestedSchema = {
  type: "object",
  properties: {
    slot: { type: "string", enum: ["mon", "tue"] },
    hours: { type: "number", minimum: 0 },
  },
  required: ["slot"],
};

const gather = {
  kind: "request",
  op: "gather",
  deadlineAt: 1_790_000_000_000,
  requestedSchema,
};

const decline = { kind: "response", requestId, action: "decline" };

const collectiveParts = [
  gather,
  { ...gather, op: "all_gather" },
  { kind: "response", requestId, action: "accept", content: { slot: "mon" } },
  decline,
  {
    kind: "close",
    requestId,
    included: [
      {
        authorAgentId: testAgentCard("agent:bob").agentId,
        msgId: digest("msg_", 2),
      },
    ],
    closedAt: 1_790_000_000_000,
    closedBy: "deadline",
  },
];

/** The data part that carries a collective part. */
const carrying = (part: unknown) => ({
  type: "data",
  value: { "org.social-harness/collective": part },
});

/** What a recipient reads from a message with this content. */
const interpreted = (content: readonly unknown[]) =>
  collectivePartOf(
    Schema.decodeUnknownSync(Message)({
      msgId,
      author: alice,
      recipients: [bob],
      content,
    }),
  );

const decodes = (part: unknown): boolean =>
  Exit.isSuccess(Effect.runSyncExit(interpreted([carrying(part)])));

vitestIt(
  "reads the one collective part content carries, none without one and a refusal with more",
  () => {
    fc.assert(
      fc.property(
        fc.constantFrom(...collectiveParts),
        fc.integer({ min: 0, max: 3 }),
        (part, copies) => {
          const read = Effect.runSync(
            Effect.either(
              interpreted([
                { type: "text", text: "Which day?" },
                ...Array.from({ length: copies }, () => carrying(part)),
              ]),
            ),
          );

          if (copies > 1) {
            expect(read).toStrictEqual(
              Either.left(
                new CollectivePartInvalidError({ reason: "duplicate" }),
              ),
            );
            return;
          }
          expect(Either.map(read, Option.isSome)).toEqual(
            Either.right(copies === 1),
          );
        },
      ),
    );
  },
);

vitestIt(
  "writes a collective part into content that reads back as the same part",
  () => {
    fc.assert(
      fc.property(
        fc.constantFrom(...collectiveParts),
        fc.option(fc.string({ minLength: 1, maxLength: 64 }), {
          nil: undefined,
        }),
        (raw, text) => {
          const part = Option.getOrThrow(
            Effect.runSync(interpreted([carrying(raw)])),
          );
          const content = Effect.runSync(encodeCollectiveContent(part, text));

          expect(Effect.runSync(interpreted(content))).toEqual(
            Option.some(part),
          );
        },
      ),
    );
  },
);

vitestIt(
  "accepts content exactly when its JSON text is at most 32,768 bytes",
  () => {
    fc.assert(
      fc.property(fc.integer({ min: 32_700, max: 32_800 }), (length) => {
        const content = [{ type: "text", text: "a".repeat(length) }];

        expect(Schema.is(Content)(content)).toBe(
          JSON.stringify(content).length <= 32_768,
        );
      }),
    );
  },
);

it("gives each collective request its own message id", () =>
  Effect.gen(function* () {
    const ask = CollectiveRequest.create({
      author: bob,
      recipients: [alice],
      op: "gather",
      question: "Which day works?",
      deadline: 30,
      requestedSchema:
        Schema.decodeUnknownSync(RequestedSchema)(requestedSchema),
    });
    const first = yield* ask;
    const second = yield* ask;

    expect(first.msgId).not.toBe(second.msgId);
  }));

vitestIt.each([
  { ...decline, requestId: digest("req_", 1) },
  { ...decline, requestId: `${requestId}=` },
])("rejects a response that names no canonical message id: %j", (response) => {
  expect(decodes(response)).toBe(false);
  expect(decodes(decline)).toBe(true);
});

const decodingFails = (
  schema: Schema.Schema.AnyNoContext,
  value: unknown,
): boolean => !Schema.is(schema)(value);

vitestIt(
  "accepts a group address of 3 to 32 members only in name order",
  () => {
    fc.assert(
      fc.property(
        fc.uniqueArray(fc.stringMatching(/^[a-z0-9]{3,8}$/), {
          minLength: 2,
          maxLength: 34,
        }),
        (names) => {
          const ordered = Arr.sort(names, Order.string);
          const sized = names.length >= 3 && names.length <= 32;
          const reversed = `group:${ordered.toReversed().join(",")}`;

          expect(
            decodingFails(GroupAddress, `group:${ordered.join(",")}`),
          ).toBe(!sized);
          expect(decodingFails(GroupAddress, reversed)).toBe(true);
          expect(decodingFails(MessageAddressInput, reversed)).toBe(false);
        },
      ),
    );
  },
);

vitestIt(
  "accepts a message identifier exactly when it encodes 32 bytes",
  () => {
    fc.assert(
      fc.property(fc.uint8Array({ maxLength: 48 }), (bytes) => {
        expect(
          decodingFails(MsgId, `msg_${Encoding.encodeBase64Url(bytes)}`),
        ).toBe(bytes.length !== 32);
      }),
    );
  },
);
