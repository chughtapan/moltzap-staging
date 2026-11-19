/** @file Message and form contracts preserve public values and content. */

import { effect as it } from "@effect/vitest";
import {
  Effect,
  Either,
  Encoding,
  Exit,
  FastCheck as fc,
  Schema,
  Struct,
} from "effect";
import { expect, it as vitestIt } from "vitest";
import { digest, testAgentCard } from "../__tests__/agents.js";
import {
  Content,
  Message,
  messageWithId,
  MsgId,
  RequestedSchema,
  type ResponseContent,
  validateResponseContent,
} from "./messages.js";

const alice = testAgentCard("alice");

const bob = testAgentCard("bob");

const msgId = Schema.decodeUnknownSync(MsgId)(digest("msg_", 1));

const content = [{ type: "text", text: "Choose a meeting time." }] as const;

it("rejects the author as a recipient", () =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknown(Message)({
      msgId,
      author: alice,
      recipients: [alice],
      content,
    }).pipe(Effect.exit);

    expect(Exit.isFailure(decoded)).toBe(true);
  }));

it("captures nested content independently of the caller's objects", () =>
  Effect.gen(function* () {
    const data = { times: ["monday"] };
    const message = yield* messageWithId(alice, [bob], { msgId }, [
      { type: "data", value: data },
    ]);
    data.times.push("tuesday");
    expect(message.content).toEqual([
      { type: "data", value: { times: ["monday"] } },
    ]);
  }));

vitestIt(
  "keeps the reply reference its sender named through storage, and none when none was named",
  () => {
    fc.assert(
      fc.property(
        fc.option(fc.integer({ min: 0, max: 255 }), { nil: undefined }),
        (byte) => {
          const replyToMsgId =
            byte === undefined
              ? undefined
              : Schema.decodeUnknownSync(MsgId)(digest("msg_", byte));
          const message = Effect.runSync(
            messageWithId(alice, [bob], { msgId, replyToMsgId }, content),
          );
          const stored = Schema.encodeSync(Schema.parseJson(Message))(message);
          const read = Schema.decodeUnknownSync(Schema.parseJson(Message))(
            stored,
          );
          expect(read.replyToMsgId).toBe(replyToMsgId);
          expect(Object.hasOwn(read, "replyToMsgId")).toBe(byte !== undefined);
        },
      ),
    );
  },
);

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
  "names exactly the fields a response is missing, adds or gets wrong",
  () => {
    const field = fc.record({
      declared: fc.boolean(),
      required: fc.boolean(),
      response: fc.constantFrom("absent", "valid", "invalid"),
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
            .filter((state) => state.response !== "absent")
            .map(({ name, response }) => [
              name,
              response === "valid" ? "yes" : "no",
            ]),
        );
        const reasonOf = (state: (typeof named)[number]) => {
          if (!state.declared) {
            return state.response === "absent" ? [] : ["unexpected"];
          }
          if (state.response === "absent") {
            return state.required ? ["missing"] : [];
          }
          return state.response === "valid" ? [] : ["invalid"];
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

it("gives each created message its own message id", () =>
  Effect.gen(function* () {
    const create = Message.create({
      author: bob,
      recipients: [alice],
      content,
    });
    const first = yield* create;
    const second = yield* create;

    expect(first.msgId).not.toBe(second.msgId);
  }));

const decodingFails = (
  schema: Schema.Schema.AnyNoContext,
  value: unknown,
): boolean => !Schema.is(schema)(value);

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
