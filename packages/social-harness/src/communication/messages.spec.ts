/** @file Message and request contracts preserve public values, forms and decoded content. */

import { effect as it } from "@effect/vitest";
import {
  Effect,
  Either,
  Encoding,
  Exit,
  JSONSchema,
  Option,
  ParseResult,
  Schema,
} from "effect";
import { describe, expect, it as vitestIt } from "vitest";
import {
  digest,
  digest as publicDigest,
  testAgentCard,
} from "../__tests__/agents.js";
import { encodeCanonical } from "../encoding/canonical-json.js";
import {
  AgentAddress as PublicAgentAddress,
  Content as PublicContent,
  GroupAddress as PublicGroupAddress,
  MessageAddressInput as PublicMessageAddressInput,
  MsgId as PublicMsgId,
} from "../index.js";
import { collectiveOperationId } from "./__tests__/request-ids.js";
import {
  type AnswerContent,
  type AnswerFormat,
  COLLECTIVE_DATA_KEY,
  CollectiveContentError,
  CollectivePartInvalidError,
  collectiveRequesterOf,
  Content,
  decodeCollectiveValue,
  encodeCollectiveContent,
  matchesFormat,
  Message,
  messageFromIntent,
  messageFromParticipants,
  RequestedSchema as messagesRequestedSchema,
  mintCollectiveId,
  MsgId,
  readCollectiveValue,
  validateAnswer,
} from "./messages.js";
import { Answer, Incoming, Request, RequestedSchema } from "./service.js";

const alice = testAgentCard("alice");

const bob = testAgentCard("bob");

const carol = testAgentCard("carol");

const msgId = Schema.decodeUnknownSync(MsgId)(digest("msg_", 1));

const content = [{ type: "text", text: "Choose a meeting time." }] as const;

// @agent-code-guard/regression-only: Retried messages retain attribution while ambiguous participant identities are rejected.
it("preserves identity, participants and content when member enumeration changes", () =>
  Effect.gen(function* () {
    const intent = { authorAgentId: alice.agentId, msgId, content };
    const first = yield* messageFromIntent([carol, alice, bob], intent);
    const recovered = yield* messageFromIntent([bob, carol, alice], intent);

    expect(first).toEqual({
      msgId,
      author: alice,
      recipients: [bob, carol],
      content,
    });
    expect(recovered).toEqual(first);
  }));

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
    const message = yield* messageFromParticipants(alice, [bob], {
      msgId,
      content: [{ type: "data", value: data }],
    });
    data.times.push("tuesday");
    expect(message.content).toEqual([
      { type: "data", value: { times: ["monday"] } },
    ]);
  }));

it("captures a request and an answer while keeping decoded operation metadata out of stored message bytes", () =>
  Effect.gen(function* () {
    const request = yield* Request.create({
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
    const question = yield* Message.create({
      author: request.author,
      recipients: request.recipients,
      content: yield* encodeCollectiveContent(
        {
          kind: "operation",
          op: request.op,
          operationId: request.operationId,
          deadlineAt: request.deadlineAt,
          requestedSchema: request.requestedSchema,
        },
        request.question,
      ),
    });
    if (question.operation?.kind !== "request") {
      return yield* Effect.dieMessage("question did not decode");
    }
    const answer = yield* Answer.create({
      author: bob,
      request: { ...question, operation: question.operation },
      response: { action: "accept", content: { day: "monday" } },
    });
    expect(answer.recipients).toEqual([alice, carol]);
    expect(answer.operation).toMatchObject({
      kind: "response",
      operationId: request.operationId,
      content: { day: "monday" },
    });
    const incoming: Incoming = { kind: "message", message: answer };
    const stored = yield* Schema.encode(Incoming)(incoming);
    expect(stored).toMatchObject({ kind: "message", message: { author: bob } });
    if (stored.kind !== "message") {
      return yield* Effect.dieMessage("stored handoff changed kind");
    }
    expect(stored.message).not.toHaveProperty("operation");
    expect(yield* Schema.decodeUnknown(Incoming)(stored)).toEqual(incoming);
    expect(JSONSchema.make(Schema.typeSchema(Incoming))).toHaveProperty(
      "anyOf",
    );
  }));

const judged = (format: AnswerFormat, texts: readonly string[]) =>
  texts.map((text) => matchesFormat(format, text));

// @agent-code-guard/regression-only: examples pin each format's accepted and refused strings.
describe("date answer formats", () => {
  vitestIt("accepts calendar dates and refuses impossible ones", () => {
    expect(
      judged("date", ["2024-02-29", "2023-02-29", "2024-13-01", "2024-2-01"]),
    ).toEqual([true, false, false, false]);
  });

  vitestIt("applies the century rule to February 29", () => {
    expect(judged("date", ["2000-02-29", "1900-02-29"])).toEqual([true, false]);
  });

  vitestIt("requires a time zone on a date-time", () => {
    expect(
      judged("date-time", [
        "2024-02-01T10:00:00Z",
        "2024-02-01t10:00:00.5+01:30",
        "2024-02-01 10:00:00-0130",
        "2024-02-01T10:00:00",
      ]),
    ).toEqual([true, true, true, false]);
  });

  vitestIt("admits a leap second only at 23:59 UTC", () => {
    expect(
      judged("date-time", [
        "2024-02-01T23:59:60Z",
        "2024-02-01T22:59:60-01:00",
        "2024-02-01T10:59:60Z",
      ]),
    ).toEqual([true, true, false]);
  });

  vitestIt(
    "admits a leap second whose UTC reading borrows across midnight",
    () => {
      expect(
        judged("date-time", [
          "2024-01-01T00:59:60+01:00",
          "2024-01-01T00:00:60+00:01",
          "2024-01-01T00:59:60+02:00",
        ]),
      ).toEqual([true, true, false]);
    },
  );
});

// @agent-code-guard/regression-only: examples pin each format's accepted and refused strings.
describe("address answer formats", () => {
  vitestIt("requires a dotted host name in an email", () => {
    expect(
      judged("email", ["a.b@example.com", "a@example", ".a@example.com"]),
    ).toEqual([true, false, false]);
  });

  vitestIt("requires a scheme and RFC 3986 characters in a uri", () => {
    expect(
      judged("uri", [
        "https://user@[::1]:8080/a?b#c",
        "urn:example:x",
        "example.com",
        "https://a b",
        "https://%zz",
      ]),
    ).toEqual([true, true, false, false, false]);
  });
});

const requestedSchema = Schema.decodeUnknownSync(messagesRequestedSchema)({
  type: "object",
  properties: {
    slot: { type: "string", enum: ["mon", "tue"] },
    hours: { type: "number", minimum: 0 },
  },
  required: ["slot"],
});

const failingFields = (content: AnswerContent) =>
  Effect.runSync(Effect.flip(validateAnswer(requestedSchema, content))).map(
    ({ field, reason }) => ({ field, reason }),
  );

// @agent-code-guard/regression-only: examples pin which answer fields fail and how each failure is named.
describe("answer validation", () => {
  vitestIt("accepts an answer satisfying every field", () => {
    const content = { slot: "tue", hours: 2 };

    expect(Effect.runSync(validateAnswer(requestedSchema, content))).toBe(
      content,
    );
  });

  vitestIt("names a field whose value fails its property schema", () => {
    expect(failingFields({ slot: "wed" })).toEqual([
      { field: "slot", reason: "invalid" },
    ]);
  });

  vitestIt("names a missing required field", () => {
    expect(failingFields({ hours: 1 })).toEqual([
      { field: "slot", reason: "missing" },
    ]);
  });

  vitestIt("names a field the schema does not declare", () => {
    expect(failingFields({ slot: "mon", mood: "fine" })).toEqual([
      { field: "mood", reason: "unexpected" },
    ]);
  });

  vitestIt(
    "names an undeclared field that shares an Object.prototype name",
    () => {
      expect(failingFields({ slot: "mon", constructor: "x" })).toEqual([
        { field: "constructor", reason: "unexpected" },
      ]);
    },
  );

  vitestIt(
    "names a missing required field that shares an Object.prototype name",
    () => {
      const toStringSchema = Schema.decodeUnknownSync(messagesRequestedSchema)({
        type: "object",
        properties: { toString: { type: "string" } },
        required: ["toString"],
      });

      expect(
        Effect.runSync(Effect.flip(validateAnswer(toStringSchema, {}))).map(
          ({ field, reason }) => ({ field, reason }),
        ),
      ).toEqual([{ field: "toString", reason: "missing" }]);
    },
  );

  vitestIt("names every failing field of one answer", () => {
    expect(failingFields({ slot: "wed", hours: -1 })).toEqual([
      { field: "slot", reason: "invalid" },
      { field: "hours", reason: "invalid" },
    ]);
  });
});

const fieldSchema = (field: object) =>
  Schema.decodeUnknownSync(messagesRequestedSchema)({
    type: "object",
    properties: { field },
  });

/** The keyword each issue of an invalid field's detail names, in order. */
const keywordsOf = (field: object, value: AnswerContent[string]) =>
  Effect.runSync(
    Effect.flip(validateAnswer(fieldSchema(field), { field: value })),
  ).flatMap(({ reason, detail }) => [
    reason,
    ...(detail ?? "").split(", ").map((issue) => issue.split(":")[0]),
  ]);

const accepts = (field: object, value: AnswerContent[string]) =>
  Exit.isSuccess(
    Effect.runSyncExit(validateAnswer(fieldSchema(field), { field: value })),
  );

// @agent-code-guard/regression-only: examples pin which keyword an invalid string answer names.
describe("string answer keywords", () => {
  vitestIt(
    "names both the type and the enum a mistyped selection fails",
    () => {
      expect(keywordsOf({ type: "string", enum: ["mon"] }, 3)).toEqual([
        "invalid",
        "type",
        "enum",
      ]);
    },
  );

  vitestIt("counts a string's length in code points", () => {
    const field = { type: "string", minLength: 2 };

    expect(accepts(field, "\u{1F600}\u{1F600}")).toBe(true);
    expect(keywordsOf(field, "\u{1F600}")).toEqual(["invalid", "minLength"]);
  });

  vitestIt("names the format a string fails", () => {
    expect(keywordsOf({ type: "string", format: "email" }, "a@b")).toEqual([
      "invalid",
      "format",
    ]);
  });

  vitestIt("counts a string's maximum length in code points", () => {
    const field = { type: "string", maxLength: 2 };

    expect(accepts(field, "\u{1F600}\u{1F600}")).toBe(true);
    expect(keywordsOf(field, "abc")).toEqual(["invalid", "maxLength"]);
  });

  vitestIt(
    "refuses a titled single-select value that two options share",
    () => {
      const field = {
        type: "string",
        oneOf: [
          { const: "a", title: "A" },
          { const: "a", title: "Also A" },
          { const: "b", title: "B" },
        ],
      };

      expect(accepts(field, "b")).toBe(true);
      expect(keywordsOf(field, "a")).toEqual(["invalid", "oneOf"]);
    },
  );
});

// @agent-code-guard/regression-only: examples pin which keyword an invalid number or boolean names.
describe("number and boolean answer keywords", () => {
  vitestIt(
    "accepts a fraction for a number and refuses a value above the maximum",
    () => {
      const field = { type: "number", maximum: 2 };

      expect(accepts(field, 1.5)).toBe(true);
      expect(accepts(field, 2)).toBe(true);
      expect(keywordsOf(field, 2.5)).toEqual(["invalid", "maximum"]);
      expect(keywordsOf(field, "1")).toEqual(["invalid", "type"]);
    },
  );

  vitestIt("refuses a string for a boolean", () => {
    expect(accepts({ type: "boolean" }, false)).toBe(true);
    expect(keywordsOf({ type: "boolean" }, "true")).toEqual([
      "invalid",
      "type",
    ]);
  });

  vitestIt(
    "refuses a fraction for an integer and a value below the minimum",
    () => {
      const field = { type: "integer", minimum: 1 };

      expect(keywordsOf(field, 1.5)).toEqual(["invalid", "type"]);
      expect(keywordsOf(field, 0)).toEqual(["invalid", "minimum"]);
    },
  );
});

// @agent-code-guard/regression-only: examples pin which keyword an invalid multi-select names.
describe("multi-select answer keywords", () => {
  vitestIt("names each selected item outside a multi-select's options", () => {
    const field = {
      type: "array",
      maxItems: 1,
      items: { type: "string", enum: ["a"] },
    };

    expect(keywordsOf(field, ["a", "b"])).toEqual([
      "invalid",
      "maxItems",
      "items[1].enum",
    ]);
  });

  vitestIt("refuses fewer selections than the minimum", () => {
    const field = {
      type: "array",
      minItems: 1,
      items: { type: "string", enum: ["a"] },
    };

    expect(accepts(field, ["a"])).toBe(true);
    expect(keywordsOf(field, [])).toEqual(["invalid", "minItems"]);
  });

  vitestIt(
    "checks a titled multi-select's items against its option consts",
    () => {
      const field = {
        type: "array",
        items: { anyOf: [{ const: "a", title: "A" }] },
      };

      expect(accepts(field, ["a"])).toBe(true);
      expect(keywordsOf(field, ["b"])).toEqual(["invalid", "items[0].anyOf"]);
    },
  );
});

const collectiveId = collectiveOperationId(testAgentCard("agent:bob").agentId);

const answerReference = {
  authorAgentId: testAgentCard("agent:bob").agentId,
  msgId: digest("msg_", 2),
};

const deadlineAt = 1_790_000_000_000;

const flatSchema = {
  type: "object",
  properties: {
    slot: { type: "string", enum: ["mon", "tue"] },
    note: { type: "string" },
    hours: { type: "number", minimum: 0 },
    confirmed: { type: "boolean" },
  },
  required: ["slot"],
};

const gather = {
  kind: "operation",
  op: "gather",
  operationId: collectiveId,
  deadlineAt,
  requestedSchema: flatSchema,
};

const decodes = (value: unknown): boolean =>
  Exit.isSuccess(Effect.runSyncExit(decodeCollectiveValue(value)));

const decodesSchema = (value: unknown): boolean =>
  Exit.isSuccess(
    Effect.runSyncExit(Schema.decodeUnknown(messagesRequestedSchema)(value)),
  );

/** Each issue message of a schema that fails to decode; empty if it decodes. */
const schemaIssues = (value: unknown) =>
  Either.match(Schema.decodeUnknownEither(messagesRequestedSchema)(value), {
    onLeft: (error) =>
      ParseResult.ArrayFormatter.formatErrorSync(error).map(
        ({ message }) => message,
      ),
    onRight: () => [],
  });

const decodeValue = (value: unknown) =>
  Effect.runSync(decodeCollectiveValue(value));

// @agent-code-guard/regression-only: examples pin the closed collective content round trip.
describe("collective content round trip", () => {
  vitestIt(
    "round-trips a request with the question before its collective data",
    () => {
      const content = Effect.runSync(
        encodeCollectiveContent(decodeValue(gather), "Which day?"),
      );

      expect(content).toEqual([
        { type: "text", text: "Which day?" },
        { type: "data", value: { [COLLECTIVE_DATA_KEY]: gather } },
      ]);
      expect(Effect.runSync(readCollectiveValue(content))).toEqual(
        Option.some(decodeValue(gather)),
      );
    },
  );

  vitestIt("round-trips an all_gather close record", () => {
    const close = {
      kind: "close",
      operationId: collectiveId,
      included: [answerReference],
      closedAt: deadlineAt,
      closedBy: "deadline",
    };

    const content = Effect.runSync(encodeCollectiveContent(decodeValue(close)));

    expect(Effect.runSync(readCollectiveValue(content))).toEqual(
      Option.some(decodeValue(close)),
    );
  });
});

describe("collective timestamps", () => {
  vitestIt.each([0, -1, 8_640_000_000_000_001])(
    "rejects the invalid request deadline %s",
    (deadlineAt) => {
      expect(decodes({ ...gather, deadlineAt })).toBe(false);
    },
  );

  vitestIt.each([0, 8_640_000_000_000_000])(
    "accepts the completion timestamp %s",
    (closedAt) => {
      expect(
        decodes({
          kind: "close",
          operationId: collectiveId,
          included: [],
          closedAt,
          closedBy: "settled",
        }),
      ).toBe(true);
    },
  );
});

// @agent-code-guard/regression-only: examples pin the closed operation and response wire grammar.
describe("collective value grammar", () => {
  vitestIt("rejects a gather without a deadline", () => {
    expect(
      decodes({
        kind: "operation",
        op: "gather",
        operationId: collectiveId,
        requestedSchema: flatSchema,
      }),
    ).toBe(false);
  });

  vitestIt("rejects an all_gather without a requested schema", () => {
    expect(
      decodes({
        kind: "operation",
        op: "all_gather",
        operationId: collectiveId,
        deadlineAt,
      }),
    ).toBe(false);
  });
});

// @agent-code-guard/regression-only: examples pin that only an accept response carries content.
describe("collective response grammar", () => {
  vitestIt("accepts an accept response carrying content", () => {
    expect(
      decodes({
        kind: "response",
        operationId: collectiveId,
        action: "accept",
        content: { slot: "mon" },
      }),
    ).toBe(true);
  });

  vitestIt("rejects a decline response carrying content", () => {
    expect(
      decodes({
        kind: "response",
        operationId: collectiveId,
        action: "decline",
        content: { slot: "mon" },
      }),
    ).toBe(false);
  });

  vitestIt("rejects an accept response without content", () => {
    expect(
      decodes({
        kind: "response",
        operationId: collectiveId,
        action: "accept",
      }),
    ).toBe(false);
  });

  vitestIt("rejects a cancel, which is not an answer", () => {
    expect(
      decodes({
        kind: "response",
        operationId: collectiveId,
        action: "cancel",
      }),
    ).toBe(false);
  });
});

// @agent-code-guard/regression-only: examples pin the MCP form-mode grammar the SDK accepts.
describe("requested schema grammar", () => {
  vitestIt(
    "accepts a flat schema of enum, string, number and boolean fields",
    () => {
      expect(decodesSchema(flatSchema)).toBe(true);
    },
  );

  vitestIt("rejects a nested object property", () => {
    expect(
      decodesSchema({
        type: "object",
        properties: { where: { type: "object", properties: {} } },
      }),
    ).toBe(false);
  });

  vitestIt("rejects a required name that is not a property", () => {
    expect(
      decodesSchema({
        type: "object",
        properties: { note: { type: "string" } },
        required: ["slot"],
      }),
    ).toBe(false);
  });

  vitestIt(
    "accepts a multi-select only when its items carry a string type",
    () => {
      const multiSelect = (items: object) => ({
        type: "object",
        properties: { slots: { type: "array", items } },
      });

      expect(decodesSchema(multiSelect({ type: "string", enum: ["a"] }))).toBe(
        true,
      );
      expect(decodesSchema(multiSelect({ enum: ["a"] }))).toBe(false);
    },
  );

  vitestIt("rejects a select whose enum offers no option", () => {
    expect(
      decodesSchema({
        type: "object",
        properties: { slot: { type: "string", enum: [] } },
      }),
    ).toBe(false);
  });
});

// @agent-code-guard/regression-only: examples pin which keywords an admitted property keeps.
describe("admitted form fields", () => {
  vitestIt(
    "keeps only the keywords of the shape that admits a property",
    () => {
      const decoded = Schema.decodeUnknownSync(messagesRequestedSchema)({
        type: "object",
        properties: {
          slot: { type: "string", enum: ["a"], minLength: 2, pattern: "x" },
          named: {
            type: "string",
            oneOf: [{ const: "a", title: "A" }],
            pattern: "x",
          },
        },
      });

      expect(decoded.properties).toEqual({
        slot: { type: "string", enum: ["a"] },
        named: { type: "string", oneOf: [{ const: "a", title: "A" }] },
      });
    },
  );

  vitestIt("names the empty enum of a select that offers no option", () => {
    expect(
      schemaIssues({
        type: "object",
        properties: {
          slots: { type: "array", items: { type: "string", enum: [] } },
        },
      }),
    ).toEqual(["items.enum: Invalid input: expected at least one option"]);
  });
});

// @agent-code-guard/regression-only: examples pin how content carries at most one collective part within the size limit.
describe("collective content", () => {
  vitestIt("reads content without a collective part as none", () => {
    const content = Schema.decodeUnknownSync(Content)([
      { type: "text", text: "hello" },
      { type: "data", value: { other: 1 } },
    ]);

    expect(Effect.runSync(readCollectiveValue(content))).toEqual(Option.none());
  });

  vitestIt("rejects content carrying two collective parts", () => {
    const part = { type: "data", value: { [COLLECTIVE_DATA_KEY]: gather } };
    const content = Schema.decodeUnknownSync(Content)([part, part]);

    expect(
      Effect.runSync(Effect.flip(readCollectiveValue(content))),
    ).toStrictEqual(new CollectivePartInvalidError({ reason: "duplicate" }));
  });

  vitestIt("rejects a collective part that does not decode", () => {
    const content = Schema.decodeUnknownSync(Content)([
      { type: "data", value: { [COLLECTIVE_DATA_KEY]: { kind: "unknown" } } },
    ]);

    expect(
      Effect.runSync(Effect.flip(readCollectiveValue(content))),
    ).toStrictEqual(new CollectivePartInvalidError({ reason: "malformed" }));
  });

  vitestIt("rejects a request whose question exceeds the content limit", () => {
    const failure = Effect.runSync(
      Effect.flip(
        encodeCollectiveContent(decodeValue(gather), "q".repeat(32_768)),
      ),
    );

    expect(failure).toStrictEqual(new CollectiveContentError());
  });
});

describe("collective operation identity", () => {
  vitestIt("mints distinct operations for one requester", () => {
    const requester = testAgentCard("agent:bob").agentId;
    const first = Effect.runSync(mintCollectiveId(requester));
    const second = Effect.runSync(mintCollectiveId(requester));

    expect(collectiveRequesterOf(first)).toBe(requester);
    expect(collectiveRequesterOf(second)).toBe(requester);
    expect(first).not.toBe(second);
    expect(decodeValue({ ...gather, operationId: first })).toMatchObject({
      operationId: first,
    });
  });

  vitestIt("identifies different requesters with the same random bytes", () => {
    const bob = testAgentCard("agent:bob").agentId;
    const mallory = testAgentCard("agent:mallory").agentId;
    const bobOperation = collectiveOperationId(bob, 7);
    const malloryOperation = collectiveOperationId(mallory, 7);

    expect(collectiveRequesterOf(bobOperation)).toBe(bob);
    expect(collectiveRequesterOf(malloryOperation)).toBe(mallory);
    expect(bobOperation).not.toBe(malloryOperation);
  });
});

vitestIt.each([
  { ...gather, operationId: digest("col_", 1) },
  { ...gather, operationId: `${collectiveId}=` },
])("rejects a noncanonical request identity: %j", (request) => {
  expect(decodes(request)).toBe(false);
  expect(decodes(gather)).toBe(true);
});

describe("public message schemas", () => {
  const canonicalGroup32 = `group:${Array.from(
    Array(32).keys(),
    (index) => `member-${String(index).padStart(2, "0")}`,
  ).join(",")}`;

  const canonicalGroup33 = `group:${Array.from(
    Array(33).keys(),
    (index) => `member-${String(index).padStart(2, "0")}`,
  ).join(",")}`;

  const decodingFails = (
    schema: Schema.Schema.AnyNoContext,
    value: unknown,
  ): boolean =>
    Either.match(Schema.decodeUnknownEither(schema)(value), {
      onLeft: () => true,
      onRight: () => false,
    });

  // @agent-code-guard/regression-only: Address schemas pin the accepted public runtime grammar.
  describe("public address schemas", () => {
    vitestIt(
      "accepts exact agent addresses and arbitrary group input order",
      () => {
        const direct = "agent:alice-agent";
        const unorderedGroup = "group:carol-agent,bob-agent";

        expect(Schema.decodeUnknownSync(PublicAgentAddress)(direct)).toBe(
          direct,
        );
        expect(
          Schema.decodeUnknownSync(PublicMessageAddressInput)(unorderedGroup),
        ).toBe(unorderedGroup);
      },
    );

    vitestIt("accepts canonical 3-member and 32-member groups", () => {
      const threeMembers = "group:alice-agent,bob-agent,carol-agent";

      expect(Schema.decodeUnknownSync(PublicGroupAddress)(threeMembers)).toBe(
        threeMembers,
      );
      expect(
        Schema.decodeUnknownSync(PublicGroupAddress)(canonicalGroup32),
      ).toBe(canonicalGroup32);
    });

    vitestIt.each([
      {
        case: "noncanonical member order",
        address: "group:carol-agent,alice-agent,bob-agent",
      },
      { case: "2 members", address: "group:alice-agent,bob-agent" },
      { case: "33 members", address: canonicalGroup33 },
    ])("rejects a group output with $case", ({ address }) => {
      expect(decodingFails(PublicGroupAddress, address)).toBe(true);
    });
  });

  const messageHash = (byteLength: number) =>
    Encoding.encodeBase64Url(new Uint8Array(byteLength).fill(7));

  // @agent-code-guard/regression-only: Identifier schemas pin the closed durable send grammar.
  describe("public message identifiers", () => {
    vitestIt("accepts the exact message identifier grammar", () => {
      const msgId = publicDigest("msg_", 7);

      expect(Schema.decodeUnknownSync(PublicMsgId)(msgId)).toBe(msgId);
    });

    // A PublicMsgId is `msg_` plus the canonical base64url of exactly 32 bytes, so
    // each case breaks one of the prefix, the length, or the round trip.
    vitestIt.each([
      { case: "another prefix", candidate: `cnv_${messageHash(32)}` },
      { case: "31 bytes", candidate: `msg_${messageHash(31)}` },
      { case: "33 bytes", candidate: `msg_${messageHash(33)}` },
      {
        case: "a noncanonical final character",
        candidate: `msg_${messageHash(32).slice(0, -1)}d`,
      },
      { case: "padding", candidate: `msg_${messageHash(32)}=` },
    ])("rejects a message identifier with $case", ({ candidate }) => {
      expect(decodingFails(PublicMsgId, candidate)).toBe(true);
    });
  });

  // @agent-code-guard/regression-only: PublicContent schemas pin the exact semantic boundary and canonical byte cap.
  describe("public content", () => {
    vitestIt("accepts exact nonempty semantic content", () => {
      const content = [
        { type: "text", text: "Meet at 10?" },
        { type: "data", value: { available: true, hour: 10 } },
      ];

      expect(Schema.decodeUnknownSync(PublicContent)(content)).toEqual(content);
    });

    vitestIt("accepts content at the exact canonical byte cap", () => {
      const text = "a".repeat(32_741);
      const content: Content = [{ type: "text", text }];

      expect(Schema.decodeUnknownSync(PublicContent)(content)).toEqual(content);
      expect(Effect.runSync(encodeCanonical(Content, content))).toHaveLength(
        32_768,
      );
      expect(
        Effect.runSync(
          encodeCanonical(Content, [{ type: "text", text: text + "a" }]).pipe(
            Effect.either,
          ),
        ),
      ).toMatchObject({ _tag: "Left" });
    });

    vitestIt.each([
      { case: "empty", content: [] },
      { case: "non-JSON", content: [{ type: "data", value: Number.NaN }] },
      { case: "ill-formed", content: [{ type: "text", text: "\ud800" }] },
      {
        case: "oversized",
        content: [{ type: "text", text: "a".repeat(32_742) }],
      },
      { case: "open", content: [{ type: "text", text: "hello", extra: true }] },
    ])("rejects $case content", ({ content }) => {
      expect(decodingFails(PublicContent, content)).toBe(true);
    });
  });
});
