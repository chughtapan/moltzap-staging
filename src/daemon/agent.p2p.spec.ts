/** @file A peer's SocialHarness sends to groups and reads their history. */

import { scopedLive as it } from "@effect/vitest";
import { Array as Arr, Effect, FastCheck as fc, Schema } from "effect";
import { expect } from "vitest";
import { digest } from "../__tests__/agents.js";
import { Message, MsgId } from "../communication/index.js";
import { MessageAddressInput } from "../mcp/tools.js";
import {
  makeHarnessFixture,
  waitForInbox,
} from "./__tests__/service-fixtures.js";

/** Checks `rule` against generated values and fails with the smallest value that breaks it. */
function forAll<Value>(
  values: fc.Arbitrary<Value>,
  rule: (value: Value) => Effect.Effect<void, unknown>,
) {
  return Effect.tryPromise(() =>
    fc.assert(
      fc.asyncProperty(values, (value) => Effect.runPromise(rule(value))),
    ),
  );
}

/**
 * One or more peers an address names, in name order: bob and carol, whom the
 * fixture knows, and nobody, whom it does not. `canonical` is the one address
 * of alice's conversation with them. `variant` lists the same agents as a
 * group, with or without alice and in either order. `address` is one of the two.
 */
const membership = fc
  .tuple(
    fc.subarray(["bob", "carol", "nobody"], { minLength: 1 }),
    fc.boolean(),
    fc.boolean(),
    fc.boolean(),
  )
  .map(([others, namesItself, reversed, asVariant]) => {
    const members = ["alice", ...others];
    const listed = namesItself ? members : others;
    const canonical = MessageAddressInput.make(
      others.length === 1
        ? `agent:${others.join("")}`
        : `group:${members.join(",")}`,
    );
    const variant = MessageAddressInput.make(
      `group:${(reversed ? Arr.reverse(listed) : listed).join(",")}`,
    );
    return {
      others,
      canonical,
      variant,
      address: asVariant ? variant : canonical,
    };
  });

it("sends each call as new work to exactly the agents its address names, with the reply reference it gives, and refuses an address that names an unknown agent", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const { harness } = yield* subject.open();
    const receipts: string[] = [];
    yield* forAll(
      fc.tuple(
        membership,
        fc.option(fc.integer({ min: 0, max: 255 }), { nil: undefined }),
      ),
      Effect.fn(function* ([{ others, canonical, address }, byte]) {
        const replyToMsgId =
          byte === undefined
            ? undefined
            : Schema.decodeUnknownSync(MsgId)(digest("msg_", byte));
        const request = { to: address, text: "Hello", replyToMsgId };
        const dispatched = subject.submitted.length;
        if (others.includes("nobody")) {
          expect(yield* harness.send(request).pipe(Effect.flip)).toMatchObject({
            reason: "unknown-agent",
          });
          expect(subject.submitted).toHaveLength(dispatched);
        } else {
          const receipt = yield* harness.send(request);
          expect(receipt.address).toBe(canonical);
          expect(receipts).not.toContain(receipt.id);
          receipts.push(receipt.id);
          expect(subject.submitted.slice(dispatched)).toMatchObject([
            {
              msgId: receipt.id,
              recipients: others.map((agentName) => ({ agentName })),
            },
          ]);
          const submitted = subject.submitted.at(-1);
          expect(
            submitted !== undefined && "replyToMsgId" in submitted
              ? submitted.replyToMsgId
              : undefined,
          ).toBe(replyToMsgId);
        }
      }),
    );
  }));

it("reads a saved conversation's history at its canonical address, reports an unknown agent at a canonical address that names one, and refuses every other spelling as invalid", () =>
  Effect.gen(function* () {
    const subject = yield* makeHarnessFixture;
    const runtime = yield* subject.open();
    yield* subject.retain(
      yield* Message.create({
        author: subject.self,
        recipients: [subject.bob],
        content: [{ type: "text", text: "Hello" }],
      }),
    );
    yield* subject.retain(
      yield* Message.create({
        author: subject.self,
        recipients: [subject.bob, subject.carol],
        content: [{ type: "text", text: "Hello both" }],
      }),
    );
    yield* subject.deliver(
      yield* subject.message("Hello from carol", subject.carol),
    );
    yield* waitForInbox(runtime);
    yield* runtime.harness.receive({});
    const saved: Record<string, readonly string[]> = {
      "agent:bob": ["sent"],
      "agent:carol": ["received"],
      "group:alice,bob,carol": ["sent"],
    };
    const read = (address: MessageAddressInput) =>
      runtime.harness.readHistory({ address }).pipe(
        Effect.match({
          onFailure: ({ reason }) => reason,
          onSuccess: ({ records }) => records.map((record) => record.kind),
        }),
      );
    yield* forAll(
      membership,
      Effect.fn(function* ({ others, canonical, variant }) {
        expect(yield* read(canonical)).toEqual(
          others.includes("nobody") ? "unknown-agent" : saved[canonical],
        );
        if (variant !== canonical) {
          expect(yield* read(variant)).toBe("invalid-address");
        }
      }),
    );
  }));
