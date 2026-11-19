/** @file A firewall saves a decision on a group draft for each recipient, and applies no contract to a group message. */

import { scopedLive as it } from "@effect/vitest";
import { Effect, FastCheck as fc } from "effect";
import { expect } from "vitest";
import { Message } from "../communication/index.js";
import {
  contractOf,
  evidence,
  form,
  noFields,
  peer,
  policy,
  privacy,
  withFirewall,
} from "./__tests__/firewall-fixtures.js";
import { Firewall } from "./service.js";

const group = [peer("bob", 2), peer("carol", 3)] as const;

/** A contract whose one act, `open`, starts and ends an instance. */
const exchange = contractOf("exchange", [
  {
    ...form,
    act: "open",
    by: ["Sender"],
    fields: noFields,
    starts: true,
    ends: true,
  },
]);

it("saves the refusal of a group draft once for each recipient", () =>
  withFirewall(
    {},
    (firewall, self) =>
      Effect.gen(function* () {
        const decision = yield* firewall.checkOutgoing(
          yield* Message.create({
            author: self,
            recipients: group,
            content: [{ type: "text", text: "The private record." }],
          }),
          evidence,
        );
        yield* decision.save;
        expect((yield* firewall.readDecisions({})).decisions).toMatchObject(
          group.map((recipient) => ({ decision: "refused", peer: recipient })),
        );
      }),
    (agentKey) => Firewall.layer(agentKey, policy([privacy])),
  ));

it("applies no contract to a group message, whatever its text", () =>
  withFirewall(
    {},
    (firewall, self) =>
      Effect.forEach(
        fc.sample(fc.string({ minLength: 1 }), 30),
        (text) =>
          Effect.gen(function* () {
            const decision = yield* firewall.checkOutgoing(
              yield* Message.create({
                author: self,
                recipients: group,
                content: [{ type: "text", text }],
              }),
              {
                ...evidence,
                protocolHistory: { messages: [], complete: true },
              },
            );
            expect(decision.kind).toBe("allowed");
          }),
        { concurrency: 1, discard: true },
      ),
    (agentKey) => Firewall.layer(agentKey, policy(), undefined, [exchange]),
  ));
