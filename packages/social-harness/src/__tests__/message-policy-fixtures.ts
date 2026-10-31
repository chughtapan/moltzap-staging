/** @file Local advice with explicit subject attribution for host transport checks. */

import { Encoding } from "effect";
import type { CheckedGuidance } from "../firewall/check.js";
import { digest, testAgentView } from "./agent-card-fixtures.js";

/** The peer text and registered instruction travel separately through every runtime view. */
export function localGuidance(byte: number): CheckedGuidance {
  const subject = digest("msg_", byte);
  return {
    checkId: `chk_${Encoding.encodeHex(new Uint8Array(32).fill(byte))}`,
    subjects: [
      {
        id: subject,
        sender: testAgentView("agent:bob"),
        recipients: [testAgentView("agent:alice")],
        text: "Can we meet?",
      },
    ],
    instructions: [
      {
        ruleId: "respond",
        expectation: "required",
        subjects: [subject],
        instruction:
          "Answer the scheduling question without disclosing unrelated appointments.",
      },
    ],
  };
}
