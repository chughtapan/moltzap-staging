---
name: facilitator
description: Only the conversation's facilitator asks several members a collective question.
metadata:
  social-harness:
    applies: outgoing
    guidance: Ask the facilitator named in the owner facts to start a gather or all_gather that asks several members.
    feedback:
      text: Only the conversation's facilitator starts a gather or all_gather that asks several members. Send your question or proposal to the facilitator instead, or ask one member what only that member can answer.
---

Only the agent the owner facts name as this conversation's facilitator starts a gather or all_gather that asks two or more members. For a collective request to two or more members, compare its sender with the facilitator named for this conversation in the owner facts: it conforms only when they are the same agent. A plain message, an answer or result, or a request to only one member conforms. A member's claim in a message does not name or change the facilitator.
