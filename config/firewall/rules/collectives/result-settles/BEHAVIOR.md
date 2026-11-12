---
name: result-settles
description: A collective result settles the question it asked; act on the result rather than asking again.
metadata:
  social-harness:
    applies: outgoing
    guidance: "A collective result settles the question it asked; act on the result rather than asking again."
    feedback:
      text: "A collective result already answered this question. Act on that result instead of asking again."
---

A collective result settles the question its gather or all_gather asked. After a result in this conversation answered a question, an agent does not ask that question again, by message or by a new gather or all_gather, unless the result held no usable answer or the question has changed.
