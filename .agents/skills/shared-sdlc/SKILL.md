---
name: shared-sdlc
description: Plan, review, or hand off Social Harness and MoltZap work using the shared developer workflow.
---

Read `README.md` in `DOCS_INTERNAL_ROOT` first for the everyday workflow, record
choice, handoff and done criteria. Resolve scope with `.records/config.json`,
then load only its overview and the public contracts relevant to the task.

If the private checkout is unavailable, continue work whose public contract is
sufficient and state which internal context is unverified. Keep one concise work
record, prepare a coherent candidate before changing an ADR, and distinguish
implementation, merge and deployment. Use existing task authorization for routine
work rather than asking for another approval at each step.

For filing, use `workflow/reference/records.md`. For decision review or admission,
use `workflow/reference/provenance.md`: missing binding evidence blocks approval;
require independent review and explicit human approval of the exact decision and
review packet, then verify the landing receipt. Never fabricate human approval.
All these paths are relative to `DOCS_INTERNAL_ROOT`, not the code checkout.

Every design and change follows the project checklist (`.agents/skills/checklist/SKILL.md`).

When designing, implementing or reviewing software, load the
`software-architecture` skill. It is shared through the same pinned installation.

Before verifying, merging or releasing, or briefing another agent to, load the delivery skill (`.agents/skills/delivery/SKILL.md`).
Before running or analysing an evaluation batch, read
`data/evals/reference/running-studies.md`.

Use the gstack and Google skills. Review a plan with gstack `/plan-eng-review`.
Review an implementation change with gstack `/ship`'s pre-landing review: its
checklist pass, its specialist reviewers matched to a Google guide where the
package instructions map one, its red team for large diffs, and its
adversarial Claude and Codex passes. That is the one review pipeline; a process skill's own reviewer does
not replace it, and routine changes get no second pipeline. When a plan
completes, record what happened as a worklog in its scope with
`bin/records worklog`; a worklog per task is optional.
