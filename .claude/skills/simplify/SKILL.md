---
name: simplify
description: >-
  Audit and simplify code, APIs and module layout. Use when reducing indirection,
  consolidating services or representations, reviewing names, or removing obsolete
  implementation and tests.
---

# Simplify

Reduce what a reader must understand to use or change the system. Trace a real
caller through the current contract and implementation before choosing a cut.

## Establish what must remain

Find the consumer and required guarantee behind each questioned abstraction.
Separate requirements from choices inherited from the implementation. Explaining
how a mechanism works does not establish why it is needed. Tests that use an
interface do not establish a production need for it.

Read the current plan or PR before resuming or delegating. Keep the chosen
terminology, approved behavior changes, deferrals and protected files there.
Preserve the owner's edits and deliberate cuts. Reopen a settled choice only
with new evidence, and identify that evidence. Keep unrelated repairs with their
owner while continuing the assigned work.

Identify behavior that a proposed cut would lose. Preserve required guarantees
and obtain agreement on changes outside the authorized scope. Product guarantees
and architectural ownership belong in the project's specifications and decision
records. Skills and agent instructions describe how to work.

## Design the contract for its callers

Define the operations callers need, their inputs, results and failure behavior
before reorganizing the implementation. Include the referenced types needed to
understand the interface. Explain obligations, effects and lifetime beyond what
the signature says. Give related operations consistent names and conventions.
Expose a field, setting or notification payload only when a caller acts on it.

Use the caller's concepts across a boundary. Keep transport records, storage
shapes and recovery bookkeeping inside their owner. Retain a separate
representation when it carries a necessary distinction, such as signed bytes or
a permitted view. Remove copies that merely translate between our own modules.

Trace where untrusted input becomes a typed value. Decode it at the boundary
that owns it, then pass typed values internally. A later external boundary
validates its own input. Authentication, signature verification and storage
compatibility checks have distinct jobs from decoding a value's shape.

Put coordination among services in their composition. Look for operations that
make callers shuttle through preparation, checking and completion. Give each
service a complete operation for its responsibility without duplicating another
service's state or lifecycle.

## Keep the implementation easy to follow

Keep a concept's state and operations together. Start with a module and add an
internal folder when substantial implementation warrants it. Split at a useful
responsibility boundary. Keep a sequential path together when small helpers or
forwarding files make it harder to follow. Treat size limits as prompts for
judgment and propose changing a limit that forces worse organization.

Use the repository's existing dependency and resource-lifetime mechanisms.
Introduce a service or object for coherent ownership and lifecycle. Keep ordinary
helpers as functions. Tests should use those dependency mechanisms rather than
require an extra interface in production.

Read a full path with its opening documentation sentence. A newcomer should be
able to predict the responsibility and find it from a concrete task. If naming
remains difficult, inspect the responsibilities before inventing another synonym.
For a naming audit, compare plausible alternatives and use independent readers
to predict meaning before seeing the author's explanation. Keep their mistakes
and disagreements. Verify the chosen name against the source and contract.

## Finish the deletion

Follow a removed feature through its callers, exports, adapters, configuration,
fixtures, generators and documentation. Remove machinery that served only the
old path. Treat intentional work in progress and explicit deferrals as such.
Compatibility removal needs the project's authorization for that release.

Keep tests of public behavior and meaningful invariants. Consolidate cases with
their owning component or service, following the repository's test layout.
Remove assertions that pin private layout, helper calls or retired symbols.
Preserve observable rejection and non-disclosure tests, and independently useful
interoperability vectors. A regenerated snapshot alone does not establish
interoperability.

## Verify and report

Run checks covering the affected callers and public operations, plus the
repository's required gates. Follow its review process and focus review on the
contracts and risks that changed. Repeat checks or review when new changes,
failures or unresolved findings justify it.

Report which concepts, owners, states, representations or transitions disappeared
and which guarantees remain. File and line counts support that account. Moving
the same complexity into fewer files or hiding it behind another helper does
not complete a simplification. State the checks actually run and any remaining
limitations.
