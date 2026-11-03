---
name: software-architecture
description: >-
  Design, implement and review software using shared architecture and engineering
  practices. Use for service boundaries, API and data design, code organization,
  implementation readability, testing, documentation and architectural audits.
---

# Software architecture and engineering practices

Build systems whose contracts, ownership and ordinary execution paths are easy
to understand. Apply these principles to new work as well as existing code.
Use the repository's language, framework and tooling conventions for concrete
implementation choices. Keep product-specific guarantees in its specifications.

## Begin with the problem and its consumers

Identify the system's goal, inputs, outputs, consumers and constraints. Trace
real use cases before choosing components. Distinguish requirements from choices
inherited from an implementation. Explaining how a mechanism works does not
establish why it is needed.

Look for an existing dependency, platform feature or service that already owns
the responsibility. Use its supported interface. Introduce custom machinery
when it supplies a needed capability, and explain the tradeoff against the
existing option. Design extensions for actual variation among consumers.

## Give responsibilities coherent boundaries

Group state and the operations that govern it under one owner. A caller should
understand a component from its contract without learning its storage,
transport or recovery internals. Repeated exceptions and long explanations can
reveal a misplaced responsibility. Investigate the boundary before adding prose.

Keep dependencies directed toward the contracts they use. Put coordination among
components in their composition, with complete operations for each component's
responsibility. Avoid making callers shuttle through another component's
preparation, validation and completion internals. Keep composition small by
giving policy and lifecycle decisions clear owners.

Use services or objects for coherent capabilities, state and resource lifetimes.
Keep ordinary helpers as functions. Use the framework's existing dependency
and resource-management facilities. Tests should use those facilities rather
than require another interface in production.

## Define APIs before implementation details

Describe caller actions, inputs, results, failures and lifetime before planning
the implementation. Include the referenced types needed to understand the
contract. State observable ordering, retry, cancellation and recovery behavior
where callers depend on it. Use diagrams to clarify meaningful flows.
Explain what identifiers name, which event and clock a timestamp records, and
whether a notification carries work or signals that work is available.

Use familiar domain names and consistent conventions across related operations,
including list, lookup and pagination. Expose a field, setting, state or
notification payload when a consumer acts on it. Keep internal budgets and
bookkeeping with their owner. Represent the domain's meaningful states directly.

Keep a finite set closed when the system owns its members. Preserve the
extensibility of values owned by providers, platforms or other external systems.
Separate configurable policy from the mechanism that applies it. Put settings
where the people responsible for changing them can maintain them.

## Own data and validation

Give each fact, invariant and authoritative state one owner. Derive other views
where practical. Use the caller's semantic values across component boundaries.
Keep wire records, database rows and recovery bookkeeping inside their owner.
Retain distinct representations when their contracts differ, such as signed
bytes and content a particular reader may see.

Decode untrusted input at the boundary that owns it, then pass typed values
internally. A later external boundary validates its own input. Authentication,
signature verification, authorization and storage compatibility have distinct
jobs from checking a value's shape. Enforce each guarantee where its owner can
establish it.

Identify which state survives interruption and which changes must commit
together to satisfy the contract. Keep that ownership explicit without exposing
storage machinery to unrelated consumers. Introduce persistence, retries and
recovery states for required behavior rather than hypothetical failure modes.

## Organize for readers

Keep a concept's types, state and operations together. Start with a module and
introduce an internal folder when substantial implementation warrants it.
Split at useful responsibility boundaries. Keep sequential work together when
small helpers or forwarding files make its execution harder to follow. Treat
size limits as prompts for judgment and propose changing a limit that forces
worse organization.

Read a full path with its opening documentation sentence. A newcomer should be
able to predict the responsibility and find it from a concrete task. Prefer
plain, precise names over unexplained qualifiers. If naming remains difficult,
inspect the responsibilities before inventing another synonym. Carry a rename
through its consumers and examples.

For a naming audit, compare plausible alternatives and have independent readers
predict meaning before seeing the author's explanation. Keep mistakes and
disagreements, and verify the chosen name against the contract and source.

## Test behavior through the owning contract

Test observable behavior and meaningful invariants through public interfaces.
Use service tests for the behavior a service owns and real integration tests for
interactions between components. Use properties when they express an invariant
across meaningful inputs. Follow the repository's test layout and keep related
cases with their owner.

Prefer assertions that survive internal refactoring. Remove tests that merely
pin private layout, helper calls or retired symbols. Preserve observable
rejection and non-disclosure tests, and independently useful interoperability
vectors. A regenerated snapshot alone does not establish interoperability.
An interface's use by tests does not establish a production need for it.

## Write documentation at the right level

Choose the reader and purpose before organizing a page. Begin with its subject
or practical outcome, then develop concepts before details. For a system
description, explain goals, inputs, outputs and how its components connect them.
Use diagrams where they help readers follow that flow.

Maintain specifications as contracts above the implementation. Generate API
reference from its owning declarations and polished caller-facing documentation.
Explain implementation only where clear code leaves a useful invariant or
rationale unstated. Put investigations and change history in issues, PRs and
commits. Record durable design decisions with their reasons and consequences.

Give each fact one authoritative home. Cut repeated inventories and details
unnecessary for the reader's task. Replace an unclear passage instead of adding
explanations around it. Prefer active voice, plain words and connected sentences.
Review the result with the source closed, then check its claims against source.

## Evolve the system deliberately

Keep current scope, chosen terminology, approved behavior changes, deferrals and
protected files in the existing plan or PR. Consult it before resuming or
delegating. Preserve the owner's edits and deliberate cuts. Revisit a settled
choice with new evidence and identify that evidence explicitly.

For a boundary or layout change, show the resulting contracts, dependency
direction, flow and affected file tree. Identify behavior a proposed change
would lose and obtain agreement on changes outside the authorized scope.
Follow the project's compatibility policy rather than assuming a reset or
preserving an unused path by default.

Follow a removed feature through its callers, exports, adapters, configuration,
fixtures, generators and documentation. Remove machinery that served only the
old path. Respect intentional work in progress and explicit deferrals. Measure
improvement by the concepts, owners, states and transitions readers must
understand. File and line counts provide supporting evidence.

Run checks covering affected consumers plus the repository's required gates.
Match review to the changed contracts and risks through its review process.
Repeat checks or review when new changes, failures or unresolved findings
justify it. Report what changed, why it helps, the checks actually run and any
remaining limitations.
