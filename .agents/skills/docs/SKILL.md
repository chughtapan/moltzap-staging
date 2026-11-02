---
name: docs
description: |
  Write, review, and maintain Social Harness documentation. Covers reader
  purpose, system descriptions, organization, distillation, and generated API reference.
  Load before editing documentation or flow diagrams.
---

# Documentation

Write documentation that helps readers learn, complete tasks, find precise
answers, or understand the system. This guide records the owner's writing
preferences and the repository's publishing conventions. Refine it as we
review documents together. `AGENTS.md` defines documentation authority.

## Choose the reader's purpose

[Diátaxis](https://diataxis.fr/compass/) distinguishes two dimensions: learning
or working, and action or knowledge. Choose the reader's need before choosing
the document's structure.

| Reader needs | While learning | While working |
| --- | --- | --- |
| Practical action | Tutorial: learn through a guided exercise | How-to guide: accomplish a real task |
| Knowledge | Explanation: understand concepts and relationships | Reference: find precise facts |

- A [tutorial](https://diataxis.fr/tutorials/) guides a learner through a
  concrete exercise. Choose a reliable path, show useful results along the
  way, and introduce concepts as the learner encounters them. Defer optional
  alternatives so the learner can follow without prior expertise.
- A [how-to guide](https://diataxis.fr/how-to-guides/) helps someone who knows
  their goal complete a task in their own environment. State prerequisites,
  necessary choices, steps, and verification. Include branches when the task
  requires judgment, and link to background explanations.
- [Reference](https://diataxis.fr/reference/) supports lookup during work.
  Organize facts by the contract or interface they describe. Use consistent
  names and structures so readers can find exact behavior, bounds, defaults,
  and failures without reading the page from the beginning.
- [Explanation](https://diataxis.fr/explanation/) develops understanding of a
  bounded subject. Connect concepts, assumptions, constraints, and
  consequences. Explain why a property matters or a design works. Link to
  decision evidence where it helps, while keeping investigation history in
  its owning record.

Use these forms where readers need them. A short definition or example can
orient a reference reader. Move a sustained lesson or discussion to its own
page when it interrupts that purpose. Create pages for actual needs rather
than four pages for every feature.

The site uses `guides/` for how-to guides and `concepts/` for explanations.

Choose a reader as well as a document type. Application developers need to
know which collaboration responsibilities the harness supplies and which they
must build. Agent owners need to connect, approve what their agent may do, and
withdraw that approval. Host integrators need send, receive, and recovery
contracts. Trace the chosen reader's whole task across pages. Distinguish an
available workflow from a proposed one before giving instructions.

## Choose the source of truth

Reader purpose and source ownership are separate choices. A manually maintained
wire specification and a generated API reference both serve reference needs.
An explanation can discuss either one's concepts. Generation does not supply
the tutorials, how-to guides, or explanations readers also need.

Maintain specifications by hand, one level above the implementation. Describe
the participants, exchanges and guarantees so a reader can predict an
interaction and judge a different implementation without opening code.
Introduce concepts before identifiers. Cut function names, internal objects,
storage layouts and execution steps that add no contract.

Match precision to the contract. Signed wire formats need exact bytes and
validation rules. An evolving architecture may establish responsibilities
before mechanisms. Keep the assumptions and failure conditions that limit a
guarantee, and state unresolved choices. A discrepancy in code requires a
decision before the specification changes.

Give each layer one coherent contract. Identify what it receives, what it
guarantees, what it returns, and what its callers must handle. Assign each
guarantee to one layer and link to that owner from other specifications. Use
the same responsibility boundaries in specifications and code. A disagreement
between them needs a design decision, not another explanation. Organize around
those responsibilities rather than reproducing the current file tree.
Keep a module's behavior and representation together when readers need both
to understand or implement it. A cross-reference can expose coupling: another
module should rely on the public input and output contract, not follow links
into its protocol or storage machinery. Audit that dependency before adding
another link or creating a smaller page.

Generate API reference from the declarations and schemas that own the API.
Derive types, validators, and language bindings from those definitions wherever
they would otherwise repeat the same contract. TypeDoc generates documentation
from TypeScript and JSDoc. Polish that source prose with the same care as a
hand-written page. Explain what callers need beyond the signature, such as
preconditions, ownership, lifetime, effects, and failures. Link to the owning
specification for protocol guarantees. Keep export inventories and copied
signatures out of hand-written specs. Put caller obligations on service methods
and public overload declarations. Comments on private helpers or implementation
bodies may disappear from the generated reference. Verify that a generated page
exists before removing its only useful explanation elsewhere.

Explain the actor, event and evidence behind a value when its type leaves
those unclear. A timestamp needs the event and clock it records. A signed
decision needs a clear distinction between what its author asserts and what
the signature proves. A notification needs to say whether it carries work or
tells its subscriber to read saved work. Describe permissions by current
authority, not by an object's origin or who used to control it.

Document implementation only when the explanation adds something a reader
cannot obtain from clear code. Implementation comments explain what the code
leaves unclear, such as a non-obvious invariant, in present tense.
Bugfix rationale, investigation outcomes, failed approaches, and change
history belong in issues, PR descriptions, and commit messages. Durable
architecture decisions belong in ADRs. Remove file tours,
dependency inventories, and narration of control flow during simplification.

Useful examples have different scopes:

- [MCP's protocol specification](https://modelcontextprotocol.io/specification/2025-11-25/basic)
  defines interoperability rules separately from its SDK reference.
- [ACP's protocol overview](https://agentclientprotocol.com/protocol/v1/overview)
  explains participants and message flow. Its
  [TypeDoc reference](https://agentclientprotocol.github.io/typescript-sdk/)
  documents the TypeScript library.
- [Exoharness's specification](https://github.com/exoharness/exo/blob/main/exoharness/docs/spec.md)
  develops concepts and responsibilities at an architectural level.
- [OpenClaw's schema workflow](https://docs.openclaw.ai/concepts/typebox)
  derives validation, schemas, and native models from typed definitions.

## Compose for the reader

Begin each document with a topic sentence that establishes its subject or
practical outcome. Develop it at the level and in the order the reader's
purpose requires. Introduce a card before its fields, authentication before
its signature profile, and a task before its commands.
The [MCP introduction](https://modelcontextprotocol.io/docs/getting-started/intro)
provides an example of moving from a definition to concrete uses and reader
paths.

For a specification or system description, begin with the system's goal and
scope. Then explain:

1. Inputs: what the system receives and the starting conditions it requires.
2. Outputs: what it produces, changes, or makes observable.
3. Transformation: introduce each component's responsibility, then trace how
   inputs become outputs through those components. Use a diagram when it makes
   the flow or relationships easier to understand.
4. Component detail: develop each component at the level the reader needs.
   Apply the same goal, input, and output questions within a component when
   further decomposition helps explain it.

Choose headings and depth to suit the document. A short description can answer
these questions in a paragraph. In a specification, the components describe
contractual responsibilities and required behavior. Keep observable ordering,
state changes, and failure conditions where they affect that contract.

Apply this reasoning across technical writing. Tutorials and how-to guides
connect a starting point to an observable result through actions. API prose
explains the caller's obligations and effects beyond the generated signature.
Explanations develop the relationships that account for the behavior. Keep
each page at its chosen level of abstraction and link to deeper detail when
the reader needs it.

Develop one topic per paragraph. Let its opening sentence establish that
topic and let the following sentences explain or support it. Keep related
words close together and use parallel forms for parallel ideas. These
composition principles come from
[The Elements of Style](https://www.gutenberg.org/files/37134/37134-h/37134-h.htm).
Use the installed `google-documentation-guide` for the broader documentation
guidance.

## Organize and distill

Choose an organizing principle that helps the reader navigate the subject:
tasks, input-to-output flow, a lifecycle, conceptual dependencies, or lookup
keys. As the material evolves, change that principle when it no longer fits
the reader's need. Split a section when it serves an independently useful
purpose or needs a different level of detail. Merge sections that answer the
same question. Reorder material to put prerequisites before dependent ideas.
Settle the structure before polishing sentences.

Put behavior shared by several modes before their differences. A reader who
chooses one mode should not have to read another mode's section to understand
a common result.

Apply DRY and distillation together. Give each fact one authoritative home and
link to it where readers need it. Before replacing a passage with a link,
verify that its target covers every actor and guarantee being removed.
Track those guarantees during a specification reduction. Record each baseline
clause and its retained clause or existing owner outside the public prose.
Recheck referring pages when that owner changes later in the same series.
Keep material that helps the reader
understand, act, decide, verify or satisfy a contract. Delete material with no
such use. Move it only when another document needs it. A rarely needed
recovery rule can still be essential.

Improve exposition by removing details and adjectives unnecessary for the
reader's task. Replace an unclear passage instead of adding another
explanation. Cut repeated summaries and callouts. Keep context beside the
action or example that needs it, and links that help readers continue their
task. Repetition within an exercise can teach a distinction, such as reading
a message twice to show that reading does not acknowledge it.

Treat a hard-to-explain boundary as a design question. Repeated exceptions,
asides, and caveats can reveal duplicated ownership or an unnecessary layer.
Inspect the callers and the contract before adding more explanation. Simplify
the implementation when its guarantees permit it, and surface a conflicting
requirement with the evidence needed to decide it.

## Revise the prose

Choose names that carry the meaning. A precise, familiar name can replace a
paragraph of explanation. Prefer plain words and direct verbs over elaborate
phrases. Apply this care to headings, files, folders, classes, methods,
variables, and APIs as well as prose. Keep distinct concepts distinct, and
give the same concept the same name wherever readers encounter it.

Carry a concept rename through its fields, types, examples and prose. Check
neighboring layers for a name that already means something else. Describe the
responsibility that distinguishes each value before inventing another synonym.
When two fields repeat one value, question the representation before writing
an explanation for the duplication.

Read as someone arriving from the Social Harness paper. Connect its concepts
to the implemented system using consistent terms. Explain a necessary
difference where readers first encounter it. Preserve the distinction between
the paper's goals and the guarantees the implementation currently provides.

Repeated qualifiers or explanations can reveal a misleading name. Inspect the
concept, its callers, and its contract before proposing a replacement. For a
deeper rename or API change, record the current term, proposed term, meaning,
reader benefit, and effect on callers or compatibility. An audit proposal
does not authorize that change.

Read the complete file path with the first sentence of its file-level JSDoc.
A newcomer should be able to predict what the file does and find it from a
concrete task.
If that needs unexplained qualifiers or several unrelated clauses, inspect the
file's responsibilities before renaming it. Split, merge or move code when its
organization causes the confusion. A longer name or comment cannot repair a
folder that mixes unrelated owners.

Test names in their surroundings. Read a path from its parent folder down to
the file, and a sentence from the actor through the action to the result.
Ask what a newcomer can infer at each step. Replace an unexplained qualifier
with the action it describes: “Alice sends a message to Bob” introduces the
idea more clearly than “an addressed send.” Keep a technical qualifier when
it distinguishes cases the reader already understands.

For a naming or organization audit, generate several plausible alternatives
before choosing. Give independent readers the same tasks and competing names
without the author's preference or explanation. Ask them to predict meaning
and find information before voting. Record wrong predictions, disagreements,
and compatibility costs alongside votes. Agent reader tests supply review
evidence, not proof of usability with people. A popular name still has to
describe the contract accurately.

- Name the actor and use an active verb: “Registry rejects expired requests.”
  Address readers as “you” when giving instructions.
- State the behavior directly. Remove catalogues of rejected designs and
  rhetorical contrasts such as “not X, not Y, but Z.” Retain a precise
  exclusion when it defines a security boundary or rejected input.
- Remove stock transitions, self-praise, inflated claims, vague qualifiers,
  and repeated assurances. State uncertainty only when the evidence leaves
  something unresolved, and identify what remains unknown.
- Prefer complete, connected sentences. Rewrite prose that relies on em
  dashes, semicolons, parenthetical asides, or fragments to join its thoughts.
  Concision means keeping useful meaning, not making every sentence short.
- Introduce precision when the reader needs it. Field names, byte limits,
  library names, and signature profiles do not belong ahead of their purpose.
  Preserve exact commands, identifiers, and protocol requirements where they
  govern an action or result.
- Define an unfamiliar term before relying on it, then use one name for it.
  Use “Social Harness” for the project and each package or executable's exact
  name for that artifact. Bold only UI labels and keyboard keys.

## Review each document and the set

Read the complete draft after editing it. Check that the opening establishes
its purpose, each section serves that purpose, and each paragraph adds
something the intended reader needs. Watch for changes of purpose within a
page, such as a setup procedure turning into a catalogue of every setting.
Rewrite awkward passages in place. Preserve the owner's edits and deliberate
cuts. Rewrite an adequate passage only to fix a concrete reader problem or an
agreed contract change. Reorganizing a document does not justify restoring cut
material or restating it under another heading.

Review a specification with the source closed. Ask the reader to identify
the actors, follow a concrete input through the system, explain the result,
and predict what happens on failure or interruption. Mark every step that
requires a code lookup, an unexplained internal term, or knowledge of this
conversation. Repair the explanation where the gap occurs.
Choose the questions and expected answers from the contract before rewriting.
Give the reader the questions and public prerequisites, and ask them to cite
the clauses supporting their answers. Keep the expected answers and the
author's explanation out of that reader's context.

Read the affected documents together. For each folder, follow the paths
readers use to learn, complete tasks, look up facts, and understand concepts.
Review the applicable guides, explanations, specifications, generated
reference, and source comments together. Check that useful material has a
clear home at the right level of detail and that links help readers continue.
Regenerate the reference after source edits and read the result.

Review correctness separately from readability. Check implementation claims
against source and public contracts, including guarantees affected by cuts or
moves. Check numerical examples and derived limits against their defining
representation. Use the Social Harness paper to resolve questions of intent, and bring
unresolved design choices to the owner.

Load the installed Google guides that address the area under review:

- `google-technical-writing` for audience, organization, and examples.
- `google-api-documentation` for caller-facing descriptions. Follow
  [Google's API documentation guidance](https://google.aip.dev/192) where it
  fits the interface and our TypeDoc conventions.
- The relevant API design guide for resource names, methods, data models,
  pagination, asynchronous work, errors, retries, or compatibility. Apply it
  to the actual interface. A resource-oriented HTTP convention does not by
  itself justify changing an Effect service or messaging protocol.
- `google-adr` and `google-design-docs` for decision records and design
  proposals. Use the [ADR guidance](https://docs.cloud.google.com/architecture/architecture-decision-records)
  to assess context, alternatives, consequences, and rationale. The records
  skill governs location, evidence, status, and approval.

An audit should make decisions easier. Trace a concrete reader or caller
problem through the owning contract, implementation, and consumers. Challenge
whether each responsibility, abstraction, state, and configuration choice is
needed. Look for competing owners, repeated transformations, and special cases
that disappear when the boundary changes. Naming follows that analysis.

Recommend a specific change and explain why it beats a credible alternative.
Show the resulting input-to-output flow, identify what is removed or
consolidated, and state the compatibility and safety costs. Give observable
acceptance criteria. Separate verified behavior from inference and identify
the evidence that could change the recommendation. An unresolved question
should name the missing evidence and how to obtain it.

Rank findings by their effect on users, correctness, and system complexity.
Keep routine repairs separate from architectural decisions. Separate current
behavior, contract requirements, and future intent. Keep proposals and
investigation results in the owning issue, PR, or decision record.

Before a docs PR merges, give a reader without source or session context a
task suited to the document. Have a learner complete a tutorial, a practitioner
apply a how-to guide, a reference reader find an exact answer, or an explanation
reader reason through an example. Use
[doc-coauthoring's reader test](https://github.com/anthropics/skills/blob/main/skills/doc-coauthoring/SKILL.md#stage-3-reader-testing)
to find where the draft makes them guess. Have them follow the changed paths,
including their commands and links. Fix blockers or record them as issues
linked from the PR. State who reviewed the draft, what they actually tried,
and what remains unverified. Use context the owner already supplied.

## Examples from our reviews

- The identity spec opened with a source location, a milestone label, and
  version notation. Delete the metadata. Explain notation beside its example
  and compatibility in its owning section.
- “Standards substrate” listed libraries and rejected formats before
  explaining identity. Remove it. Keep each standard with the encoding rule
  it determines.
- A Registry source README described `http.ts` as HTTP handling and
  `storage.ts` as persistence. Delete the inventory. The reason a failed
  registration still consumes its replay nonce belongs with that guarantee.
- The Registry README summarized a linked guide's setup, health checks, and
  storage caveat. Keep the task link and let the guide carry the procedure.
- TypeDoc showed signing inputs as unlinked type names. Its native
  [`@inline` tag](https://typedoc.org/documents/Tags._inline.html) now renders
  their fields from the source types. Keep caller constraints in JSDoc and
  let the generator produce the field lists.
- Daemon setup and first messages serve different needs. Keep installation,
  registration, recovery, and diagnostics in the how-to. Put the guided
  send-and-read exercise in a tutorial that links to that setup.

## Write a README

A README helps someone understand what they have found, decide whether it
fits their task, and begin using or changing it. Identify who arrives at this
page and what they already know. A package reader may arrive directly from
npm without seeing the repository's introduction.

For software users, establish the purpose and a concrete use before presenting
detailed choices. Show an ordinary supported interaction early, including its
result. A short example, transcript, or diagram can make the contribution of
the software clear. State the prerequisites an example assumes and link to
their setup. Call it a quickstart only when it gives a complete path to the
promised result.

Choose a recommended starting path and explain alternatives by the needs
they serve. Put status or limits that affect adoption beside that choice.

Scope the README to its audience:

| Location | Reader needs |
| --- | --- |
| Project root | Understand the problem, see the project in use, and choose a starting path. |
| Library or adapter package | Understand what this package adds, install it, and use its public interface in the required host or environment. |
| Service or image | Understand what runs, supply its prerequisites, start it, and recognize a healthy result. |
| Source directory | Find context needed to change this area that clear code and existing documentation do not supply. Remove a README that only inventories files or repeats enforced rules. |

Use headings that promise what their sections deliver. An installation section
provides installation instructions or clearly identified routes to them. Keep
contributor setup distinct from using a published artifact. Link to the owning
guides and reference for complete procedures and contracts. Keep enough
context on the page for its intended reader to understand the purpose and
next step.

Read successful examples selectively. [Zod](https://github.com/colinhacks/zod/blob/main/packages/zod/README.md#what-is-zod)
connects schema, input, and validated result. [Flask](https://github.com/pallets/flask/blob/main/README.md#a-simple-example)
shows a small application and how it starts. [uv](https://github.com/astral-sh/uv/blob/main/README.md#features)
shows commands and results for named tasks. The
[MCP TypeScript SDK](https://github.com/modelcontextprotocol/typescript-sdk/blob/main/README.md)
separates client and server starting paths. Borrow the technique that serves
our reader and apply the existing prose and distillation principles.

For reader review, ask someone arriving directly at the README to explain its
subject and choose the next action. When the README includes a usage example,
have them identify its starting conditions and result. For a promised
quickstart, follow through to that result. A source-directory reader should
be able to use the context to guide a code change. Check links and examples
in the places the README appears, including GitHub and package registries.
Verify that published links work as well as local paths.

## Site pages

Site pages have `title`, a one-sentence `description`, and `read_when` with
two to four reader situations. A guide's description names its outcome.
Mintlify renders the title as H1, so body headings start at H2.

```yaml
---
title: Connect an agent
description: Connect your agent to a network and send its first message.
read_when:
  - You have an agent and want it on a network
  - Your agent connects but its messages do not arrive
---
```

Put prerequisites before dependent actions and number steps by dependency.
Keep commands with their expected results and likely failures. State a
quickstart's expected time cost and observable result, such as a delivered
message. Troubleshooting starts with the symptom or exact error, then explains
the diagnostic, result, remedy, and verification.

Keep troubleshooting headings short enough to scan. Put long diagnostic
strings in code blocks beside their remedies, and preserve the anchors that
diagnostic links use.

Use optional detail only where a reader benefits from it. Accordions can
hold advanced material. Reserve warnings for data loss or security. Include
notes and follow-up links only when they add useful context.

Hand-written `.mdx` pages use root-relative site links for content the site
hosts. The `docs/reference/spec/*.md` pages also serve GitHub readers and can use relative
file links. Choose the folder from the
[documentation map](../../../docs/README.md), and use the same reader purposes
in site navigation. When a page moves, update repository links and code
diagnostic URLs to its new home.

## Examples and references

Use the smallest complete example of a plausible task. State the working
directory, prerequisites, and replacements before the command. Keep names,
ports, and paths consistent. Use safe identities and obvious secret
placeholders. Prefer a supported CLI to hand-edited configuration.

Put one command in each fenced block with a language tag and no shell prompt.
Use `<angle-bracketed>` placeholders. Label partial configuration, pseudocode,
and representative output. Say what success looks like and show output when
the reader needs to compare it. Verify examples against the implementation.

Configuration references give exact names, accepted values, requiredness,
defaults, and purpose. Explain units, bounds, precedence, activation, and
interactions where they affect use. Document exit status and output streams
when scripts depend on them. Put platform and installation limits beside
the affected instructions.

Cite code by symbol name, such as `file.ts → handleFrame`. Use line numbers
only in PRs, issues, and reviews, which describe a fixed revision.

## Source ownership and checks

Build the API reference with `pnpm docs:api`. TypeDoc writes `dist/api`.
The [missing-exports plugin](https://github.com/Gerrit0/typedoc-plugin-missing-exports)
documents locally defined types referenced by public methods under `Referenced
types`. Check their links and descriptions without broadening package exports
to satisfy the generator. Exclude dependency build outputs so each package's
source owns its reference.
Inspect derived type pages as well as their links. A page that exposes schema
construction types can hide the value callers receive. TypeDoc's
[`@useDeclaredType`](https://typedoc.org/documents/Tags._useDeclaredType.html)
can render a derived value type from the compiler. Check the output before
keeping it. When only the value type is public, give its internal schema a
distinct name so the generator does not publish two misleading pages for one
type. Keep the type derived from its schema.
Shared site values live in `docs/docs.json` variables, and reusable content
lives in `docs/snippets/`. Check site links with `pnpm -s docs:check`.

Add a diagram when it makes a relationship or invariant easier to understand.
Keep it with the explanation that owns that fact. API diagrams belong in the
owning symbol's JSDoc. `pnpm docs:check:mermaid` checks the Mermaid dialect
GitHub accepts. TypeDoc displays these diagrams as code blocks.
