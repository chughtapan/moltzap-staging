---
name: docs
description: |
  Write, review, and maintain Social Harness documentation: the README, the
  onboarding skills, the specification and the text of comments, commits and
  pull requests. Load before editing any of them.
---

# Documentation

The repository has four kinds of documentation. Decide which one a fact
belongs to before writing it, and give each fact one home. `AGENTS.md`
defines documentation authority.

| Kind | Where | Reader | Holds |
| --- | --- | --- | --- |
| Explanation | `README.md` | Someone deciding whether to use Social Harness | What it is, why it exists, how it works, its limits |
| Setup | `skills/` | An agent doing the setup for its owner | What a working result needs and what to ask the owner |
| Contract | `docs/reference/spec/` | Someone predicting or implementing behaviour | What the daemon guarantees and refuses |
| Authority | `docs/reference/constitution.md` | Anyone resolving a conflict | The layers and what each owns |

## README

The README explains the product: a front door a business puts in front of its
agent, where norms become protocols that a firewall enforces. It establishes
the purpose, shows one real exchange with its result, states the limits that
affect a decision to adopt, and points to the skills. It carries no setup
steps, settings or commands. Take examples from a run that happened.

## Skills

The three skills in `skills/` are the only setup instructions. A person never
follows them by hand, and nothing else in the repository repeats them.

- A skill says what must be true of a working result, the facts only the
  daemon's contract can supply, and what to ask the owner. It does not script
  the steps, and it assumes no provider and no agent host.
- Put a fact at the lowest level that reaches the agent in time: an MCP tool
  description or an error message before a line in a skill. If a fresh agent
  does the right thing without a line, leave the line out.
- Signing in, paying and supplying a secret are the owner's. A skill never
  asks for a secret in chat.
- When code changes a tool, setting or variable a skill names, change the
  skill in the same pull request.
- Change a skill from evidence: an agent that followed it and what it got
  wrong or had to guess.

## Specification

The specification states behaviour this project owns, one level above the
implementation: the participants, what the daemon receives, what it
guarantees, what it returns and what its callers must handle. A reader should
predict an interaction without opening code.

- In a code change the specification may only lose text or get a minimally
  corrected sentence. Adding to it needs the owner's approval.
- Never restate MCP, OAuth or a library. Link to the owner of that behaviour.
- List every removed, corrected or added specification sentence under its own
  heading in the pull request body.
- Give each guarantee one page and link to it from the others. Before
  replacing a passage with a link, check the target covers everything removed.
- Cut function names, internal objects, storage layouts and execution steps
  that add no contract. Exact fields and bounds stay where another program
  depends on them.
- A discrepancy between code and specification needs a decision, not another
  explanation.

The constitution changes only with the owner's approval. When a change makes
one of its sentences wrong, report the sentence instead of editing it.

## Comments, commits and pull requests

Repository text is for a reader who arrives cold.

- Describe the code as it is. Leave out what it used to be, what was tried,
  and words such as "now", "no longer", "new" and "previously".
- Write nothing addressed to the maintainer and nothing that depends on a
  conversation.
- A comment explains what the code leaves unclear, such as a non-obvious
  invariant. Remove file tours and narration of control flow.
- A JSDoc says what callers need beyond the signature: preconditions,
  ownership, effects and failures.
- A pull request body says what the change does and what a reader of the
  diff would otherwise have to work out.

## Prose

- Open with the point: the subject or the outcome. Develop one topic per
  paragraph.
- Name the actor and use an active verb: "The daemon refuses an expired
  token."
- Use one name for one concept everywhere, and define an unfamiliar term
  before relying on it. Use "Social Harness" for the project and each
  package's or executable's exact name for that artifact.
- Prefer plain words and complete, connected sentences. Rewrite prose that
  leans on em dashes, semicolons, parenthetical asides or fragments.
- State behaviour directly. Remove rejected designs, "not X but Y" contrasts,
  stock transitions, praise and vague qualifiers. Keep an exclusion that
  defines a security boundary.
- Improve a passage by removing what the reader does not need, not by adding
  another explanation. A boundary that is hard to explain is a design
  question: inspect the code before writing more.

## Check

Read the whole document after editing it, as its reader. For a skill, the
test is an agent with nothing but the skill. For the specification, close the
source and follow one input through to its result and its failure.
Check claims against the code. Run `pnpm -s docs:check` for links.
