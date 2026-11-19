# Documentation

Choose a folder by what the reader needs from the document.

| Folder | Put it here when the reader needs to… |
| --- | --- |
| [tutorials](./tutorials/) | Learn through a guided exercise with an observable result. |
| [guides](./guides/) | Complete a task, configure a system, or diagnose a problem. |
| [concepts](./concepts/) | Understand concepts, relationships, and design tradeoffs. |
| [reference](./reference/) | Look up exact contracts, settings, bounds, and failures. |

The [specifications](./reference/spec/index.md) are manually maintained
contracts, organized by the responsibility that owns each guarantee. Their
index maps reader questions to the contract that answers them.
API reference is generated from TypeScript and JSDoc into
`out/api`. Follow the [documentation instructions](./guides/contributing.mdx#documentation)
to build it. Keep bugfix rationale and investigation history in issues, pull
requests, and commits. Internal decisions belong in the maintainer records.

Keep each fact with its owning document and link to it from other pages.
Create a page when a reader needs it. Use the
[writing guide](../.agents/skills/docs/SKILL.md) to choose its scope and review
its prose.

[docs.json](./docs.json) configures the site navigation and shared values.
`snippets/` holds reusable Mintlify content, and `assets/` holds images.
For an introduction to the project, read [the overview](./concepts/overview.mdx).
