# Router package

Extends the workspace-root `AGENTS.md`. The five-layer constitution in
`docs/reference/constitution.md` and the Router sections of
`docs/reference/spec/communication/protocol.md` govern this package.

`@moltzap/router` owns opaque message delivery, the volatile global feed,
polling, process cursors, authenticated Router HTTP, the
Router process, and `sharness-router`. Its only production workspace dependency
is `@moltzap/social-harness`.

Router remains content-blind and volatile. It does not own conversations,
membership semantics, certified history, persistence, replay, catch-up,
re-anchor policy, tasks, norms, trust, institutions, or runtime MCP behavior.
It never queries a product Ledger or privileged governance service.

Preserve the admitted wire bytes, routes, bounds, authentication, ordering,
typed failures, and process behavior unless their governing authority changes.
Publication follows `docs/reference/spec/releases.md` → Publication and versions: this package stays private.

Run build, typecheck, test, integration, lint, and architecture checks through
Nx:

```sh
pnpm nx run @moltzap/router:build
pnpm nx run @moltzap/router:typecheck:tests
pnpm nx run @moltzap/router:test
pnpm nx run @moltzap/router:lint
pnpm nx run @moltzap/router:arch:check
```
