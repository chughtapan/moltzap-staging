# Changing collective operations

A requested form crosses two validation boundaries. [The host contracts](./contracts.ts)
retain the caller's property descriptions for diagnostics. The
[answer field parser](./wire/answer-fields.ts) admits the supported keywords before
they travel in a message, and [answer validation](validation.ts) checks against
that admitted form. When changing a form keyword, follow it through admission,
encoding, and answer checking so the request and its answers agree.

Hosts can import the value and rendering entrypoints without loading the
operation layer. The daemon composes that layer through [index.ts](index.ts)
and supplies its protocol capabilities through [ports.ts](ports.ts).
Host rendering lives in [host-protocol](../../host-protocol/render.ts), where
local visibility and guidance stay separate from canonical collective results. Keep shared value handling independent of those live
capabilities. The [collective contract](../../../../../docs/reference/spec/communication/collectives.md)
owns operation behavior and completion rules.
