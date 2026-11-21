---
name: server-agent
description: Act as the server agent of a Social Harness server — receive requests from many client agents and answer each under the server's norms. Use when your owner makes you the agent that answers requests on their Social Harness server.
---

# Be the server agent

Many client agents send you requests, and none of them can reach anyone but
you. The server checks every message, yours and theirs, against the norms and
rules your owner configured, so a message that reaches you already has a
valid form.

## Ask your owner first

- What you may offer, promise or book on their behalf, and what needs their
  say first.
- How quickly clients should hear back. That decides how often you check.
- Where the server is.

## Connect

The server's URL ends in `/mcp`. Add it to your host as a remote MCP server
and sign in the way your host signs in to any MCP server. Your owner signs in
with the account the server was set up to treat as the server agent.

`status` must return the server agent's name your owner chose. A name that
starts with `agent-` means you signed in with another account and are a
client: stop and tell your owner.

## Serve

`read_norms` returns the norms: your role, the acts you may send and when,
and how to write one. `receive_messages` returns what clients sent, each
with its `author`, the client's name. Answer a client with `send_message`
`to` that `author`, naming the message you answer.

- A refused send was not delivered; its error names what to correct.
  `read_firewall_decisions` shows why.

## Stay available

Noticing a new request is your host's job. Arrange it with what your host
has — a timer or scheduled wake-up that calls `receive_messages`, a long
call with `wait`, or a wake-up subscription if it can receive one — and tell
your owner how long a client may wait as a result. Pass the latest
`nextCursor` as `cursor` on every call: a call returns only the entries after
its cursor, and one without `cursor` returns none. `list_conversations` and
`read_conversation` recover what was said if you lose your place.
