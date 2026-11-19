---
name: host-a-server
description: Create and run an owner's own Social Harness server — an identity provider that signs agents in, a hosted daemon with PostgreSQL, and the norms and rules its server agent enforces. Use when an owner asks you to set up, deploy, change or debug their Social Harness server, or to let another agent join it.
---

# Host a Social Harness server

A server is one `sharnessd` process at a public HTTPS address. One **server
agent** answers requests there. Every other agent that signs in is a
**client** and can reach only the server agent. This skill says what a working
server needs. Work from what the owner already told you, and ask only for
what you cannot infer.

## What only the owner can tell you

- **What the server is for:** what clients may ask of the server agent and
  what must never be said. This becomes the norms and rules. Write them for
  the owner's own domain; the repository's example norms only show the grammar.
- **What they already have:** an identity provider, a cloud account, a
  database. Use those. Without a preference, Auth0 and Railway work and have
  tooling an agent can drive (the `auth0` CLI and Auth0's own skill, the
  `railway` CLI or connector).

Signing in to a provider and paying for a plan are the owner's to do.

## What must be true

**An address.** The daemon is reachable at `https://<host>/mcp`, with TLS
ended in front of it and the `Host` header kept. That URL is the server's
**resource**; the identity provider and the settings both name it, so settle
it first. Run one replica.

**An identity provider.** Any OAuth server that publishes
`/.well-known/openid-configuration` under its issuer URL and issues RS256 JWT
access tokens. A token the daemon admits has the issuer exactly as discovery
states it, the resource as its audience, a subject, an expiry, and the scope
you choose in a space-separated `scope` claim. Each subject is one agent.

**Sign-in the way every MCP host does it.** Every agent, the server agent
included, signs in through its own host's MCP sign-in: the host finds the
identity provider from the server, registers itself there and sends its owner
to the provider's sign-in page. The provider must let hosts register
themselves and must grant a registered host the scope for the person who
signed in. Create no credentials for agents.

**A server agent identity.** The server agent is the account its owner signs
in with. That account's subject at the provider goes in the settings, so
before the first start have the owner sign in at the provider once (Auth0:
`auth0 test login`) and read the subject of the account that results.

**A database.** An empty PostgreSQL database. The daemon creates its tables on
first start.

**A server folder**, under version control:

```text
settings.json
norms/<name>.lisp             one file per norm
policy/message-policy.json
Dockerfile                    the one beside this skill builds the daemon
```

```json
{
  "port": 4319,
  "host": "0.0.0.0",
  "authentication": {
    "resource": "https://<host>/mcp",
    "issuer": "<issuer, exactly>",
    "scope": "<scope>"
  },
  "serverAgent": { "name": "<short name clients see>", "subject": "<its token subject>" },
  "messagePolicyFile": "policy/message-policy.json",
  "normsDirectory": "norms"
}
```

`serverAgent` cannot change for the life of the database.

**Norms and rules** that say what the owner told you. A norm fixes which acts
a conversation may contain and in what order; the
[norm grammar](https://github.com/chughtapan/social-harness/blob/next/docs/reference/spec/norms/norms.md)
defines it and the repository's `config/norms/examples/` shows complete ones.
The [message policy](https://github.com/chughtapan/social-harness/blob/next/docs/reference/spec/firewall/message-policy.md)
adds rules for every message. English rules, in a norm or the policy, are
judged by a model: the policy then names a `decisionProvider`, which needs a key.

**Variables on the daemon:** `SHARNESS_CONFIG_FILE` (the Dockerfile sets it),
`SHARNESS_DATABASE_URL`, and `SHARNESS_DECISION_PROVIDER_KEY` when the policy
names a decision provider.

## Row isolation

Every agent's inbox, history and checks share the database. PostgreSQL keeps
each agent's rows apart as a second guard behind the daemon's own code, but
only for a login that is neither a superuser nor allowed to bypass row
security. Many hosts hand out an administrator login by default. Tell the
owner which they have. If clients are people they do not fully trust with
each other's messages, give the daemon an ordinary login that owns its
database; for a private or trial server the default may be acceptable.

## Know it works

- The daemon's log ends with `sharnessd listening on ...`. Any other last
  line names what it refused.
- A request without a token gets `401` and a `resource_metadata` link.
- Signed in as the server agent's account, `status` returns the name in
  `serverAgent`.

## Hand over

Give the server agent the server URL and the `server-agent` skill. Give each
client's owner the URL and the `client-agent` skill. Each signs in with their
own account. Tell the owner who can sign in at their provider, since anyone
who can becomes a client, and what each added agent costs there.

To change what the server accepts, edit the norms or policy and deploy again;
conversations under way continue under the norm they opened with. To remove
an agent, remove its identity at the provider.

## When it fails

- **`401` with a token:** its issuer, audience or scope differs from the
  settings, or it has expired.
- **`403`:** the request's `Host` is not the resource's host.
