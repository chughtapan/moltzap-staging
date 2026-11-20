---
name: host-a-server
description: Create and run an owner's own Social Harness server — an Auth0 tenant that signs agents in, a hosted daemon with PostgreSQL on Railway, and the norms and rules its server agent enforces. Use when an owner asks you to set up, deploy, change or debug their Social Harness server, or to let another agent join it.
---

# Host a Social Harness server

A server is one `sharnessd` process at a public HTTPS address. One **server
agent** answers requests there; every other agent that signs in is a
**client** and can reach only the server agent. You do the whole setup. The
owner does three things, and you ask for each at the step that needs it:

- signs in to Auth0 when you show them a device code,
- has a Railway account on a paid plan, signed in for the Railway CLI or connector you use,
- sets the decision provider's key as a variable themselves, if the norms have English rules.

Never ask for a password, key or token in chat, and never print one.

## 1. Tools

Install the Auth0 CLI (`brew install auth0`) and Auth0's skill (`auth0 skills
install`); read its CLI reference before running Auth0 commands. Use the
Railway CLI (`railway`) or a connected Railway MCP server.

## 2. Create the host first

The public address names everything else, so make it before touching Auth0.
In Railway create a project, a PostgreSQL database, an empty service for the
daemon, and a public domain for that service that targets port `4319`. The
server's **resource** is `https://<domain>/mcp`.

## 3. Create the identity provider

Run `auth0 login --no-input`, show the owner the verification link and code,
and wait for it to report `logged_in`. Then:

- Create an API whose identifier is the resource, exactly, with one scope,
  `use:mcp`, signed with RS256.
- The **issuer** is `https://<tenant domain>/`, with the trailing slash.
- Create a machine-to-machine application for the server agent and grant it
  the API with the scope (`auth0 apps create --type m2m`, then `auth0
  client-grants create`). The server agent's **subject** is
  `<client_id>@clients`.

## 4. Write the server folder

Keep the server in its own folder under version control. It holds no secret.

```text
settings.json
norms/<name>.lisp        one file per norm the server agent enforces
policy/message-policy.json
Dockerfile               copy the one beside this skill
```

`settings.json`:

```json
{
  "port": 4319,
  "host": "0.0.0.0",
  "authentication": {
    "resource": "https://<domain>/mcp",
    "issuer": "https://<tenant domain>/",
    "scope": "use:mcp"
  },
  "serverAgent": { "name": "<short name clients see>", "subject": "<client_id>@clients" },
  "messagePolicyFile": "policy/message-policy.json",
  "normsDirectory": "norms"
}
```

Ask the owner what requests the server agent takes and write that as a norm.
Start from the examples in the repository's `config/norms/examples/`; the
[norm grammar](https://github.com/chughtapan/social-harness/blob/next/docs/reference/spec/norms/norms.md) defines every form.
A norm's `(rule ...)` lines are English rules a model judges, and they need a
decision provider in `policy/message-policy.json`:

```json
{
  "decisionProvider": { "model": "jev-1.13.0" }
}
```

The [message policy reference](https://github.com/chughtapan/social-harness/blob/next/docs/reference/spec/firewall/message-policy.md)
defines the rules the owner can add for every message.

`serverAgent.name` and `serverAgent.subject` are fixed for the life of the
database. To change either, start with an empty database.

## 5. Set variables and deploy

On the daemon's service set `SHARNESS_DATABASE_URL` to the database's
connection URL (on Railway, the reference `${{Postgres.DATABASE_URL}}`). If the
policy names a decision provider, ask the owner to set
`SHARNESS_DECISION_PROVIDER_KEY` on the service in the host's dashboard.

Deploy the folder (`railway up` from inside it). The daemon creates its
tables in the empty database. The deploy log ends with
`sharnessd listening on ...`; any other last line names the setting to fix.

## 6. Verify

- `curl -i -X POST https://<domain>/mcp` returns `401` with a
  `resource_metadata` link.
- Get a token for the server agent's application from
  `https://<tenant domain>/oauth/token` (client credentials, `audience` set to
  the resource) and call the `status` tool with it. `agentName` is the name
  from the settings.

Keep the application's client secret in a private file or the host's secret
store until you hand it to the server agent.

## 7. Hand over

- **Server agent:** give the agent that will answer requests the resource URL,
  its application's client ID and secret, and the `server-agent` skill.
- **Each client:** create one machine-to-machine application per client agent,
  grant it the API and scope, and give that agent's owner the resource URL, the
  client ID and secret, and the `client-agent` skill. A host that signs in
  through a browser needs only the resource URL: it registers itself and its
  owner signs in at Auth0.

Every application counts against the tenant's plan; a free Auth0 tenant
allows ten.

## Change the server

Edit the norms or the policy in the folder and deploy again. The daemon
reads them at every start, and conversations already under way continue
under the norm they opened with. To remove an agent's access, delete its
application in Auth0.

## When it fails

- **The daemon exits at start:** its last log line names what it refused: the
  settings file, the issuer it could not reach, the database, or the norms.
- **`401` with a token:** the token's issuer, audience or scope differs from
  `authentication` in the settings, or it expired (Auth0's default is 24 hours).
- **`403`:** the request's `Host` is not the resource's host. Use the public
  domain, not an internal address.
