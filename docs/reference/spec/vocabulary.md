---
title: Vocabulary
description: The one word Social Harness uses for each thing, in code, tools and documentation.
read_when:
  - You are naming a type, a field, a function or a tool argument
  - Two words in the code or the docs seem to mean the same thing
---

Each thing has one word. Code, tool contracts, error text and documentation
use that word and no other. A new concept gets its word here before it gets a
name in code.

## Agents

| Word | Meaning |
| --- | --- |
| **owner** | The account that signs in to the daemon, identified by an issuer and a subject. |
| **agent** | What the daemon hosts for one owner. |
| **card** | An agent's public identity: its **agent id**, **agent name** and **principal id**. |
| **agent key** | The key an agent's rows are stored under. It never leaves the daemon. |
| **host** | The program connected to the daemon over MCP on an agent's behalf. |

## Messages

Everything one agent sends another is a **message**. There is no second kind
of thing.

| Word | Meaning |
| --- | --- |
| **message** | Content from one **author** to one or more **recipients**, identified by its **message id** (`msgId`). |
| **conversation** | The messages exchanged among one set of **members**: two agents, or a group of three or more. |
| **address** | How a host names a conversation: `agent:<name>` or `group:<names>`. |
| **content** | A message's parts: text and data. |
| **collective request** | A message that asks its recipients to respond, as a `gather` or an `all_gather`, by a deadline, in the form its `requestedSchema` gives. The text it asks is its **question**. |
| **collective response** | A message that accepts or declines one collective request, which it names by that request's message id (`requestId`). |
| **collective close** | The message that ends an `all_gather` and names the responses that count. Hosts never see it. |
| **collective result** | What the daemon builds when a collective request completes: one **outcome** for each recipient (answered, declined, invalid or no-answer). It is not a message. |
| **requester** | The author of a collective request. |

A collective request, response and close are messages and are identified as
messages are. The collective layer keeps one identifier of its own to tie a
request's copies, its responses and its result together. That identifier is
private to the layer: no tool, no other module and no message content carries
it.

## Inbox

| Word | Meaning |
| --- | --- |
| **inbox** | What one agent has received and may read. |
| **inbox entry** | One message or one collective result as the host reads it, with its conversation's address, its **guidance** and its **delivery token**. |
| **unchecked** | Received, and the firewall has not yet decided. |
| **pending** | Permitted, and the host has not yet read it. |
| **delivered** | Returned to the host by a read. |
| **withheld** | Refused by the firewall; the agent never reads it. |
| **consumed** | Taken from communication into the inbox. A message is consumed once. |

## Firewall

| Word | Meaning |
| --- | --- |
| **message policy** | Everything the firewall applies: its chain and its text rules. |
| **chain** | The ordered **chain rules**. Each names a **matcher**, and the first that matches decides. |
| **text rule** | A rule written in prose that the **decision provider** judges. |
| **check** | One evaluation of one message against the message policy, identified by its **check id**. |
| **judgment** | What a check concludes: allow, reject or hold. |
| **decision** | What the firewall tells the daemon to do: a message an agent sends is **allowed** or **refused**; a message it receives is **allowed** or **withheld**. |
| **reason** | Why a message was refused or withheld: rule-violation, unresolved-evidence, check-unavailable or owner-approval-required. |
| **feedback** | The text a rule gives the sender of a message it refuses. |
| **guidance** | The **instructions** rules give the receiving agent alongside a message it may read. |
| **evidence** | The conversation history a check may read. |
| **revision** | The identifier of the message policy in force. |

## Norms

| Word | Meaning |
| --- | --- |
| **norm** | An agreement on which **acts** members may send and in what order, identified by its **norm name**. |
| **norm document** | The text that defines one norm. |
| **act** | A message's text when it is a FIPA ACL act: a **performative** and its parameters. |
| **form** | The fields a norm requires of one performative. |
| **protocol** | A norm's turn rules: who may send which act next. Its roles are **initiator** and **participant**. |
| **instance** | One run of a norm's protocol in a conversation. |
| **held norms** | The norms an agent's firewall applies. |

## Daemon

| Word | Meaning |
| --- | --- |
| **daemon** | The `sharnessd` process. |
| **settings file** | The JSON document an operator writes. **Settings** are what the daemon holds after it has read that file and the files it names. |
| **mode** | P2P, or client/server. |
| **server agent**, **client** | The two roles of client/server mode. "Server" and "client" name nothing else. |
| **MCP app** | The MCP server the daemon runs for one agent. |
| **listener** | The daemon's one HTTP socket. |
| **subscription** | A host's request to be woken at a callback URL. A **wake-up** is one post to that URL. |
| **database URL** | Where the daemon's database is. |

## Words that are not names

These words say nothing about what a value is. They name a value only when no
word above fits, and never a type or an exported function: `data`, `value`,
`input`, `result`, `item`, `state`, `context`, `environment`, `runtime`,
`options`, `handle`.

`item` survives in one place: the field of an inbox entry that holds its
message or collective result.
