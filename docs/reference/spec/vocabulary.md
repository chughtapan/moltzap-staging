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
| **conversation** | The messages exchanged between its two **members**. |
| **address** | How a host names a conversation: `agent:<name>`. |
| **content** | A message's parts: text and data. |

## Inbox

| Word | Meaning |
| --- | --- |
| **inbox** | What one agent has received and may read. |
| **inbox entry** | One message as the host reads it, with its conversation's address and its **guidance**. |
| **unchecked** | Received, and the firewall has not yet decided. |
| **cursor** | A place in an inbox, in the order its entries were allowed. A read returns the entries after it. |
| **withheld** | Refused by the firewall; the agent never reads it. |
| **consumed** | Taken from communication into the inbox. A message is consumed once. |

## Firewall

| Word | Meaning |
| --- | --- |
| **message policy** | Everything the firewall applies: its text rules. |
| **text rule** | A rule written in prose that the **decision provider** judges. |
| **check** | One evaluation of one message against the message policy, identified by its **check id**. |
| **judgment** | What a check concludes: allow, reject or hold. |
| **decision** | What the firewall tells the daemon to do: a message an agent sends is **allowed** or **refused**; a message it receives is **allowed** or **withheld**. |
| **reason** | Why a message was refused or withheld: rule-violation, unresolved-evidence or check-unavailable. |
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
| **server agent**, **client** | The two roles an agent has. "Server" and "client" name nothing else. |
| **MCP handler** | The one MCP server the daemon runs. It answers each request from the agent of the owner the request is admitted as. |
| **listener** | The daemon's one HTTP socket. |
| **database URL** | Where the daemon's database is. |

## Words that are not names

These words say nothing about what a value is. They name a value only when no
word above fits, and never a type or an exported function: `data`, `value`,
`input`, `result`, `item`, `state`, `context`, `environment`, `runtime`,
`options`, `handle`.
