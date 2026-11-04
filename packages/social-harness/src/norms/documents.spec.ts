/** @file Shipped and invalid norm documents. */

import { Either } from "effect";
import { readdirSync, readFileSync } from "node:fs";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { NormDocumentError, readNorm } from "./documents.js";

const directory = fileURLToPath(
  new URL("../../config/norms/", import.meta.url),
);
const shipped = [
  ...readdirSync(directory).filter((file) => file.endsWith(".lisp")),
  ...readdirSync(join(directory, "examples")).map((file) =>
    join("examples", file),
  ),
];

describe("readNorm", () => {
  it.each(shipped)("reads the shipped document %s", (file) => {
    const norm = Either.getOrThrow(
      readNorm(readFileSync(join(directory, file), "utf8")),
    );
    expect(`${norm.name}.lisp`).toBe(basename(file));
  });

  it("reads a norm's protocol, roles, forms and rules", () => {
    const norm = Either.getOrThrow(
      readNorm(`
        ; A comment.
        (norm swap :protocol fipa-request :initiator Asker :participant Owner
              :description "Swap a shift.")
        (content request
          (field shift :type select :choices (morning evening) :description "The shift.")
          (field note :type string :max-length 80 :optional true))
        (rule "Ask a day ahead.")
      `),
    );
    expect(norm).toMatchObject({
      name: "swap",
      protocol: "fipa-request",
      roles: { initiator: "Asker", participant: "Owner" },
      description: "Swap a shift.",
      contents: {
        request: {
          type: "object",
          properties: {
            shift: {
              type: "string",
              enum: ["morning", "evening"],
              description: "The shift.",
            },
            note: { type: "string", maxLength: 80 },
          },
          required: ["shift"],
        },
      },
      rules: ["Ask a day ahead."],
    });
  });

  it.each([
    { what: "no norm form", source: "(content inform)" },
    {
      what: "two norm forms",
      source: '(norm a :description "A.") (norm b :description "B.")',
    },
    {
      what: "an unknown protocol",
      source:
        '(norm a :protocol fipa-auction :initiator A :participant B :description "A.")',
    },
    {
      what: "a protocol without roles",
      source: '(norm a :protocol fipa-request :description "A.")',
    },
    {
      what: "an unknown field type",
      source:
        '(norm a :description "A.") (content inform (field x :type date))',
    },
    {
      what: "a select without choices",
      source:
        '(norm a :description "A.") (content inform (field x :type select))',
    },
    {
      what: "a repeated content form",
      source: '(norm a :description "A.") (content inform) (content inform)',
    },
    {
      what: "an unknown form",
      source: '(norm a :description "A.") (permit inform)',
    },
  ])("refuses a document with $what", ({ source }) => {
    expect(Either.getOrThrow(Either.flip(readNorm(source)))).toBeInstanceOf(
      NormDocumentError,
    );
  });
});

describe("readNorm with a norm's own turns", () => {
  it("reads who sends each act, the facts it needs and the facts it changes", () => {
    const norm = Either.getOrThrow(
      readNorm(`
        (norm swap :initiator Asker :participant Owner :description "Swap a shift.")
        (content request :by Asker :unless open :initiates (open asked))
        (content propose :by Owner :when asked :max 2
                 :initiates offered :terminates asked
          (field shift :type string))
        (content accept-proposal :by Asker :when offered :terminates (open offered))
        (content cancel)
      `),
    );
    expect(norm).toMatchObject({
      roles: { initiator: "Asker", participant: "Owner" },
      turns: {
        request: {
          by: "initiator",
          when: [],
          unless: ["open"],
          initiates: ["open", "asked"],
          terminates: [],
        },
        propose: {
          by: "participant",
          when: ["asked"],
          unless: [],
          initiates: ["offered"],
          terminates: ["asked"],
          max: 2,
        },
        "accept-proposal": {
          by: "initiator",
          when: ["offered"],
          unless: [],
          initiates: [],
          terminates: ["open", "offered"],
        },
      },
      contents: { propose: { properties: { shift: { type: "string" } } } },
    });
  });

  it.each([
    {
      what: "turns and a protocol",
      source:
        '(norm a :protocol fipa-request :initiator A :participant B :description "A.") (content request :by A :initiates x)',
    },
    {
      what: "roles but neither a protocol nor turns",
      source:
        '(norm a :initiator A :participant B :description "A.") (content request)',
    },
    {
      what: "a turn by an unknown role",
      source:
        '(norm a :initiator A :participant B :description "A.") (content request :by C :initiates x)',
    },
    {
      what: "an unknown turn keyword",
      source:
        '(norm a :initiator A :participant B :description "A.") (content request :by A :after agree :initiates x)',
    },
    {
      what: "a fact that is not a name",
      source:
        '(norm a :initiator A :participant B :description "A.") (content request :by A :initiates ("Open"))',
    },
    {
      what: "a fact no act initiates",
      source:
        '(norm a :initiator A :participant B :description "A.") (content request :by A :initiates open) (content agree :by B :when opened :terminates open)',
    },
    {
      what: "no act the initiator can open an instance with",
      source:
        '(norm a :initiator A :participant B :description "A.") (content request :by B :initiates open) (content agree :by A :when open :terminates open)',
    },
    {
      what: "an act without a turn among turns",
      source:
        '(norm a :initiator A :participant B :description "A.") (content request :by A :initiates open) (content agree)',
    },
    {
      what: "turn keywords on cancel",
      source:
        '(norm a :initiator A :participant B :description "A.") (content request :by A :initiates open) (content cancel :by A)',
    },
  ])("refuses a document with $what", ({ source }) => {
    expect(Either.getOrThrow(Either.flip(readNorm(source)))).toBeInstanceOf(
      NormDocumentError,
    );
  });
});
