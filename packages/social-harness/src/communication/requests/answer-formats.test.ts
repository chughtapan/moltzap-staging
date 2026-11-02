/** @file Pins the string formats a form-mode field may require of an answer. */

import { describe, expect, it } from "vitest";
import { type AnswerFormat, matchesFormat } from "./answers.js";

const judged = (format: AnswerFormat, texts: readonly string[]) =>
  texts.map((text) => matchesFormat(format, text));

// @agent-code-guard/regression-only: examples pin each format's accepted and refused strings.
describe("date answer formats", () => {
  it("accepts calendar dates and refuses impossible ones", () => {
    expect(
      judged("date", ["2024-02-29", "2023-02-29", "2024-13-01", "2024-2-01"]),
    ).toEqual([true, false, false, false]);
  });

  it("applies the century rule to February 29", () => {
    expect(judged("date", ["2000-02-29", "1900-02-29"])).toEqual([true, false]);
  });

  it("requires a time zone on a date-time", () => {
    expect(
      judged("date-time", [
        "2024-02-01T10:00:00Z",
        "2024-02-01t10:00:00.5+01:30",
        "2024-02-01 10:00:00-0130",
        "2024-02-01T10:00:00",
      ]),
    ).toEqual([true, true, true, false]);
  });

  it("admits a leap second only at 23:59 UTC", () => {
    expect(
      judged("date-time", [
        "2024-02-01T23:59:60Z",
        "2024-02-01T22:59:60-01:00",
        "2024-02-01T10:59:60Z",
      ]),
    ).toEqual([true, true, false]);
  });

  it("admits a leap second whose UTC reading borrows across midnight", () => {
    expect(
      judged("date-time", [
        "2024-01-01T00:59:60+01:00",
        "2024-01-01T00:00:60+00:01",
        "2024-01-01T00:59:60+02:00",
      ]),
    ).toEqual([true, true, false]);
  });
});

// @agent-code-guard/regression-only: examples pin each format's accepted and refused strings.
describe("address answer formats", () => {
  it("requires a dotted host name in an email", () => {
    expect(
      judged("email", ["a.b@example.com", "a@example", ".a@example.com"]),
    ).toEqual([true, false, false]);
  });

  it("requires a scheme and RFC 3986 characters in a uri", () => {
    expect(
      judged("uri", [
        "https://user@[::1]:8080/a?b#c",
        "urn:example:x",
        "example.com",
        "https://a b",
        "https://%zz",
      ]),
    ).toEqual([true, true, false, false, false]);
  });
});
