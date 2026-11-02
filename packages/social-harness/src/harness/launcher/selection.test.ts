/** @file The rules that decide whether a start takes a published release. */

import { FastCheck as fc } from "effect";
import { describe, expect, it } from "vitest";
import { choose, compareVersions } from "./selection.js";

const recorded = {
  version: "2026.1021.1",
  liveWireVersions: ["2026.1019.2", "2026.1022.1"],
};
const follow = { kind: "follow", distTag: "latest" } as const;

/** A calendar version whose parts compare correctly as padded text. */
const sortKey = (version: string): string =>
  version
    .split(".")
    .map((part) => part.padStart(5, "0"))
    .join(".");

const calendarVersion = fc
  .tuple(
    fc.integer({ min: 2025, max: 2028 }),
    fc.integer({ min: 101, max: 1231 }),
    fc.integer({ min: 0, max: 20 }),
  )
  .map(([year, day, counter]) => `${year}.${day}.${counter}`);

describe("following a dist-tag", () => {
  it("takes only a newer release whose wire version the recorded daemon admits", () => {
    fc.assert(
      fc.property(
        calendarVersion,
        fc.constantFrom(...recorded.liveWireVersions, "2026.1101.1"),
        (version, wireVersion) => {
          const choice = choose(recorded, { version, wireVersion }, follow);
          const newer = (compareVersions(version, recorded.version) ?? 0) > 0;
          const admitted = recorded.liveWireVersions.includes(wireVersion);
          expect(choice.kind === "take").toBe(newer && admitted);
        },
      ),
    );
  });

  it("never moves an agent to an older release", () => {
    expect(
      choose(
        recorded,
        { version: "2026.1020.9", wireVersion: "2026.1019.2" },
        follow,
      ),
    ).toEqual({ kind: "keep", reason: "older" });
  });

  it("keeps the recorded release over a newer one outside its wire window, as every reset release is", () => {
    expect(
      choose(
        recorded,
        { version: "2026.1101.1", wireVersion: "2026.1101.1" },
        follow,
      ),
    ).toEqual({ kind: "keep", reason: "outside-wire-window" });
  });
});

describe("an exact pin", () => {
  it("takes any other release, in either direction and across a wire change", () => {
    fc.assert(
      fc.property(calendarVersion, fc.string(), (version, wireVersion) => {
        const choice = choose(
          recorded,
          { version, wireVersion },
          { kind: "pinned", version },
        );
        expect(choice.kind === "take").toBe(version !== recorded.version);
      }),
    );
  });
});

describe("calendar versions", () => {
  it("order like their year, month and day, and counter, read as numbers", () => {
    fc.assert(
      fc.property(calendarVersion, calendarVersion, (left, right) => {
        const order = compareVersions(left, right) ?? 0;
        expect(Math.sign(order)).toBe(
          Math.sign(sortKey(left).localeCompare(sortKey(right))),
        );
      }),
    );
  });

  it("leave a version that is not a calendar version unresolved", () => {
    expect(
      choose(recorded, { version: "next", wireVersion: "2026.1019.2" }, follow),
    ).toEqual({ kind: "keep", reason: "unresolved" });
  });
});
