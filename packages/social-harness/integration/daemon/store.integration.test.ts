/** @file A fresh state directory becomes reopenable after initialization. */
import { live as it } from "@effect/vitest";
import { Effect } from "effect";
import { expect } from "vitest";
import {
  stateDirectory,
  withDaemonStore,
} from "../../src/__tests__/store-schema-fixtures.js";
import { inspectEndpointStore } from "../../src/daemon/store.js";

it("distinguishes an empty directory from initialized state", () =>
  Effect.gen(function* () {
    const directory = stateDirectory();
    const before = yield* inspectEndpointStore(directory);
    yield* withDaemonStore(directory, () => Effect.void);
    expect([before, yield* inspectEndpointStore(directory)]).toEqual([
      "create",
      "reopen",
    ]);
  }));
