/** @file Registry returns verified cards through one closed failure channel. */

import type { Effect, Types } from "effect";
import type {
  Registry,
  RegistryError,
  RegistryLookupResult,
  RegistryRegisterResult,
} from "./registry.js";

type Expect<Value extends true> = Value;
/** Both operations require only their captured Registry service. */
export type RegistryChannels = [
  Expect<
    Types.Equals<
      ReturnType<typeof Registry.lookup>,
      Effect.Effect<RegistryLookupResult, RegistryError, Registry>
    >
  >,
  Expect<
    Types.Equals<
      ReturnType<typeof Registry.register>,
      Effect.Effect<RegistryRegisterResult, RegistryError, Registry>
    >
  >,
];
