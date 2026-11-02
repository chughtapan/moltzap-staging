/** @file The Registry process requires no external services and exposes only its closed startup error. */
import type { Layer, Types } from "effect";
import type { layer, StartupError } from "./index.js";
type Expect<Value extends true> = Value;
/** Process composition keeps platform adapters behind the boundary. */
export type ServerLayerIsExact = Expect<
  Types.Equals<typeof layer, Layer.Layer<never, StartupError>>
>;
