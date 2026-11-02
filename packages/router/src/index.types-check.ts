/** @file The Router process layer exposes only its self-contained startup boundary. */
import type { Layer, Types } from "effect";
import type * as RouterServerPackage from "./index.js";
type Expect<Value extends true> = Value;
type RouterServerLayer = typeof RouterServerPackage.RouterServer.layer;
type ServerLayerProvidesNothing = Expect<
  Types.Equals<Layer.Layer.Success<RouterServerLayer>, never>
>;
type ServerLayerFailsOnlyAtStartup = Expect<
  Types.Equals<
    Layer.Layer.Error<RouterServerLayer>,
    RouterServerPackage.RouterServer.StartupError
  >
>;
type ServerLayerIsSelfContained = Expect<
  Types.Equals<Layer.Layer.Context<RouterServerLayer>, never>
>;
type ServerExportsAreExact = Expect<
  Types.Equals<keyof typeof RouterServerPackage, "RouterServer">
>;
type ServerNamespaceIsExact = Expect<
  Types.Equals<
    keyof typeof RouterServerPackage.RouterServer,
    "StartupError" | "help" | "layer"
  >
>;
/** Compile-time evidence that process construction stays self-contained. */
export type ServerContracts = [
  ServerLayerProvidesNothing,
  ServerLayerFailsOnlyAtStartup,
  ServerLayerIsSelfContained,
  ServerExportsAreExact,
  ServerNamespaceIsExact,
];
