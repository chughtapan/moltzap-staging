/** @file An opaque package entry keeps OpenClaw's host types out of the package import. */
import plugin from "./plugin.js";

interface OpaquePluginEntry {
  readonly id: string;
}

/**
 * What importing the package by name yields: the plugin typed by its id only,
 * so OpenClaw's host types stay out of the package's declarations.
 */
const packageEntry: OpaquePluginEntry = plugin;

// eslint-disable-next-line import-x/no-default-export -- consumers that import the package by name expect a default export; OpenClaw itself loads `dist/plugin.js` from the package's `openclaw.extensions`.
export default packageEntry;
