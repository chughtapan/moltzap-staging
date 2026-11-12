/**
 * @file Ships the same Social Harness skills with every host channel.
 * Run from the channel package directory after `tsc -b` so its published
 * `dist/skills` contains the library's instructions unchanged.
 */
import { cpSync } from "node:fs";
import { resolve } from "node:path";

cpSync(new URL("../../skills", import.meta.url), resolve("dist/skills"), {
  recursive: true,
});
