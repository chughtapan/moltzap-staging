/** @file Shared MCP helpers use the workspace's typed lint rules. */
import { packageEslintConfig } from "../../eslint.shared.js";
export default packageEslintConfig({ tsconfigRootDir: import.meta.dirname });
