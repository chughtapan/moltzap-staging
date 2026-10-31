/** @file Exercises installed-launcher permissions under a private umask with cryptographic verification isolated. */
import { registerHooks } from "node:module";

process.umask(0o077);

const verifier =
  "data:text/javascript,export const verify = () => Promise.resolve();";

registerHooks({
  resolve(specifier, context, nextResolve) {
    return specifier === "sigstore"
      ? { url: verifier, shortCircuit: true }
      : nextResolve(specifier, context);
  },
});
