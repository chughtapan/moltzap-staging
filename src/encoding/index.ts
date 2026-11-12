/** @file JSON text, exact structs and identifier checks shared by every layer that reads or writes external data. */

/** Struct schemas that reject unknown properties. */
export { exactStruct } from "./exact-struct.js";
/** JSON text encoding, decoding and comparison, with their failure and the size bound of a saved item. */
export {
  ClientRepresentationError,
  decodeJson,
  encodeJson,
  hasWellFormedUnicode,
  jsonText,
  MAXIMUM_RUNTIME_ITEM_BYTES,
  representationFailure,
  sameJson,
} from "./json.js";
/** Canonical identifier and base64url checks, and the orderings sorted lists use. */
export {
  canonicalIdentifier,
  compareAgentIds,
  compareAscii,
  isCanonicalIdentifier,
  isStrictlyAscending,
} from "./identifier.js";
