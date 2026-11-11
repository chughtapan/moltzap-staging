/** @file Canonical JSON bytes and canonical identifiers shared by every layer that reads or writes external data. */

/** Canonical JSON encoding, exact decoding and byte comparison, with their failures and size bound. */
export {
  canonicalBytes,
  ClientRepresentationError,
  decodeCanonical,
  encodeCanonical,
  encodeCanonicalJson,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
  representationFailure,
  sameBytes,
} from "./canonical-json.js";
/** Canonical identifier and base64url checks, and the orderings sorted lists use. */
export {
  canonicalIdentifier,
  compareAgentIds,
  compareAscii,
  isCanonicalIdentifier,
  isStrictlyAscending,
} from "./identifier.js";
