/** @file Canonical JSON bytes and canonical identifiers shared by every layer that reads or writes external data. */

/** Canonical JSON encoding, exact decoding and byte comparison, with their failures and size bound. */
export {
  canonicalBytes,
  ClientRepresentationError,
  decodeCanonical,
  decodeCanonicalJson,
  encodeCanonical,
  encodeCanonicalJson,
  equalCanonical,
  exactParseOptions,
  exactStruct,
  MAXIMUM_RUNTIME_ITEM_BYTES,
  parseCanonicalJson,
  representationFailure,
  sameBytes,
} from "./canonical-json.js";
/** Canonical identifier and base64url checks, and the orderings sorted lists use. */
export {
  canonicalIdentifier,
  compareAgentIds,
  compareAscii,
  decodeCanonicalBase64Url,
  hasCanonicalBase64UrlLength,
  isCanonicalIdentifier,
  isStrictlyAscending,
} from "./identifier.js";
