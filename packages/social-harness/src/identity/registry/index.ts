/** @file Registry client service and its typed identity operations. */
/** Retries preserve registration identity and request fields. */
export {
  RegistrationId,
  type RegistryLookupRequest,
  type RegistryLookupResult,
  type RegistryRegisterCall,
  RegistryRegisterRequest,
  type RegistryRegisterResult,
} from "./contract.js";
/** Client calls pin Registry authority before returning verified identities. */
export { Registry, RegistryError } from "./service.js";
