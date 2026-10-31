/**
 * @file Production composition for one explicitly configured endpoint daemon.
 * @module service
 */

import { NodeHttpClient } from "@effect/platform-node";
import { Data, Duration, Effect, Layer, Option } from "effect";
import type {
  EndpointStoreError,
  OutboxTooOldError,
} from "../communication/store/index.js";
import { Router } from "../communication/router/api/client.js";
import {
  type FirewallPolicyError,
  type LoadedMessagePolicy,
  loadMessagePolicy,
  type MessagePolicyConfigurationError,
  policyConfigurationError,
  policyInstallationError,
} from "../firewall/config.js";
import {
  Firewall,
  type FirewallOptions as ManagedFirewallOptions,
} from "../firewall/index.js";
import { registrationAdmissionRefusalMessage } from "../host-protocol/index.js";
import {
  Identity,
  type IdentityError,
  localIdentityLayer,
} from "../identity/local.js";
import { Registry } from "../identity/registry/index.js";
import { wireVersions } from "../identity/wire-version.js";
import {
  type DaemonBootstrap,
  DaemonConfigurationError,
  type DaemonProcessConfiguration,
} from "./bootstrap.js";
import {
  loadDaemonBootstrap,
  loadDaemonProcessConfiguration,
  processHelp,
} from "./configuration.js";
import { readCredential } from "./credentials.js";
import { type DaemonRuntimeError, runDaemonRuntime } from "./lifecycle.js";
import { inspectEndpointStore, layer as storageLayer } from "./store.js";
import { spanExport } from "./telemetry.js";

// safer-arch-ignore no-large-public-surface: Daemon process composition, firewall installation and the actual Registry and Router client tags with their operation values share the trusted service embedding boundary.
// safer-arch-ignore require-curated-public-facade: The supported service subpath exposes daemon composition and its Identity, Firewall, Registry and Router service contracts.

/** Local registration, saved identity and remote agent resolution. */
export {
  Identity,
  IdentityError,
  RegistrationError,
} from "../identity/local.js";

const REGISTRY_REQUEST_TIMEOUT = Duration.seconds(30);
const ROUTER_SEND_TIMEOUT = Duration.seconds(30);
const ROUTER_POLL_TIMEOUT = Duration.seconds(35);

const liveWireVersions = wireVersions.liveSet;

/** Managed firewall configuration and checking contracts for daemon embeddings. */
export {
  CheckInputError,
  type DecisionProvider,
  DecisionProviderError,
  type DecisionRequest,
  type DecisionResponse,
  decodeMessagePolicy,
  Firewall,
  type FirewallOperations,
  type FirewallOptions,
  type FirewallSettings,
  loadMessagePolicy,
  type Message,
  type MessageContext,
  MessagePolicy,
  type TextPolicySettings,
  type TextRule,
} from "../firewall/index.js";

// eslint-disable-next-line @typescript-eslint/no-namespace -- The published service subpath intentionally merges its runtime values with the closed startup-error type.
export namespace Daemon {
  /** Usage text listing the daemon process's environment variables. */
  export const help = processHelp;

  /**
   * Trusted composition supplies the provider and fixed rules while owner tools edit local rules.
   * See {@link withFirewall} for the composition fields.
   */
  export type FirewallOptions = ManagedFirewallOptions;

  /** Closed startup phase with sanitized configuration details. */
  export class StartupError extends Data.TaggedError("DaemonStartupError")<{
    readonly phase: "configuration" | "storage" | "listener" | "registration";
    readonly reason?:
      | "outbox-too-old"
      | "incompatible"
      | "agent-not-admitted"
      | "admission-refused"
      | "name_taken"
      | "key_already_registered"
      | "idempotency_conflict"
      | "dependency-unavailable"
      | "persistence-failed"
      | "incompatible-daemon";
    readonly detail?: string;
  }> {
    /**
     * Only closed refusal reasons and sanitized configuration details enter the process log.
     * @returns The startup phase and any safe operator diagnostic.
     */
    override get message(): string {
      const reason = this.reason === undefined ? "" : ` (${this.reason})`;
      const detail = this.detail === undefined ? "" : `: ${this.detail}`;
      let remedy = "";
      if (this.reason === "agent-not-admitted") {
        remedy =
          ". The network no longer admits this agent because the network was reset; reset the agent and register again. Docs: https://docs.moltzap.xyz/guides/run-sharnessd#reset-an-agent-after-the-network-restarts";
      } else if (this.reason === "admission-refused") {
        remedy = `. ${registrationAdmissionRefusalMessage}`;
      } else if (this.reason === "incompatible") {
        remedy =
          ". Use a fresh state directory and register again. Docs: https://docs.moltzap.xyz/reference/spec/daemon/daemon#persistence";
      } else if (this.reason === "outbox-too-old") {
        remedy =
          ". Drain the outbox on the previous release before upgrading, or apply a reset release. Docs: https://docs.moltzap.xyz/guides/upgrades#apply-a-reset-release";
      }
      return `sharnessd startup failed in phase ${this.phase}${reason}${detail}${remedy}`;
    }
  }

  const runDaemon = (composed?: FirewallOptions) =>
    Effect.gen(function* () {
      const configuration = yield* loadDaemonProcessConfiguration.pipe(
        Effect.mapError(configurationFailure),
      );
      const configured = yield* configuredFirewall(configuration, composed);
      const bootstrap = yield* loadDaemonBootstrap(configuration).pipe(
        Effect.mapError(configurationFailure),
      );
      const opening = yield* inspectEndpointStore(
        configuration.stateDirectory,
      ).pipe(Effect.mapError(storageFailure));
      if (opening === "create") {
        yield* bootstrap.admissionCredential.pipe(
          Effect.mapError(configurationFailure),
        );
      }
      const persistence = storageLayer({
        stateDirectory: configuration.stateDirectory,
        liveWireVersions: yield* liveWireVersions,
      }).pipe(Layer.mapError(storageFailure));
      const network = networkLayer(configuration);
      const identity = localIdentityLayer({
        registrySignerPublicKey: configuration.registrySignerPublicKey,
        agentPublicKey: bootstrap.agentPublicKey,
        signingAuthority: bootstrap.signingAuthority,
        admissionCredential: bootstrap.admissionCredential,
      }).pipe(Layer.mapError(storageFailure));
      const policy = Firewall.layer(configured.options).pipe(
        Layer.mapError((cause) => installFailure(cause, configured.loaded)),
      );
      const services = Layer.mergeAll(identity, policy).pipe(
        Layer.provideMerge(network),
        Layer.provideMerge(persistence),
      );
      return yield* serve(bootstrap).pipe(Effect.provide(services));
    }).pipe(Effect.withSpan("Daemon.layer"));

  function serve(bootstrap: DaemonBootstrap) {
    return Effect.gen(function* () {
      const identity = yield* Identity;
      if (
        (yield* identity.me().pipe(Effect.mapError(storageFailure))).kind ===
        "unregistered"
      ) {
        yield* bootstrap.admissionCredential.pipe(
          Effect.mapError(configurationFailure),
        );
      }
      return yield* runDaemonRuntime({ bootstrap }).pipe(
        Effect.mapError(runtimeStartupFailure),
      );
    });
  }

  /**
   * The opt-in span export each composition is built inside, so it closes
   * after the daemon and flushes every span the daemon ended.
   */
  const telemetry = Layer.mapError(spanExport, configurationFailure);

  /**
   * Complete production process composition for `sharnessd`, with the
   * firewall `SHARNESS_MESSAGE_POLICY_FILE` describes when it is set.
   */
  export const layer: Layer.Layer<never, StartupError> = Layer.scopedDiscard(
    runDaemon(),
  ).pipe(Layer.provide(telemetry));

  /**
   * Fixed and editable text rules are installed before the daemon accepts host calls.
   * The environment must not also name a message-policy document.
   * @param options Provider settings, owner facts, fixed rules and initial editable rules.
   * @returns The complete daemon layer with both message boundaries checked.
   */
  export const withFirewall = (
    options: FirewallOptions,
  ): Layer.Layer<never, StartupError> => {
    return Layer.scopedDiscard(runDaemon(options)).pipe(
      Layer.provide(telemetry),
    );
  };

  /**
   * Public startup failures retain the phase and actionable compatibility refusal.
   * @param error Private runtime failure to project onto the process boundary.
   * @returns The closed startup error without private runtime detail.
   */
  function runtimeStartupFailure(error: DaemonRuntimeError): StartupError {
    return new StartupError({
      phase: error.phase,
      ...(error.reason === undefined ? {} : { reason: error.reason }),
    });
  }

  /** A trusted embedding cannot also select an environment policy document. */
  function configuredFirewall(
    configuration: DaemonProcessConfiguration,
    composed?: FirewallOptions,
  ) {
    return loadMessagePolicy(configuration, readCredential).pipe(
      Effect.flatMap((configured) => {
        if (composed !== undefined && Option.isSome(configured)) {
          return Effect.fail(
            policyConfigurationError("message-policy-composed"),
          );
        }
        const loaded = Option.getOrUndefined(configured);
        return Effect.succeed({ loaded, options: composed ?? loaded });
      }),
      Effect.mapError(daemonPolicyConfigurationError),
      Effect.mapError(configurationFailure),
    );
  }

  /**
   * A file-described firewall that fails to install names its setting.
   * @param cause The closed installation failure.
   * @param loaded The configured document and its resolved behavior sources.
   * @returns The configuration-phase startup failure.
   */
  function installFailure(
    cause: FirewallPolicyError,
    loaded?: LoadedMessagePolicy,
  ): StartupError {
    if (loaded !== undefined) {
      return configurationFailure(
        daemonPolicyConfigurationError(
          policyInstallationError(cause, loaded.behaviorRulePaths),
        ),
      );
    }
    return configurationFailure(cause);
  }

  /**
   * Policy startup failures retain their exact reason and message at the daemon boundary.
   * @param cause The policy loader's closed configuration failure.
   * @returns The daemon configuration error with the same reason and message.
   */
  function daemonPolicyConfigurationError(
    cause: MessagePolicyConfigurationError,
  ): DaemonConfigurationError {
    return new DaemonConfigurationError({
      reason: cause.reason,
      message: cause.message,
    });
  }

  function configurationFailure(cause: unknown): StartupError {
    return new StartupError({
      phase: "configuration",
      detail:
        cause instanceof DaemonConfigurationError ? cause.message : undefined,
    });
  }

  /** Format refusal retains its reason at the process boundary. */
  const storeStartupReasons = {
    closed: undefined,
    conflict: undefined,
    corrupt: undefined,
    incompatible: "incompatible",
    "invalid-continuation": undefined,
    "invalid-input": undefined,
    "not-found": undefined,
    persistence: undefined,
  } satisfies Record<EndpointStoreError["reason"], StartupError["reason"]>;

  function storageFailure(
    error: EndpointStoreError | OutboxTooOldError | IdentityError,
  ): StartupError {
    switch (error._tag) {
      case "IdentityError":
        return new StartupError({ phase: "storage" });
      case "OutboxTooOldError":
        return new StartupError({ phase: "storage", reason: "outbox-too-old" });
      case "EndpointStoreError":
        return new StartupError({
          phase: "storage",
          reason: storeStartupReasons[error.reason],
        });
      default: {
        const exhaustive: never = error;
        return exhaustive;
      }
    }
  }
}

function networkLayer(configuration: DaemonProcessConfiguration) {
  return Layer.merge(
    Registry.layer({
      origin: configuration.registryOrigin,
      registrySignerPublicKey: configuration.registrySignerPublicKey,
      requestTimeout: REGISTRY_REQUEST_TIMEOUT,
    }),
    Router.layer({
      origin: configuration.routerOrigin,
      sendTimeout: ROUTER_SEND_TIMEOUT,
      pollTimeout: ROUTER_POLL_TIMEOUT,
    }),
  ).pipe(Layer.provide(NodeHttpClient.layer));
}

/** Immutable resolution and registration require only an injected HTTP client. */
export { Registry } from "../identity/registry/index.js";

/** Closed network operation values and client failures for service embedders. */
export { Router } from "../communication/router/api/client.js";
/** Route refusals and transport failures retain separate typed channels. */
export {
  PollCursor,
  RouterConnectionError,
  RouterInstanceId,
  RouterInvalidResponseError,
  RouterPollRequest,
  RouterPollResult,
  RouterRequestTimeoutError,
  RouterSendRequest,
  RouterSendResult,
  SignedMessageDigest,
} from "../communication/router/api/contract.js";
/** Registration and immutable reads preserve exact operation errors and results. */
export {
  OperationId,
  RegistryConnectionError,
  RegistryInvalidResponseError,
  RegistryLookupRequest,
  type RegistryLookupResult,
  RegistryRegisterRequest,
  type RegistryRegisterResult,
  RegistryRequestTimeoutError,
} from "../identity/registry/contract.js";
