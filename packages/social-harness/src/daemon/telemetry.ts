/** @file Opt-in OTLP span export and process diagnostics for the daemon. */

import * as NodeSdk from "@effect/opentelemetry/NodeSdk";
import { diag, DiagConsoleLogger, DiagLogLevel } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { BatchSpanProcessor } from "@opentelemetry/sdk-trace-base";
import { Config, Effect, Either, Layer, Option } from "effect";
import { packageVersion } from "../release.js";
import { DaemonConfigurationError } from "./bootstrap.js";

/**
 * One standard OpenTelemetry variable, trimmed.
 * @param name An `OTEL_*` environment variable.
 * @returns Its value, or none when it is unset or blank, as OpenTelemetry's
 *     own exporter reads it.
 */
const otelVariable = (name: string) =>
  Config.option(Config.string(name)).pipe(
    Config.map((value) =>
      value.pipe(
        Option.map((text) => text.trim()),
        Option.filter((text) => text !== ""),
      ),
    ),
  );

const spanExportSettings = Config.all({
  endpoint: otelVariable("OTEL_EXPORTER_OTLP_ENDPOINT"),
  tracesEndpoint: otelVariable("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
  tracesProtocol: otelVariable("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"),
  protocol: otelVariable("OTEL_EXPORTER_OTLP_PROTOCOL"),
  serviceName: otelVariable("OTEL_SERVICE_NAME"),
  resourceAttributes: otelVariable("OTEL_RESOURCE_ATTRIBUTES"),
});

type SpanExportSettings = Config.Config.Success<typeof spanExportSettings>;

/**
 * The resource's service name. The resource layer lets an explicit name
 * override both standard variables, so the default applies only when neither
 * names one.
 * @param settings The span export settings read from the environment.
 * @returns `OTEL_SERVICE_NAME`, else the last `service.name` in
 *     `OTEL_RESOURCE_ATTRIBUTES` parsed the way the resource layer parses it,
 *     else `sharnessd`.
 */
function spanServiceName(settings: SpanExportSettings): string {
  const attributed = Option.getOrElse(settings.resourceAttributes, () => "")
    .split(",")
    .map((attribute) => attribute.split("="))
    .filter(
      (parts) => parts.length === 2 && parts[0]?.trim() === "service.name",
    )
    .at(-1)?.[1]
    ?.trim();
  return Option.getOrElse(
    settings.serviceName,
    () => attributed ?? "sharnessd",
  );
}

/**
 * The `v1/traces` URL under a base OTLP endpoint, keeping its query.
 * @param endpoint The configured `OTEL_EXPORTER_OTLP_ENDPOINT`.
 * @returns The traces URL, or `endpoint` unchanged when it does not parse,
 *     so the caller's check refuses it.
 */
function tracesUnder(endpoint: string): string {
  if (!URL.canParse(endpoint)) {
    return endpoint;
  }
  const url = new URL(endpoint);
  url.pathname = `${url.pathname.replace(/\/$/u, "")}/v1/traces`;
  return url.href;
}

/**
 * The URL spans are sent to, or why the settings cannot start an exporter.
 * The URL is `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, else `v1/traces` under
 * `OTEL_EXPORTER_OTLP_ENDPOINT`; the caller passes it to the exporter, so the
 * URL checked is the URL used. A URL
 * that is not http(s), which the exporter would replace with its localhost
 * default or could not reach, and a trace protocol other than http/protobuf
 * are refused.
 * @param endpoint The configured `OTEL_EXPORTER_OTLP_ENDPOINT`.
 * @param settings The span export settings read from the environment.
 * @returns The traces URL, or the refusal.
 */
function spanExportUrl(
  endpoint: string,
  settings: SpanExportSettings,
): Either.Either<string, DaemonConfigurationError> {
  const protocol = settings.tracesProtocol.pipe(
    Option.orElse(() => settings.protocol),
    Option.getOrElse(() => "http/protobuf"),
  );
  if (protocol !== "http/protobuf") {
    return Either.left(
      new DaemonConfigurationError({
        reason: "environment",
        message:
          "The OTLP trace protocol, OTEL_EXPORTER_OTLP_TRACES_PROTOCOL or else OTEL_EXPORTER_OTLP_PROTOCOL, must be http/protobuf. Docs: https://opentelemetry.io/docs/specs/otel/protocol/exporter/#specify-protocol",
      }),
    );
  }
  const url = Option.getOrElse(settings.tracesEndpoint, () =>
    tracesUnder(endpoint),
  );
  return URL.canParse(url) && /^https?:$/u.test(new URL(url).protocol)
    ? Either.right(url)
    : Either.left(
        new DaemonConfigurationError({
          reason: "environment",
          message:
            "The OTLP traces URL, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or else OTEL_EXPORTER_OTLP_ENDPOINT, must be an http or https URL. Docs: https://opentelemetry.io/docs/specs/otel/protocol/exporter/#endpoint-urls-for-otlphttp",
        }),
      );
}

/**
 * OpenTelemetry's own warnings and export failures, which go nowhere without
 * a logger, on standard error. The diag logger is process-global: export
 * takes it over, without the override warning, and leaves it in place.
 */
const exportDiagnostics = Layer.effectDiscard(
  Effect.sync(() =>
    diag.setLogger(new DiagConsoleLogger(), {
      logLevel: DiagLogLevel.WARN,
      suppressOverrideMessage: true,
    }),
  ),
);

/**
 * Exports every span the daemon records as OTLP/HTTP protobuf while
 * `OTEL_EXPORTER_OTLP_ENDPOINT` is set; unset, the layer is empty and no
 * exporter exists. Settings the exporter could not honour, per
 * {@link spanExportUrl}, are refused at startup rather than exported
 * somewhere or in a format the collector does not expect. The OpenTelemetry
 * exporter and batch processor read the rest of their standard variables
 * themselves, and the resource takes `OTEL_RESOURCE_ATTRIBUTES`.
 */
// eslint-disable-next-line agent-code-guard/require-span-on-exported-effect -- The exporter installs tracing; tracing its acquisition changes the daemon's exported spans.
export const spanExport: Layer.Layer<never, DaemonConfigurationError> =
  Layer.unwrapEffect(
    Effect.gen(function* () {
      const settings = yield* spanExportSettings.pipe(Effect.orDie);
      if (Option.isNone(settings.endpoint)) {
        return Layer.empty;
      }
      const url = yield* spanExportUrl(settings.endpoint.value, settings);
      return NodeSdk.layer(() => ({
        resource: {
          serviceName: spanServiceName(settings),
          serviceVersion: packageVersion,
        },
        spanProcessor: new BatchSpanProcessor(new OTLPTraceExporter({ url })),
      })).pipe(Layer.discard, Layer.provide(exportDiagnostics));
    }),
  );
