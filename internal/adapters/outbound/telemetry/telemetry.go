// Package telemetry is the outbound adapter that wires OpenTelemetry into
// this service: a TracerProvider and a MeterProvider, both exporting over
// OTLP/gRPC to a Collector, plus Go runtime metrics, the HTTP-server RED
// middleware (http.go) and the Tier-2 business-metrics port
// implementation (metrics.go).
//
// It sits in the same tier as the postgres/kafka adapters -- the domain and
// application layers never import it. A composition root (cmd/network-
// inventory-planning, cmd/mcp, cmd/nip-reports, cmd/nip-projector) calls
// Setup once at startup and defers the returned shutdown func.
//
// Export is deliberately non-blocking: if no Collector is listening at the
// configured endpoint, spans and metrics are dropped silently and the
// service starts and serves exactly as it would without OTel.
//
// Copied (fleet convention, ADR 0011) from warehouse-planning's
// internal/adapters/outbound/telemetry package, with only the
// service.name resource attribute (passed in by the caller, not
// hardcoded here) differing at the call site.
package telemetry

import (
	"context"
	"errors"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// DefaultOTLPEndpoint is the OTel Collector's standard gRPC receiver port on
// localhost, used when no endpoint is configured.
const DefaultOTLPEndpoint = "localhost:4317"

// EnvOTLPEndpoint is the standard OTel env var the composition roots read.
const EnvOTLPEndpoint = "OTEL_EXPORTER_OTLP_ENDPOINT"

// DefaultServiceVersion is reported when SERVICE_VERSION is unset.
const DefaultServiceVersion = "dev"

// Service names, one per deployable (ADR 0011). The Prometheus
// `service_name` label carries these, which is what the warehouse-infra
// dashboard's service_regex (`network-inventory-planning.*`) selects on.
const (
	ServiceAPI       = "network-inventory-planning"
	ServiceMCP       = "network-inventory-planning-mcp"
	ServiceReports   = "network-inventory-planning-reports"
	ServiceProjector = "network-inventory-planning-projector"
)

// defaultEnvironment is the deployment.environment.name value used when the
// ENVIRONMENT env var is unset.
const defaultEnvironment = "local"

// metricExportInterval is how often the MeterProvider pushes to the
// Collector. Kept explicit rather than relying on the SDK default so the
// scrape cadence is visible in code review.
const metricExportInterval = 30 * time.Second

// Setup builds and installs the global TracerProvider and MeterProvider,
// both exporting via OTLP/gRPC to otlpEndpoint (DefaultOTLPEndpoint when
// empty), installs the W3C trace-context + baggage propagator, and starts
// Go runtime metrics collection.
//
// It returns a shutdown func that flushes and closes both providers; call it
// from the graceful-shutdown path. Setup never blocks on the Collector being
// reachable -- a missing Collector degrades to "telemetry dropped", never to
// "service won't start".
func Setup(ctx context.Context, serviceName, serviceVersion, otlpEndpoint string) (func(context.Context) error, error) {
	if otlpEndpoint == "" {
		otlpEndpoint = DefaultOTLPEndpoint
	}

	res, err := newResource(serviceName, serviceVersion)
	if err != nil {
		return nil, err
	}

	// WithInsecure and no grpc.WithBlock dial option: the exporter connects
	// lazily in the background, so New returns immediately whether or not a
	// Collector is listening.
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(otlpEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		// Nothing is installed globally yet, so unwind the tracer side.
		return nil, errors.Join(err, tracerProvider.Shutdown(ctx))
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(metricExportInterval),
		)),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if err := runtime.Start(runtime.WithMeterProvider(meterProvider)); err != nil {
		return nil, errors.Join(err,
			tracerProvider.Shutdown(ctx),
			meterProvider.Shutdown(ctx),
		)
	}

	return func(shutdownCtx context.Context) error {
		return errors.Join(
			tracerProvider.Shutdown(shutdownCtx),
			meterProvider.Shutdown(shutdownCtx),
		)
	}, nil
}

// SetupFromEnv is the one-liner the composition roots call: Setup with
// OTEL_EXPORTER_OTLP_ENDPOINT and SERVICE_VERSION read from the
// environment. Start-up never fails on telemetry: a setup error is
// returned for the caller to log, together with a non-nil no-op shutdown,
// so `defer shutdown(ctx)` is always safe.
func SetupFromEnv(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	version := os.Getenv("SERVICE_VERSION")
	if version == "" {
		version = DefaultServiceVersion
	}
	shutdown, err := Setup(ctx, serviceName, version, os.Getenv(EnvOTLPEndpoint))
	if err != nil || shutdown == nil {
		return func(context.Context) error { return nil }, err
	}
	return shutdown, nil
}

// newResource builds the resource shared by both providers. Attribute keys
// come from semconv constants rather than hand-typed strings so they track
// the spec version this module is built against.
func newResource(serviceName, serviceVersion string) (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
			semconv.DeploymentEnvironmentNameKey.String(Environment()),
		),
	)
}

// Environment returns the deployment environment from ENVIRONMENT,
// defaulting to "local".
func Environment() string {
	if v := os.Getenv("ENVIRONMENT"); v != "" {
		return v
	}
	return defaultEnvironment
}
