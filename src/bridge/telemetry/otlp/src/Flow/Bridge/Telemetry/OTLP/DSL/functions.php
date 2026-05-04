<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\DSL;

use Flow\Bridge\Telemetry\OTLP\Exporter\{OTLPLogExporter, OTLPMetricExporter, OTLPSpanExporter};
use Flow\Bridge\Telemetry\OTLP\Serializer\{JsonSerializer, ProtobufSerializer};
use Flow\Bridge\Telemetry\OTLP\Transport\{CurlTransport, CurlTransportOptions, GrpcTransport};
use Flow\ETL\Attribute\{DocumentationDSL, Module, Type as DSLType};
use Flow\Telemetry\Context\{ContextStorage, MemoryContextStorage};
use Flow\Telemetry\Logger\{LogProcessor, LoggerProvider};
use Flow\Telemetry\Meter\{AggregationTemporality, MetricProcessor};
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Serializer\Serializer;
use Flow\Telemetry\Tracer\Sampler\{AlwaysOnSampler, Sampler};
use Flow\Telemetry\Tracer\{SpanProcessor, TracerProvider};
use Flow\Telemetry\Transport\Transport;
use Psr\Clock\ClockInterface;

/**
 * Create a JSON serializer for OTLP.
 *
 * Returns a JsonSerializer that converts telemetry data to OTLP JSON wire format.
 * Use this with CurlTransport for JSON over HTTP.
 *
 * Example usage:
 * ```php
 * $serializer = otlp_json_serializer();
 * $transport = otlp_curl_transport($endpoint, $serializer);
 * ```
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_json_serializer() : JsonSerializer
{
    return new JsonSerializer();
}

/**
 * Create a Protobuf serializer for OTLP.
 *
 * Returns a ProtobufSerializer that converts telemetry data to OTLP Protobuf binary format.
 * Use this with CurlTransport for Protobuf over HTTP, or with GrpcTransport.
 *
 * Requires:
 * - google/protobuf package
 * - open-telemetry/gen-otlp-protobuf package
 *
 * Example usage:
 * ```php
 * $serializer = otlp_protobuf_serializer();
 * $transport = otlp_curl_transport($endpoint, $serializer);
 * ```
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_protobuf_serializer() : ProtobufSerializer
{
    return new ProtobufSerializer();
}

/**
 * Create a gRPC transport for OTLP endpoints.
 *
 * Creates a GrpcTransport configured to send telemetry data to an OTLP-compatible
 * endpoint using gRPC protocol with Protobuf serialization.
 *
 * Requires:
 * - ext-grpc PHP extension
 * - google/protobuf package
 * - open-telemetry/gen-otlp-protobuf package
 *
 * Example usage:
 * ```php
 * $transport = otlp_grpc_transport(
 *     endpoint: 'localhost:4317',
 *     serializer: otlp_protobuf_serializer(),
 * );
 * ```
 *
 * @param string $endpoint gRPC endpoint (e.g., 'localhost:4317')
 * @param ProtobufSerializer $serializer Protobuf serializer for encoding telemetry data
 * @param array<string, string> $headers Additional headers (metadata) to include in requests
 * @param bool $insecure Whether to use insecure channel credentials (default true for local dev)
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_grpc_transport(
    string $endpoint,
    ProtobufSerializer $serializer,
    array $headers = [],
    bool $insecure = true,
) : GrpcTransport {
    return new GrpcTransport($endpoint, $serializer, $headers, $insecure);
}

/**
 * Create curl transport options for OTLP.
 *
 * Returns a CurlTransportOptions builder for configuring curl transport settings
 * using a fluent interface.
 *
 * Example usage:
 * ```php
 * $options = otlp_curl_options()
 *     ->withTimeout(60)
 *     ->withConnectTimeout(15)
 *     ->withHeader('Authorization', 'Bearer token')
 *     ->withCompression()
 *     ->withSslVerification(verifyPeer: true);
 *
 * $transport = otlp_curl_transport($endpoint, $serializer, $options);
 * ```
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_curl_options() : CurlTransportOptions
{
    return new CurlTransportOptions();
}

/**
 * Create an async curl transport for OTLP endpoints.
 *
 * Creates a CurlTransport that uses curl_multi for non-blocking I/O.
 * Requests are queued and executed asynchronously. Completed requests are
 * processed on subsequent send() calls or on shutdown().
 *
 * Requires: ext-curl PHP extension
 *
 * Example usage:
 * ```php
 * // JSON over HTTP (async) with default options
 * $transport = otlp_curl_transport(
 *     endpoint: 'http://localhost:4318',
 *     serializer: otlp_json_serializer(),
 * );
 *
 * // Protobuf over HTTP (async) with custom options
 * $transport = otlp_curl_transport(
 *     endpoint: 'http://localhost:4318',
 *     serializer: otlp_protobuf_serializer(),
 *     options: otlp_curl_options()
 *         ->withTimeout(60)
 *         ->withHeader('Authorization', 'Bearer token')
 *         ->withCompression(),
 * );
 * ```
 *
 * @param string $endpoint OTLP endpoint URL (e.g., 'http://localhost:4318')
 * @param Serializer $serializer Serializer for encoding telemetry data (JSON or Protobuf)
 * @param CurlTransportOptions $options Transport configuration options
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_curl_transport(
    string $endpoint,
    Serializer $serializer,
    CurlTransportOptions $options = new CurlTransportOptions(),
) : CurlTransport {
    return new CurlTransport($endpoint, $serializer, $options);
}

/**
 * Create an OTLP span exporter.
 *
 * Example usage:
 * ```php
 * $exporter = otlp_span_exporter($transport);
 * $processor = batching_span_processor($exporter);
 * ```
 *
 * @param Transport $transport The transport for sending span data
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_span_exporter(Transport $transport) : OTLPSpanExporter
{
    return new OTLPSpanExporter($transport);
}

/**
 * Create an OTLP metric exporter.
 *
 * Example usage:
 * ```php
 * $exporter = otlp_metric_exporter($transport);
 * $processor = batching_metric_processor($exporter);
 * ```
 *
 * @param Transport $transport The transport for sending metric data
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_metric_exporter(Transport $transport) : OTLPMetricExporter
{
    return new OTLPMetricExporter($transport);
}

/**
 * Create an OTLP log exporter.
 *
 * Example usage:
 * ```php
 * $exporter = otlp_log_exporter($transport);
 * $processor = batching_log_processor($exporter);
 * ```
 *
 * @param Transport $transport The transport for sending log data
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_log_exporter(Transport $transport) : OTLPLogExporter
{
    return new OTLPLogExporter($transport);
}

/**
 * Create a tracer provider configured for OTLP export.
 *
 * Example usage:
 * ```php
 * $processor = batching_span_processor(otlp_span_exporter($transport));
 * $provider = otlp_tracer_provider($processor, $clock);
 * $tracer = $provider->tracer($resource, 'my-service', '1.0.0');
 * ```
 *
 * @param SpanProcessor $processor The processor for handling spans
 * @param ClockInterface $clock The clock for timestamps
 * @param Sampler $sampler The sampler for deciding whether to record spans
 * @param ContextStorage $contextStorage The context storage for propagating trace context
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_tracer_provider(
    SpanProcessor $processor,
    ClockInterface $clock,
    Sampler $sampler = new AlwaysOnSampler(),
    ContextStorage $contextStorage = new MemoryContextStorage(),
) : TracerProvider {
    return new TracerProvider($processor, $clock, $contextStorage, $sampler);
}

/**
 * Create a meter provider configured for OTLP export.
 *
 * Example usage:
 * ```php
 * $processor = batching_metric_processor(otlp_metric_exporter($transport));
 * $provider = otlp_meter_provider($processor, $clock);
 * $meter = $provider->meter($resource, 'my-service', '1.0.0');
 * ```
 *
 * @param MetricProcessor $processor The processor for handling metrics
 * @param ClockInterface $clock The clock for timestamps
 * @param AggregationTemporality $temporality The aggregation temporality for metrics
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_meter_provider(
    MetricProcessor $processor,
    ClockInterface $clock,
    AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
) : MeterProvider {
    return new MeterProvider($processor, $clock, $temporality);
}

/**
 * Create a logger provider configured for OTLP export.
 *
 * Example usage:
 * ```php
 * $processor = batching_log_processor(otlp_log_exporter($transport));
 * $provider = otlp_logger_provider($processor, $clock);
 * $logger = $provider->logger($resource, 'my-service', '1.0.0');
 * ```
 *
 * @param LogProcessor $processor The processor for handling log records
 * @param ClockInterface $clock The clock for timestamps
 * @param ContextStorage $contextStorage The context storage for propagating context
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_logger_provider(
    LogProcessor $processor,
    ClockInterface $clock,
    ContextStorage $contextStorage = new MemoryContextStorage(),
) : LoggerProvider {
    return new LoggerProvider($processor, $clock, $contextStorage);
}
