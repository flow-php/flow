<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\DSL;

use Flow\Bridge\Telemetry\OTLP\Exporter\OTLPExporter;
use Flow\Bridge\Telemetry\OTLP\Serializer\JsonSerializer;
use Flow\Bridge\Telemetry\OTLP\Serializer\ProtobufSerializer;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\AsyncCurlTransportOptions;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\CurlTransportOptions;
use Flow\Bridge\Telemetry\OTLP\Transport\GrpcTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\StreamTransport;
use Flow\Bridge\Telemetry\OTLP\Transport\Transport;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type as DSLType;
use Flow\Telemetry\Context\ContextStorage;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Logger\LogProcessor;
use Flow\Telemetry\Meter\AggregationTemporality;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\MetricProcessor;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\ParentBasedSampler;
use Flow\Telemetry\Tracer\Sampler\Sampler;
use Flow\Telemetry\Tracer\SpanProcessor;
use Flow\Telemetry\Tracer\TracerProvider;
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
function otlp_json_serializer(): JsonSerializer
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
 *
 * Example usage:
 * ```php
 * $serializer = otlp_protobuf_serializer();
 * $transport = otlp_curl_transport($endpoint, $serializer);
 * ```
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_protobuf_serializer(): ProtobufSerializer
{
    return new ProtobufSerializer();
}

/**
 * Create a gRPC transport for OTLP endpoints.
 *
 * Creates a GrpcTransport configured to send telemetry data to an OTLP-compatible
 * endpoint using gRPC protocol with Protobuf serialization. OTLP/gRPC mandates
 * Protobuf, so the serializer is built internally and not configurable.
 *
 * Requires:
 * - ext-grpc PHP extension
 * - google/protobuf package
 *
 * @param string $endpoint gRPC endpoint (e.g., 'localhost:4317')
 * @param array<string, string> $headers Additional headers (metadata) to include in requests
 * @param bool $insecure Whether to use insecure channel credentials (default true for local dev)
 * @param int $timeoutMs Per-call deadline in milliseconds (covers connect + send + receive)
 * @param int $shutdownTimeoutMs Wall-clock budget for draining pending calls at shutdown
 * @param ?Transport $failover Optional failover transport receiving prior batches when primary fails
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_grpc_transport(
    string $endpoint,
    array $headers = [],
    bool $insecure = true,
    int $timeoutMs = GrpcTransport::DEFAULT_TIMEOUT_MS,
    int $shutdownTimeoutMs = GrpcTransport::DEFAULT_SHUTDOWN_TIMEOUT_MS,
    ?Transport $failover = null,
): Transport {
    return new GrpcTransport($endpoint, $headers, $insecure, $timeoutMs, $shutdownTimeoutMs, $failover);
}

/**
 * Create curl transport options for OTLP.
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_curl_options(): CurlTransportOptions
{
    return new CurlTransportOptions();
}

/**
 * Create a synchronous curl transport for OTLP endpoints.
 *
 * Creates a CurlTransport that drives each request to completion and reports the
 * outcome immediately (returns on success, throws on failure). OTLP/HTTP allows
 * JSON or Protobuf encoding; defaults to JSON. Keeping export off the application
 * hot path is the job of the batching processor in front of the exporter.
 *
 * Requires: ext-curl PHP extension
 *
 * @param string $endpoint OTLP endpoint URL (e.g., 'http://localhost:4318')
 * @param JsonSerializer|ProtobufSerializer $serializer Serializer for encoding telemetry data (JSON or Protobuf)
 * @param CurlTransportOptions $options Transport configuration options
 * @param ?Transport $failover Optional failover transport receiving the batch when the primary fails
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_curl_transport(
    string $endpoint,
    JsonSerializer|ProtobufSerializer $serializer = new JsonSerializer(),
    CurlTransportOptions $options = new CurlTransportOptions(),
    ?Transport $failover = null,
): Transport {
    return new CurlTransport($endpoint, $serializer, $options, $failover);
}

/**
 * Create async curl transport options for OTLP.
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_async_curl_options(): AsyncCurlTransportOptions
{
    return new AsyncCurlTransportOptions();
}

/**
 * Create an asynchronous curl transport for OTLP endpoints.
 *
 * @param string $endpoint OTLP endpoint URL (e.g., 'http://localhost:4318')
 * @param JsonSerializer|ProtobufSerializer $serializer Serializer for encoding telemetry data (JSON or Protobuf)
 * @param AsyncCurlTransportOptions $options Transport configuration options
 * @param ?Transport $failover Optional failover transport receiving prior batches when primary fails
 * @param ErrorHandler $error_handler Handler for failures reaped on send()/tick()/shutdown() (no failover)
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_async_curl_transport(
    string $endpoint,
    JsonSerializer|ProtobufSerializer $serializer = new JsonSerializer(),
    AsyncCurlTransportOptions $options = new AsyncCurlTransportOptions(),
    ?Transport $failover = null,
    ErrorHandler $error_handler = new ErrorLogHandler(),
): AsyncCurlTransport {
    return new AsyncCurlTransport($endpoint, $serializer, $options, $failover, $error_handler);
}

/**
 * Create a stream transport for OTLP that writes JSONL to a single destination.
 *
 * Accepts an absolute file path or a php:// stream wrapper such as
 * 'php://stdout', 'php://stderr', 'php://memory', or 'php://temp'. Each
 * export() call appends one JSON Line under LOCK_EX so concurrent writers
 * interleave at line boundaries. The $filePermissions and $createDirectories
 * parameters apply only when the destination is a file path.
 *
 * Per the OTLP File Exporter spec only JSON encoding is supported.
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_stream_transport(
    string $destination,
    int $filePermissions = 0644,
    bool $createDirectories = true,
): Transport {
    return new StreamTransport($destination, $filePermissions, $createDirectories);
}

/**
 * Create an OTLP exporter that dispatches logs, metrics, and spans through a single transport.
 *
 * Example usage:
 * ```php
 * $exporter = otlp_exporter($transport);
 * $spanProcessor = batching_span_processor($exporter);
 * $metricProcessor = batching_metric_processor($exporter);
 * $logProcessor = batching_log_processor($exporter);
 * ```
 *
 * @param Transport $transport The transport for sending telemetry data
 * @param ErrorHandler $errorHandler Handler for Throwables raised by the transport
 */
#[DocumentationDSL(module: Module::TELEMETRY_OTLP, type: DSLType::HELPER)]
function otlp_exporter(Transport $transport, ErrorHandler $errorHandler = new ErrorLogHandler()): OTLPExporter
{
    return new OTLPExporter($transport, $errorHandler);
}

/**
 * Create a tracer provider configured for OTLP export.
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
    Sampler $sampler = new ParentBasedSampler(new AlwaysOnSampler()),
    ContextStorage $contextStorage = new MemoryContextStorage(),
): TracerProvider {
    return new TracerProvider($processor, $clock, $contextStorage, $sampler);
}

/**
 * Create a meter provider configured for OTLP export.
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
): MeterProvider {
    return new MeterProvider($processor, $clock, $temporality);
}

/**
 * Create a logger provider configured for OTLP export.
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
): LoggerProvider {
    return new LoggerProvider($processor, $clock, $contextStorage);
}
