<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Tracer\Span;
use Opentelemetry\Proto\Collector\Logs\V1\ExportLogsServiceRequest;
use Opentelemetry\Proto\Collector\Metrics\V1\ExportMetricsServiceRequest;
use Opentelemetry\Proto\Collector\Trace\V1\ExportTraceServiceRequest;

/**
 * Interface for gRPC serializers that create OTLP protobuf request objects.
 *
 * Unlike the Serializer interface which returns string payloads,
 * this interface returns protobuf request objects for direct gRPC transmission.
 */
interface GrpcSerializer
{
    /**
     * Create an ExportLogsServiceRequest for gRPC transport.
     *
     * @param array<LogEntry> $entries
     */
    public function createLogsRequest(array $entries) : ExportLogsServiceRequest;

    /**
     * Create an ExportMetricsServiceRequest for gRPC transport.
     *
     * @param array<Metric> $metrics
     */
    public function createMetricsRequest(array $metrics) : ExportMetricsServiceRequest;

    /**
     * Create an ExportTraceServiceRequest for gRPC transport.
     *
     * @param array<Span> $spans
     */
    public function createSpansRequest(array $spans) : ExportTraceServiceRequest;
}
