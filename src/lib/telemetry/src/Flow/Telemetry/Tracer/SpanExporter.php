<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\Transport\Transport;

/**
 * Interface for exporting spans to external systems.
 *
 * Exporters are responsible for transmitting span data to backends
 * like Jaeger, Zipkin, OTLP collectors, or custom storage.
 */
interface SpanExporter
{
    /**
     * Export a batch of spans.
     *
     * Each span carries its own Resource and InstrumentationScope.
     *
     * @param array<Span> $spans The spans to export
     *
     * @return bool True on success, false on failure
     */
    public function export(array $spans) : bool;

    /**
     * Get the transports used by this exporter.
     *
     * @return array<Transport> Always returns at least one transport
     */
    public function transports() : array;
}
