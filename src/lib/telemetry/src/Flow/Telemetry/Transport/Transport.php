<?php

declare(strict_types=1);

namespace Flow\Telemetry\Transport;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Tracer\Span;

/**
 * Interface for sending telemetry data to backends.
 *
 * Transports handle the actual network communication to telemetry backends
 * like OTLP collectors, Jaeger, or other observability platforms.
 *
 * The signal type (traces, metrics, logs) is inferred from the payload type.
 *
 * Example implementation:
 * ```php
 * final class HttpTransport implements Transport
 * {
 *     public function sendSpans(array $spans): void
 *     {
 *         $json = $this->serializer->serializeSpans($spans);
 *         $this->client->post($this->endpoint . '/v1/traces', $json);
 *     }
 * }
 * ```
 */
interface Transport
{
    /**
     * Send log entries to the backend.
     *
     * @param array<LogEntry> $entries The log entries to export
     *
     * @throws TransportException On transport failure
     */
    public function sendLogs(array $entries) : void;

    /**
     * Send metrics to the backend.
     *
     * @param array<Metric> $metrics The metrics to export
     *
     * @throws TransportException On transport failure
     */
    public function sendMetrics(array $metrics) : void;

    /**
     * Send spans to the backend.
     *
     * @param array<Span> $spans The spans to export
     *
     * @throws TransportException On transport failure
     */
    public function sendSpans(array $spans) : void;

    /**
     * Shutdown the transport, releasing any resources.
     *
     * Should complete any pending sends before shutting down.
     * This method is idempotent - calling it multiple times is safe.
     */
    public function shutdown() : void;
}
