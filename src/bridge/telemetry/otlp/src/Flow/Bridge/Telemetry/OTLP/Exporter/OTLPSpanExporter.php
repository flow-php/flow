<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Exporter;

use Flow\Telemetry\Tracer\{Span, SpanExporter};
use Flow\Telemetry\Transport\Transport;

/**
 * Exports spans to OTLP endpoint.
 *
 * Example usage:
 * ```php
 * $exporter = new OTLPSpanExporter(
 *     transport: $httpTransport,
 * );
 *
 * $exporter->export($spans);
 * ```
 */
final readonly class OTLPSpanExporter implements SpanExporter
{
    public function __construct(
        private Transport $transport,
    ) {
    }

    /**
     * @param array<Span> $spans
     */
    public function export(array $spans) : bool
    {
        if (\count($spans) === 0) {
            return true;
        }

        try {
            $this->transport->sendSpans($spans);

            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [$this->transport];
    }
}
