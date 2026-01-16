<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\Tracer\{Span, SpanExporter};
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * Exporter that stores spans in memory for direct access.
 *
 * Useful for testing and inspection where you need direct access
 * to exported data without serialization.
 */
final class MemorySpanExporter implements SpanExporter
{
    /**
     * @var array<Span>
     */
    private array $spans = [];

    /**
     * @param array<Span> $spans
     */
    public function export(array $spans) : bool
    {
        foreach ($spans as $span) {
            $this->spans[] = $span;
        }

        return true;
    }

    /**
     * Reset all stored data.
     */
    public function reset() : void
    {
        $this->spans = [];
    }

    /**
     * Get all exported spans.
     *
     * @return array<Span>
     */
    public function spans() : array
    {
        return $this->spans;
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }
}
