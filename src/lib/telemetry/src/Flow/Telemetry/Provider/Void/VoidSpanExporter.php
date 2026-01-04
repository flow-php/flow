<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Tracer\{Span, SpanExporter};
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * No-op span exporter that discards all data.
 */
final readonly class VoidSpanExporter implements SpanExporter
{
    /**
     * @param array<Span> $spans
     */
    public function export(array $spans) : bool
    {
        return true;
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }
}
