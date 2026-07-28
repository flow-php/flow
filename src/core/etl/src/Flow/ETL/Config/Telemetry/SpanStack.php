<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;

use function array_pop;

final class SpanStack
{
    /**
     * @var array<Span>
     */
    private array $spans = [];

    public function drain(Tracer $tracer, string $reason): void
    {
        while (($span = array_pop($this->spans)) !== null) {
            $tracer->complete($span->setStatus(SpanStatus::error($reason)));
        }
    }

    public function pop(): ?Span
    {
        return array_pop($this->spans);
    }

    public function push(Span $span): void
    {
        $this->spans[] = $span;
    }
}
