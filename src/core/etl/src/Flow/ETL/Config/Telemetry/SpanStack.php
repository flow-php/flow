<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

use Flow\Telemetry\Context\Scope;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;

use function array_pop;

final class SpanStack
{
    /**
     * @var array<SpanScope>
     */
    private array $spans = [];

    public function drain(Tracer $tracer, string $reason): void
    {
        while (($spanScope = array_pop($this->spans)) !== null) {
            $spanScope->scope->detach();
            $tracer->complete($spanScope->span->setStatus(SpanStatus::error($reason)));
        }
    }

    public function pop(): ?SpanScope
    {
        return array_pop($this->spans);
    }

    public function push(Span $span, Scope $scope): void
    {
        $this->spans[] = new SpanScope($span, $scope);
    }
}
