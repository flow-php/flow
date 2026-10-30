<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Telemetry;

use Flow\Telemetry\Tracer\SpanStatus;
use Flow\Telemetry\Tracer\Tracer;

use function array_pop;
use function array_search;
use function array_splice;

final class SpanStack
{
    /**
     * @var list<SpanScope>
     */
    private array $spans = [];

    public function drain(Tracer $tracer, string $reason): void
    {
        while (($spanScope = array_pop($this->spans)) !== null) {
            $spanScope->scope?->detach();
            $tracer->complete($spanScope->span->setStatus(SpanStatus::error($reason)));
        }
    }

    public function push(SpanScope $spanScope): void
    {
        $this->spans[] = $spanScope;
    }

    /**
     * @return bool false when $spanScope is not open: completed already, or drained
     */
    public function remove(SpanScope $spanScope): bool
    {
        $position = array_search($spanScope, $this->spans, true);

        if ($position === false) {
            return false;
        }

        array_splice($this->spans, $position, 1);

        return true;
    }
}
