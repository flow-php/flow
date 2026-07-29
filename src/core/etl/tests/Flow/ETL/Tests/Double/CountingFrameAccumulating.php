<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\FrameAccumulating;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;

/**
 * Sums the referenced entry over the frame, counting how many accumulators WindowProcessor builds and
 * how many rows it feeds them. Pins the incremental strategy: a partition of n rows must never cost more
 * than n accumulate() calls when the frame only grows.
 */
final class CountingFrameAccumulating implements FrameAccumulating, WindowFunction
{
    public int $accumulateCalls = 0;

    public int $accumulatorCalls = 0;

    public int $valueCalls = 0;

    private ?Window $window = null;

    public function __construct(
        private readonly Reference $ref,
    ) {}

    public function accumulator(FlowContext $context): FrameAccumulator
    {
        $this->accumulatorCalls++;

        return new CountingFrameAccumulator($this->ref, $this);
    }

    public function apply(WindowContext $window): mixed
    {
        $accumulator = $this->accumulator($window->flowContext());

        foreach ($window->frame() as $frameRow) {
            $accumulator->accumulate($frameRow);
        }

        return $accumulator->value();
    }

    public function over(Window $window): static
    {
        $this->window = $window;

        return $this;
    }

    public function toString(): string
    {
        return 'counting_frame_accumulating()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}
