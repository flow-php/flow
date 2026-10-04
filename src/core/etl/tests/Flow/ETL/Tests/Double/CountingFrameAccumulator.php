<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window\FrameAccumulator;

final class CountingFrameAccumulator implements FrameAccumulator
{
    private float|int $sum = 0;

    public function __construct(
        private readonly Reference $ref,
        private readonly CountingFrameAccumulating $spy,
    ) {}

    public function accumulate(Rows $rows, int $index): void
    {
        $this->spy->accumulateCalls++;

        // @mago-ignore analysis:mixed-assignment
        $value = $rows->column($this->ref->base())->value($index);

        if (is_int($value) || is_float($value)) {
            $this->sum += $value;
        }
    }

    public function value(): float|int|null
    {
        $this->spy->valueCalls++;

        return $this->sum;
    }
}
