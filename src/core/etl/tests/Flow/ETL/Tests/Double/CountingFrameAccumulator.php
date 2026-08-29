<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window\FrameAccumulator;

final class CountingFrameAccumulator implements FrameAccumulator
{
    private float|int $sum = 0;

    public function __construct(
        private readonly Reference $ref,
        private readonly CountingFrameAccumulating $spy,
    ) {}

    public function accumulate(Row $row): void
    {
        $this->spy->accumulateCalls++;

        $value = $row->valueOf($this->ref);

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
