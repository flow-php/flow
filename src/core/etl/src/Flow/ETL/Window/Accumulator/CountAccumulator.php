<?php

declare(strict_types=1);

namespace Flow\ETL\Window\Accumulator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window\FrameAccumulator;

final class CountAccumulator implements FrameAccumulator
{
    private int $count = 0;

    public function __construct(
        private readonly ?Reference $ref,
        FlowContext $context,
    ) {}

    public function accumulate(Rows $rows, int $index): void
    {
        if ($this->ref === null) {
            $this->count++;

            return;
        }

        try {
            if ($rows->column($this->ref->base())->value($index) !== null) {
                $this->count++;
            }
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('Count window function error: ' . $e->getMessage(), 0, $e);
        }
    }

    public function value(): float|int|null
    {
        return $this->count;
    }
}
