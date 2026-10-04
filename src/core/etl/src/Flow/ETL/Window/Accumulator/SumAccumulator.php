<?php

declare(strict_types=1);

namespace Flow\ETL\Window\Accumulator;

use Flow\Calculator\RunningSum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window\FrameAccumulator;

use function is_numeric;

final class SumAccumulator implements FrameAccumulator
{
    private readonly RunningSum $runningSum;

    private bool $summed = false;

    public function __construct(
        private readonly Reference $ref,
        private readonly bool $exact,
        FlowContext $context,
    ) {
        $this->runningSum = new RunningSum($context->calculator());
    }

    public function accumulate(Rows $rows, int $index): void
    {
        try {
            // @mago-ignore analysis:mixed-assignment
            $value = $rows->column($this->ref->base())->value($index);

            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->runningSum->add($value, $this->exact);
                $this->summed = true;
            }
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('Sum window function error: ' . $e->getMessage(), 0, $e);
        }
    }

    public function value(): float|int|null
    {
        return $this->summed ? $this->runningSum->value() : null;
    }
}
