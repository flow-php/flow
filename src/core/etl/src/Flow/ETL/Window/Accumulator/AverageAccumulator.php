<?php

declare(strict_types=1);

namespace Flow\ETL\Window\Accumulator;

use Flow\Calculator\Calculator;
use Flow\Calculator\Rounding;
use Flow\Calculator\RunningSum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window\FrameAccumulator;

use function is_numeric;

final class AverageAccumulator implements FrameAccumulator
{
    private readonly Calculator $calculator;

    private int $count = 0;

    private readonly RunningSum $runningSum;

    public function __construct(
        private readonly Reference $ref,
        private readonly int $scale,
        private readonly Rounding $rounding,
        private readonly bool $exact,
        FlowContext $context,
    ) {
        $this->calculator = $context->calculator();
        $this->runningSum = new RunningSum($this->calculator);
    }

    public function accumulate(Rows $rows, int $index): void
    {
        try {
            /** @var mixed $value */
            $value = $rows->column($this->ref->base())->value($index);

            if (is_numeric($value)) {
                // @mago-ignore analysis:possibly-invalid-argument
                $this->runningSum->add($value, $this->exact);
                $this->count++;
            }
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('Average window function error: ' . $e->getMessage(), 0, $e);
        }
    }

    public function value(): float|int|null
    {
        if (0 === $this->count) {
            return null;
        }

        return $this->calculator->divide($this->runningSum->value(), $this->count, $this->scale, $this->rounding);
    }
}
