<?php

declare(strict_types=1);

namespace Flow\ETL\Window\Accumulator;

use Flow\Calculator\Calculator;
use Flow\Calculator\Rounding;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window\FrameAccumulator;

use function is_numeric;

final class AverageAccumulator implements FrameAccumulator
{
    private readonly Calculator $calculator;

    private int $count = 0;

    private float|int $sum = 0;

    public function __construct(
        private readonly Reference $ref,
        private readonly int $scale,
        private readonly Rounding $rounding,
        FlowContext $context,
    ) {
        $this->calculator = $context->calculator();
    }

    public function accumulate(Row $row): void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if (is_numeric($value)) {
                // @mago-ignore analysis:possibly-invalid-argument
                $this->sum = $this->calculator->add($this->sum, $value);
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

        return $this->calculator->divide($this->sum, $this->count, $this->scale, $this->rounding);
    }
}
