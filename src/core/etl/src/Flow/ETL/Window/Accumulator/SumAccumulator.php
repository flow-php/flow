<?php

declare(strict_types=1);

namespace Flow\ETL\Window\Accumulator;

use Flow\Calculator\RunningSum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window\FrameAccumulator;

use function is_numeric;

final class SumAccumulator implements FrameAccumulator
{
    /**
     * Constant for a bool $exact, null when it is a ScalarFunction and has to be resolved per row.
     */
    private readonly ?bool $constantExact;

    private readonly RunningSum $runningSum;

    private float|int|null $sum = null;

    public function __construct(
        private readonly Reference $ref,
        private readonly ScalarFunction|bool $exact,
        private readonly FlowContext $context,
    ) {
        $this->constantExact = is_bool($exact) ? $exact : null;
        $this->runningSum = new RunningSum($context->calculator());
    }

    public function accumulate(Row $row): void
    {
        try {
            $value = $row->get($this->ref);

            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->sum = $this->runningSum->add(
                    $this->sum ?? 0,
                    $value,
                    $this->constantExact ?? (new Parameter($this->exact))->asBoolean($row, $this->context) ?? false,
                );
            }
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('Sum window function error: ' . $e->getMessage(), 0, $e);
        }
    }

    public function value(): float|int|null
    {
        return $this->sum;
    }
}
