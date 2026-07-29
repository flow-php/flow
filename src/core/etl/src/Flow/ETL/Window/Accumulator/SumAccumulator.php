<?php

declare(strict_types=1);

namespace Flow\ETL\Window\Accumulator;

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

    private float|int|null $sum = null;

    public function __construct(
        private readonly Reference $ref,
        private readonly ScalarFunction|bool $exact,
        private readonly FlowContext $context,
    ) {
        $this->constantExact = is_bool($exact) ? $exact : null;
    }

    public function accumulate(Row $row): void
    {
        try {
            $value = $row->valueOf($this->ref);

            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->sum = $this->add(
                    $this->sum ?? 0,
                    $value,
                    $this->constantExact ?? (new Parameter($this->exact))->asBoolean($row, $this->context),
                );
            }
        } catch (InvalidArgumentException $e) {
            $this->context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Sum window function error: ' . $e->getMessage(), 0, $e));
        }
    }

    public function value(): mixed
    {
        return $this->sum;
    }

    /**
     * @param float|int|numeric-string $value
     */
    private function add(float|int $sum, float|int|string $value, bool $exact): float|int
    {
        if ($exact) {
            return $this->context->calculator()->add($sum, $value);
        }

        $result = $sum + $value;

        if (
            is_float($result)
            && floor($result) === $result
            && $result >= (float) PHP_INT_MIN
            && $result < (float) PHP_INT_MAX
        ) {
            return (int) $result;
        }

        return $result;
    }
}
