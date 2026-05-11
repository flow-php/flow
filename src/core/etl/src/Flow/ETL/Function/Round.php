<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class Round extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $value,
        private readonly ScalarFunction|int $precision = 0,
        private readonly ScalarFunction|int $mode = PHP_ROUND_HALF_UP,
    ) {}

    public function eval(Row $row, FlowContext $context): int|float|null
    {
        $value = (new Parameter($this->value))->asNumber($row, $context);
        $precision = (new Parameter($this->precision))->asInt($row, $context);
        $mode = (new Parameter($this->mode))->asInt($row, $context);

        if ($value === null || $precision === null || $mode === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Round function requires non-null values'));
        }

        if ($mode < 1 || $mode > 4) {
            $mode = 1;
        }

        return $precision === 0 ? (int) \round($value, $precision, $mode) : \round($value, $precision, $mode);
    }
}
