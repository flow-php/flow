<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Round extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $value,
        private readonly ScalarFunction|int $precision = 0,
        private readonly ScalarFunction|int $mode = PHP_ROUND_HALF_UP,
    ) {
    }

    public function eval(Row $row) : ?float
    {
        $value = (new Parameter($this->value))->asNumber($row);
        $precision = (new Parameter($this->precision))->asInt($row);
        $mode = (new Parameter($this->mode))->asInt($row);

        if ($value === null || $precision === null || $mode === null) {
            return null;
        }

        if ($mode < 1 || $mode > 4) {
            $mode = 1;
        }

        return \round($value, $precision, $mode);
    }
}
