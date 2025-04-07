<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\Row;

final class Minus extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $left,
        private readonly ScalarFunction|int|float $right,
    ) {
    }

    public function eval(Row $row) : int|float|null
    {
        $leftValue = (new Parameter($this->left))->asNumber($row);
        $rightValue = (new Parameter($this->right))->asNumber($row);

        if ($leftValue === null || $rightValue === null) {
            return null;
        }

        return (new Calculator())->subtract($leftValue, $rightValue);
    }
}
