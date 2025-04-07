<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\Row;

final class Power extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $left,
        private readonly ScalarFunction|int $right,
    ) {
    }

    public function eval(Row $row) : float|int|null
    {
        $leftValue = (new Parameter($this->left))->asNumber($row);
        $rightValue = (new Parameter($this->right))->asInt($row);

        if ($leftValue === null || $rightValue === null) {
            return null;
        }

        if ($rightValue === 0) {
            return null;
        }

        return (new Calculator())->power($leftValue, $rightValue);
    }
}
