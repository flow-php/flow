<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\{Calculator, Rounding};
use Flow\ETL\Row;

final class Divide extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float|string $left,
        private readonly ScalarFunction|int|float|string $right,
        private readonly ScalarFunction|int|null $scale = null,
        private readonly ScalarFunction|Rounding|null $rounding = null,
    ) {
    }

    public function eval(Row $row) : int|float|null
    {
        $leftValue = (new Parameter($this->left))->asNumber($row);
        $rightValue = (new Parameter($this->right))->asNumber($row);
        $scale = (new Parameter($this->scale))->asInt($row);
        $rounding = (new Parameter($this->rounding))->asEnum($row, Rounding::class);

        if ($leftValue === null || $rightValue === null) {
            return null;
        }

        if ($rightValue === 0) {
            return null;
        }

        return (new Calculator())->divide($leftValue, $rightValue, $scale, $rounding);
    }
}
