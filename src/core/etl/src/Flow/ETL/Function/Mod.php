<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\Row;

final class Mod extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $left,
        private readonly ScalarFunction|int|float $right,
        private readonly ScalarFunction|int $scale = 0,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $leftValue = (new Parameter($this->left))->asNumber($row);
        $rightValue = (new Parameter($this->right))->asNumber($row);
        $scale = (new Parameter($this->scale))->asInt($row, 6);

        if ($leftValue === null || $rightValue === null) {
            return null;
        }

        if ($rightValue === 0) {
            return null;
        }

        $leftEntry = (new Parameter($this->left))->asEntry($row);
        $rightEntry = (new Parameter($this->right))->asEntry($row);

        if ($leftEntry instanceof Row\Entry\FloatEntry || $rightEntry instanceof Row\Entry\FloatEntry) {
            $scale = max(
                $leftEntry instanceof Row\Entry\FloatEntry ? $leftEntry->precision : 6,
                $rightEntry instanceof Row\Entry\FloatEntry ? $rightEntry->precision : 6,
                $scale
            );
        }

        return (new Calculator())->modulus($leftValue, $rightValue, $scale);
    }
}
