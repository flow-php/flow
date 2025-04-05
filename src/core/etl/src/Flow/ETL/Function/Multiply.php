<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_float, type_integer};
use Flow\Calculator\Calculator;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;

final class Multiply extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $left,
        private readonly ScalarFunction|int|float $right,
        private readonly ScalarFunction|int $scale = 0,
    ) {
    }

    public function eval(Row $row) : ?ScalarResult
    {
        $leftValue = (new Parameter($this->left))->asNumber($row);
        $rightValue = (new Parameter($this->right))->asNumber($row);
        $scale = (new Parameter($this->scale))->asInt($row, 0);

        if ($leftValue === null || $rightValue === null) {
            return null;
        }

        $leftEntry = (new Parameter($this->left))->asEntry($row);
        $rightEntry = (new Parameter($this->right))->asEntry($row);

        if ($leftEntry instanceof Row\Entry\FloatEntry || $rightEntry instanceof Row\Entry\FloatEntry) {
            $scale = max(
                $leftEntry instanceof Row\Entry\FloatEntry ? $leftEntry->precision : 0,
                $rightEntry instanceof Row\Entry\FloatEntry ? $rightEntry->precision : 0,
                $scale
            );
        }

        $result = (new Calculator())->multiply($leftValue, $rightValue, $scale);

        if (\is_int($result)) {
            return new ScalarResult($result, type_integer());
        }

        return new ScalarResult($result, type_float(precision: $scale));
    }
}
