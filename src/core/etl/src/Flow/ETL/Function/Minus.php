<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_float, type_integer};
use Flow\Calculator\Calculator;
use Flow\ETL\Function\Math\FloatScale;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;

final class Minus extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $left,
        private readonly ScalarFunction|int|float $right,
        private readonly ScalarFunction|int|null $scale = null,
    ) {
    }

    public function eval(Row $row) : ?ScalarResult
    {
        $leftValue = (new Parameter($this->left))->asNumber($row);
        $rightValue = (new Parameter($this->right))->asNumber($row);

        if ($leftValue === null || $rightValue === null) {
            return null;
        }

        if (\is_int($leftValue) && \is_int($rightValue)) {
            return new ScalarResult($leftValue - $rightValue, type_integer());
        }

        $leftScale = (new FloatScale($this->left, $this->scale))->scale($row);
        $rightScale = (new FloatScale($this->left, $this->scale))->scale($row);

        $scale = max($leftScale, $rightScale);

        $result = (new Calculator())->subtract($leftValue, $rightValue, $scale);

        if ($scale === 0) {
            return new ScalarResult($result, type_integer());
        }

        return new ScalarResult($result, type_float(precision: $scale));
    }
}
