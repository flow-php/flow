<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_float, type_integer};
use Flow\Calculator\Calculator;
use Flow\ETL\Function\Math\FloatScale;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;

final class Divide extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float|string $left,
        private readonly ScalarFunction|int|float|string $right,
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

        if ($rightValue === 0) {
            return null;
        }

        $leftScale = (new FloatScale($this->left, $this->scale))->scale($row);
        $rightScale = (new FloatScale($this->left, $this->scale))->scale($row);

        $scale = max($leftScale, $rightScale);

        // we know that the result is going to be float so if the scale wasn't set
        // intentionally we set it to 6
        if ($scale === 0 && $this->scale === null && ($leftValue % $rightValue) !== 0) {
            $scale = 6;
        }

        var_dump(\json_encode(['scale' => $scale, 'left_scale' => $leftScale, 'right_scale' => $rightScale, 'left_value' => $leftValue,  'right_value' => $rightValue]));
        $result = (new Calculator())->divide($leftValue, $rightValue, $scale);

        if ($scale === 0) {
            return new ScalarResult($result, type_integer());
        }

        return new ScalarResult($result, type_float(precision: $scale));
    }
}
