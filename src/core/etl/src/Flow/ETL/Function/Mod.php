<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\{FlowContext, Row};

final class Mod extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int $left,
        private readonly ScalarFunction|int $right,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : ?int
    {
        $leftValue = (new Parameter($this->left))->asInt($row, $context);
        $rightValue = (new Parameter($this->right))->asInt($row, $context);

        if ($leftValue === null || $rightValue === null) {
            return null;
        }

        if ($rightValue === 0) {
            return null;
        }

        return (new Calculator())->modulus($leftValue, $rightValue);
    }
}
