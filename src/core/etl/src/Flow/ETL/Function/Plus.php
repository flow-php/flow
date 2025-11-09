<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Plus extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $left,
        private readonly ScalarFunction|int|float $right,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : int|float|null
    {
        $leftValue = (new Parameter($this->left))->asNumber($row, $context);
        $rightValue = (new Parameter($this->right))->asNumber($row, $context);

        if ($leftValue === null || $rightValue === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Plus function requires non-null values'));
        }

        return (new Calculator())->add($leftValue, $rightValue);
    }
}
