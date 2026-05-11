<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class Power extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $left,
        private readonly ScalarFunction|int $right,
    ) {}

    public function eval(Row $row, FlowContext $context): float|int|null
    {
        $leftValue = (new Parameter($this->left))->asNumber($row, $context);
        $rightValue = (new Parameter($this->right))->asInt($row, $context);

        if ($leftValue === null || $rightValue === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('Power function requires non-null values'));
        }

        return (new Calculator())->power($leftValue, $rightValue);
    }
}
