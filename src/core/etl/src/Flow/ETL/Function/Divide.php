<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\{Calculator, Rounding};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

final class Divide extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float|string $left,
        private readonly ScalarFunction|int|float|string $right,
        private readonly ScalarFunction|int|null $scale = null,
        private readonly ScalarFunction|Rounding|null $rounding = null,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : int|float|null
    {
        $leftValue = (new Parameter($this->left))->asNumber($row, $context);
        $rightValue = (new Parameter($this->right))->asNumber($row, $context);
        $scale = (new Parameter($this->scale))->asInt($row, $context);
        $rounding = (new Parameter($this->rounding))->asEnum($row, $context, Rounding::class);

        if ($leftValue === null || $rightValue === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Divide function requires non-null values'));
        }

        if ($rightValue === 0) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Divide function cannot divide by zero'));
        }

        return (new Calculator())->divide($leftValue, $rightValue, $scale, $rounding);
    }
}
