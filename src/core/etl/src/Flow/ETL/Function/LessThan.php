<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};
use Flow\Types\Type\ValueComparator;

final class LessThan extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $left = (new Parameter($this->left))->eval($row, $context);
        $leftType = (new Parameter($this->left))->asType($row, $context);
        $right = (new Parameter($this->right))->eval($row, $context);
        $rightType = (new Parameter($this->right))->asType($row, $context);

        (new ValueComparator())->assertComparableTypes($leftType, $rightType, '<');

        if ($left === null || $right === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('LessThan function requires non-null values'));
        }

        return $left < $right;
    }
}
