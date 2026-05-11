<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type\ValueComparator;

final class Equals extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {}

    public function eval(Row $row, FlowContext $context): bool
    {
        $left = (new Parameter($this->left))->eval($row, $context);
        $leftType = (new Parameter($this->left))->asType($row, $context);
        $right = (new Parameter($this->right))->eval($row, $context);
        $rightType = (new Parameter($this->right))->asType($row, $context);

        (new ValueComparator())->assertComparableTypes($leftType, $rightType, '==');

        return $left == $right;
    }
}
