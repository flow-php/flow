<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\Types\Type\ValueComparator;

final class GreaterThan extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {
    }

    public function eval(Row $row) : bool
    {
        $left = (new Parameter($this->left))->eval($row);
        $leftType = (new Parameter($this->left))->asType($row);
        $right = (new Parameter($this->right))->eval($row);
        $rightType = (new Parameter($this->right))->asType($row);

        (new ValueComparator())->assertComparableTypes($leftType, $rightType, '>');

        if ($left === null || $right === null) {
            return false;
        }

        return $left > $right;
    }
}
