<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;
use Flow\Types\Type\ValueComparator;

final class NotSame extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {
    }

    public function eval(Row $row) : bool
    {
        $left = (new Parameter($this->left))->eval($row);
        $right = (new Parameter($this->right))->eval($row);

        (new ValueComparator())->assertComparable($left, $right, '!==');

        return $left !== $right;
    }
}
