<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\Comparison\Comparable;
use Flow\ETL\Row;

final class GreaterThanEqual extends ScalarFunctionChain
{
    use Comparable;

    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {
    }

    public function eval(Row $row) : bool
    {
        $left = (new Parameter($this->left))->eval($row);
        $right = (new Parameter($this->right))->eval($row);

        $this->assertComparable($left, $right, '>=');

        if ($left === null || $right === null) {
            return false;
        }

        return $left >= $right;
    }
}
