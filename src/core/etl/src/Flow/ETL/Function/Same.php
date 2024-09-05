<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\Comparison\Comparable;
use Flow\ETL\Row;

final class Same extends ScalarFunctionChain
{
    use Comparable;

    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {
    }

    public function eval(Row $row) : bool
    {
        $base = (new Parameter($this->left))->eval($row);
        $next = (new Parameter($this->right))->eval($row);

        $this->assertComparable($base, $next, '===');

        return $base === $next;
    }
}
