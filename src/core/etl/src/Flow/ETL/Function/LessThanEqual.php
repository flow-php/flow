<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Function\Comparison\Comparable;
use Flow\ETL\Row;

final class LessThanEqual extends ScalarFunctionChain
{
    use Comparable;

    public function __construct(
        private readonly ScalarFunction $base,
        private readonly ScalarFunction $next
    ) {
    }

    public function eval(Row $row) : bool
    {
        $base = $this->base->eval($row);
        $next = $this->next->eval($row);

        $this->assertComparable($base, $next, '<=');

        return $base <= $next;
    }
}
