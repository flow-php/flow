<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Equals extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $base,
        private readonly ScalarFunction $next
    ) {
    }

    public function eval(Row $row) : bool
    {
        $base = $this->base->eval($row);
        $next = $this->next->eval($row);

        return $base == $next;
    }
}
