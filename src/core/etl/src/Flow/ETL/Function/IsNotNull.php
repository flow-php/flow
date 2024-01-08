<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class IsNotNull extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref
    ) {
    }

    public function eval(Row $row) : bool
    {
        return $this->ref->eval($row) !== null;
    }
}
