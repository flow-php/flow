<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class IsNumeric implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(
        private readonly ScalarFunction $ref
    ) {
    }

    public function eval(Row $row) : bool
    {
        return \is_numeric($this->ref->eval($row));
    }
}
