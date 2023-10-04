<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class IsNumeric implements Expression
{
    public function __construct(
        private readonly Expression $ref
    ) {
    }

    public function eval(Row $row) : bool
    {
        return \is_numeric($this->ref->eval($row));
    }
}
