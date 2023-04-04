<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class IsNull implements Expression
{
    public function __construct(
        private readonly Expression $ref
    ) {
    }

    public function eval(Row $row) : bool
    {
        return $this->ref->eval($row) === null;
    }
}
