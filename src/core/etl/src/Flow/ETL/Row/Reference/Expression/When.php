<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class When implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly Expression $then,
        private readonly ?Expression $else = null
    ) {
    }

    public function eval(Row $row) : mixed
    {
        if ($this->ref->eval($row)) {
            return $this->then->eval($row);
        }

        if ($this->else) {
            return $this->else->eval($row);
        }

        return $this->ref->eval($row);
    }
}
