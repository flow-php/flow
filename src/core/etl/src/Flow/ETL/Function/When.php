<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class When implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction $then,
        private readonly ?ScalarFunction $else = null
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
