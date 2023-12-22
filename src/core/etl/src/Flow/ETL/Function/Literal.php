<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Literal implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(
        private readonly mixed $value
    ) {
    }

    public function eval(Row $row) : mixed
    {
        return $this->value;
    }
}
