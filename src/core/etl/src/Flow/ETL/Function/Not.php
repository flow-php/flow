<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Not implements ScalarFunction
{
    public function __construct(private readonly ScalarFunction $expression)
    {
    }

    public function eval(Row $row) : mixed
    {
        return !$this->expression->eval($row);
    }
}
