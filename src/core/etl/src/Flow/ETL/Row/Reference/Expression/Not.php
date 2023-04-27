<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Not implements Expression
{
    public function __construct(private readonly Expression $expression)
    {
    }

    public function eval(Row $row) : mixed
    {
        return !$this->expression->eval($row);
    }
}
