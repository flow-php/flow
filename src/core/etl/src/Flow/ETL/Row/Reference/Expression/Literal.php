<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Literal implements Expression
{
    public function __construct(
        private readonly mixed $value
    ) {
    }

    public function eval(Row $row) : mixed
    {
        return $this->value;
    }
}
