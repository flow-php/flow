<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Count implements Expression
{
    public function __construct(private readonly Expression $expression)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $val */
        $val = $this->expression->eval($row);

        if (\is_countable($val)) {
            return \count($val);
        }

        return null;
    }
}
