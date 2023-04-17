<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ToUpper implements Expression
{
    public function __construct(private Expression $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        return match (\gettype($value)) {
            'string' => \mb_strtoupper($value),
            default => $value,
        };
    }
}
