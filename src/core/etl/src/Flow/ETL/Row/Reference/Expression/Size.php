<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Size implements Expression
{
    public function __construct(private readonly Expression $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (\is_string($value)) {
            return \mb_strlen($value);
        }

        if (\is_countable($value)) {
            return \count($value);
        }

        return null;
    }
}
