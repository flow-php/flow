<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArrayReverse implements Expression
{
    public function __construct(private readonly Expression $left, private readonly bool $preserveKeys)
    {
    }

    public function eval(Row $row) : mixed
    {
        $left = $this->left->eval($row);

        if (!\is_array($left)) {
            return null;
        }

        return \array_reverse($left, $this->preserveKeys);
    }
}
