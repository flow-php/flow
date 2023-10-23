<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ArrayMergeCollection implements Expression
{
    public function __construct(private readonly Expression $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!\is_array($value)) {
            return null;
        }

        foreach ($value as $element) {
            if (!\is_array($element)) {
                return null;
            }
        }

        return \array_merge(...\array_values($value));
    }
}
