<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayMergeCollection implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(private readonly ScalarFunction $ref)
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
