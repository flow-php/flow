<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayMergeCollection extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|array $array)
    {
    }

    public function eval(Row $row) : mixed
    {
        $array = (new Parameter($this->array))->asArray($row);

        if ($array === null) {
            return null;
        }

        foreach ($array as $element) {
            if (!\is_array($element)) {
                return null;
            }
        }

        return \array_merge(...\array_values($array));
    }
}
