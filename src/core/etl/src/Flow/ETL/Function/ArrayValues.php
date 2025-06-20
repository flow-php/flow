<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayValues extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     */
    public function __construct(private readonly ScalarFunction|array $array)
    {
    }

    /**
     * @return null|array<int, mixed>
     */
    public function eval(Row $row) : mixed
    {
        $array = (new Parameter($this->array))->asArray($row);

        if (!\is_array($array)) {
            return null;
        }

        return \array_values($array);
    }
}
