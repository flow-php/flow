<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayFilter extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|array $array, private readonly mixed $value = null)
    {
    }

    public function eval(Row $row) : mixed
    {
        $array = (new Parameter($this->array))->asArray($row);
        $value = (new Parameter($this->value))->eval($row);

        if (!\is_array($array)) {
            return null;
        }

        return \array_filter($array, fn ($item) => $item !== $value);
    }
}
