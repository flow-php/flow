<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayKeep extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|array $array, private readonly mixed $value = null)
    {
    }

    public function eval(Row $row) : mixed
    {
        $array = (new Parameter($this->array))->asArray($row);

        if (null === $array) {
            return null;
        }

        $value = (new Parameter($this->value))->eval($row);

        return \array_filter($array, fn ($item) => $item === $value);
    }
}
