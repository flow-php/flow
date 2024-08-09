<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ToUpper extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|string $value)
    {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->value))->asString($row);

        return match (\gettype($value)) {
            'string' => \mb_strtoupper($value),
            default => $value,
        };
    }
}
