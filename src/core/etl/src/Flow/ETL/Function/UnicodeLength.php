<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\u;
use Flow\ETL\Row;

final class UnicodeLength extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
    ) {
    }

    public function eval(Row $row) : ?int
    {
        $value = (new Parameter($this->value))->asString($row);

        if ($value === null) {
            return null;
        }

        return u($value)->length();
    }
}
