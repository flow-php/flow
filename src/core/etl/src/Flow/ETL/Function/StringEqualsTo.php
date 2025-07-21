<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class StringEqualsTo extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $string,
    ) {
    }

    public function eval(Row $row) : ?bool
    {
        $value = (new Parameter($this->value))->asString($row);
        $string = (new Parameter($this->string))->asString($row);

        if ($value === null || $string === null) {
            return null;
        }

        return s($value)->equalsTo($string);
    }
}
