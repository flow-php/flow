<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class Repeat extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $times,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $times = (new Parameter($this->times))->asInt($row);

        if ($value === null) {
            return null;
        }

        if ($times === null || $times <= 0) {
            return '';
        }

        return s($value)->repeat($times)->toString();
    }
}
