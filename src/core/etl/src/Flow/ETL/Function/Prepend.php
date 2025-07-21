<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class Prepend extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $prefix,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $prefix = (new Parameter($this->prefix))->asString($row);

        if ($value === null) {
            return null;
        }

        if ($prefix === null) {
            return $value;
        }

        return s($value)->prepend($prefix)->toString();
    }
}
