<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class Size extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
    ) {
    }

    public function eval(Row $row) : ?int
    {
        $value = (new Parameter($this->value))->eval($row);

        if (\is_string($value)) {
            return s($value)->length();
        }

        if (\is_countable($value)) {
            return \count($value);
        }

        return null;
    }
}
