<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class EnsureEnd extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string $suffix,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $suffix = (new Parameter($this->suffix))->asString($row);

        if ($value === null) {
            return null;
        }

        if ($suffix === null || $suffix === '') {
            return $value;
        }

        return s($value)->ensureEnd($suffix)->toString();
    }
}
