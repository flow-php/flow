<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class Truncate extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $length,
        private readonly ScalarFunction|string $ellipsis = '...',
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $length = (new Parameter($this->length))->asInt($row);
        $ellipsis = (new Parameter($this->ellipsis))->asString($row);

        if ($value === null) {
            return null;
        }

        if ($length === null) {
            return $value;
        }

        return s($value)->truncate($length, $ellipsis ?? '...')->toString();
    }
}
