<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class Slice extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|int $start,
        private readonly ScalarFunction|int|null $length = null,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $start = (new Parameter($this->start))->asInt($row);
        $length = $this->length !== null ? (new Parameter($this->length))->asInt($row) : null;

        if ($value === null) {
            return null;
        }

        if ($start === null) {
            return $value;
        }

        return s($value)->slice($start, $length)->toString();
    }
}
