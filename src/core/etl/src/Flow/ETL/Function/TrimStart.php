<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Symfony\Component\String\s;
use Flow\ETL\Row;

final class TrimStart extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|string $value,
        private readonly ScalarFunction|string|null $chars = null,
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $value = (new Parameter($this->value))->asString($row);
        $chars = $this->chars !== null ? (new Parameter($this->chars))->asString($row) : null;

        if ($value === null) {
            return null;
        }

        if ($chars === null) {
            return s($value)->trimStart()->toString();
        }

        return s($value)->trimStart($chars)->toString();
    }
}
