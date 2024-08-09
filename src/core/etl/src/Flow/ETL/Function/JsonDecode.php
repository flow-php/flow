<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class JsonDecode extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $value,
        private readonly ScalarFunction|int $flags = JSON_THROW_ON_ERROR
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->value))->asString($row);
        $flags = (int) (new Parameter($this->flags))->asInt($row);

        if ($value === null) {
            return null;
        }

        try {
            return \json_decode($value, true, 512, $flags);
        } catch (\JsonException $e) {
            return null;
        }
    }
}
