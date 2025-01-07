<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class JsonEncode extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $value,
        private readonly ScalarFunction|int $flags = JSON_THROW_ON_ERROR,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->value))->eval($row);
        $flags = (int) (new Parameter($this->flags))->asInt($row);

        try {
            return \json_encode($value, $flags);
        } catch (\JsonException) {
            return null;
        }
    }
}
