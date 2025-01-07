<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_array, type_string};
use Flow\ETL\Row;

final class JsonDecode extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $value,
        private readonly ScalarFunction|int $flags = JSON_THROW_ON_ERROR,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->value))->as($row, type_string(), type_array());
        $flags = (int) (new Parameter($this->flags))->asInt($row);

        if ($value === null) {
            return null;
        }

        if (\is_array($value)) {
            return $value;
        }

        try {
            return \json_decode((string) $value, true, 512, $flags);
        } catch (\JsonException) {
            return null;
        }
    }
}
