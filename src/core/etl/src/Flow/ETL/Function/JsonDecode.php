<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class JsonDecode extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref, private readonly int $flags = JSON_THROW_ON_ERROR)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (!\is_string($value)) {
            return null;
        }

        try {
            return \json_decode($value, true, 512, $this->flags);
        } catch (\JsonException $e) {
            return null;
        }
    }
}
