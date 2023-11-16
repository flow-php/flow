<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class JsonEncode implements ScalarFunction
{
    public function __construct(private readonly ScalarFunction $ref, private readonly int $flags = JSON_THROW_ON_ERROR)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        try {
            return \json_encode($value, $this->flags);
        } catch (\JsonException $e) {
            return null;
        }
    }
}
