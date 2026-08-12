<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\ValueConverter;

use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function is_object;

final readonly class JsonValueConverter implements ValueConverter
{
    public function decode(mixed $value): mixed
    {
        return $value === null ? null : type_json()->cast($value);
    }

    public function encode(mixed $value): mixed
    {
        return is_object($value) ? type_string()->cast($value) : $value;
    }
}
