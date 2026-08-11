<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\ValueConverter;

use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_uuid;
use function is_object;

final readonly class UuidValueConverter implements ValueConverter
{
    public function decode(mixed $value): mixed
    {
        return $value === null ? null : type_uuid()->cast($value);
    }

    public function encode(mixed $value): mixed
    {
        return is_object($value) ? type_string()->cast($value) : $value;
    }
}
