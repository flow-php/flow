<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

final class CidrConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [PostgreSqlType::CIDR];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'cidr');
    }
}
