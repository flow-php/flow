<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Exception;

final class ValueConversionException extends ClientException
{
    public static function cannotConvert(mixed $value, string $targetType) : self
    {
        $valueType = \get_debug_type($value);

        return new self(\sprintf('Cannot convert value of type "%s" to "%s"', $valueType, $targetType));
    }

    public static function unsupportedOid(int $oid) : self
    {
        return new self(\sprintf('No type converter registered for PostgreSQL OID %d', $oid));
    }
}
